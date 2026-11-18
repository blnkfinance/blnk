/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/database"
	dbmocks "github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// lostAtomicBulk is a queued atomic bulk request whose items' tasks were
// lost after their rows were saved, so only queue recovery can land them.
type lostAtomicBulk struct {
	id        string
	src       string
	dsts      []string
	tasks     []*model.Transaction
	rows      []*model.Transaction
	inspector *asynq.Inspector
	webhooks  string
}

// queueLostAtomicBulk queues an atomic bulk request paying the amounts from a
// source that holds 100, then drops the batch's task. Tasks and rows are in
// request order.
func queueLostAtomicBulk(t *testing.T, b *Blnk, ds database.IDataSource, runAsync bool, amounts ...float64) *lostAtomicBulk {
	t.Helper()
	ctx := context.Background()

	suffix := strings.ToLower(gofakeit.LetterN(8))
	cfg := b.Config()
	cfg.Queue.TransactionQueue = "transaction_queue_recovery_" + suffix
	cfg.Queue.WebhookQueue = "webhook_queue_recovery_" + suffix
	// Webhooks are only enqueued here; nothing serves this queue.
	cfg.Notification.Webhook.Url = "http://127.0.0.1:1/recovery"
	queue := cfg.Queue.TransactionQueue + "_1"
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.Redis.Dns})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queue, true)
		_ = inspector.DeleteQueue(cfg.Queue.WebhookQueue, true)
		_ = inspector.Close()
	})

	funding, src := newBalancePair(t, ds)
	recordAppliedTxn(t, b, funding.BalanceID, src.BalanceID, 100)
	bulk := &lostAtomicBulk{src: src.BalanceID, inspector: inspector, webhooks: cfg.Queue.WebhookQueue}
	var items []*model.Transaction
	for _, amount := range amounts {
		dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
		require.NoError(t, err)
		bulk.dsts = append(bulk.dsts, dst.BalanceID)
		items = append(items, &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: amount, Precision: 100, Currency: "USD"})
	}
	result, err := b.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, RunAsync: runAsync, Transactions: items})
	require.NoError(t, err)
	bulk.id = result.BatchID

	var task *asynq.TaskInfo
	deadline := time.Now().Add(10 * time.Second)
	for task == nil {
		require.True(t, time.Now().Before(deadline), "the batch was not queued")
		time.Sleep(50 * time.Millisecond)
		task, _ = inspector.GetTaskInfo(queue, bulk.id)
	}
	var batch QueuedAtomicBatch
	require.NoError(t, json.Unmarshal(task.Payload, &batch))
	require.NoError(t, inspector.DeleteTask(queue, task.ID))
	bulk.tasks = batch.Transactions
	require.Len(t, bulk.tasks, len(amounts))
	for _, txn := range bulk.tasks {
		require.Equal(t, bulk.id, txn.MetaData["QUEUED_PARENT_TRANSACTION"])
		row, err := ds.GetTransaction(ctx, txn.ParentTransaction)
		require.NoError(t, err)
		require.Equal(t, StatusQueued, row.Status)
		bulk.rows = append(bulk.rows, row)
	}
	return bulk
}

// bulkEvents lists the bulk_transaction.* webhooks enqueued for the batch.
func (bulk *lostAtomicBulk) bulkEvents(t *testing.T) []string {
	t.Helper()
	tasks, _ := bulk.inspector.ListPendingTasks(bulk.webhooks, asynq.PageSize(100))
	var events []string
	for _, task := range tasks {
		var hook struct {
			Event string `json:"event"`
			Data  struct {
				BatchID string `json:"batch_id"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(task.Payload, &hook))
		if strings.HasPrefix(hook.Event, "bulk_transaction.") && hook.Data.BatchID == bulk.id {
			events = append(events, hook.Event)
		}
	}
	return events
}

func balanceOf(t *testing.T, ds database.IDataSource, id string) string {
	t.Helper()
	balance, err := ds.GetBalanceByIDLite(id)
	require.NoError(t, err)
	return balance.Balance.String()
}

// A stuck item of an atomic batch that has already failed is rejected by
// recovery, not applied after the batch was rolled back and reported.
func TestRecovery_StuckItemOfFailedAtomicBatchIsRejected(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	ctx := context.Background()
	bulk := queueLostAtomicBulk(t, b, ds, false, 50, 500)

	// The worker refuses the 500, which fails the batch with nothing applied.
	_, err := b.RejectTransaction(ctx, bulk.tasks[1], "insufficient funds")
	require.NoError(t, err)
	require.Equal(t, []string{"bulk_transaction.failed"}, bulk.bulkEvents(t))

	require.NoError(t, NewQueuedTransactionRecoveryProcessor(b).processStuckTransaction(ctx, bulk.rows[0]))

	assert.Equal(t, "10000", balanceOf(t, ds, bulk.src), "the source must keep its 100")
	assert.Equal(t, "0", balanceOf(t, ds, bulk.dsts[0]), "the 50 must not be paid after its batch failed")
	recovered, err := ds.GetTransactionByRef(ctx, bulk.tasks[0].Reference)
	require.NoError(t, err)
	assert.Equal(t, StatusRejected, recovered.Status)
	assert.Equal(t, []string{"bulk_transaction.failed"}, bulk.bulkEvents(t), "the batch is reported once")
}

// When recovery lands the last item of an async atomic batch, the batch is
// reported applied, as when the worker lands it.
func TestRecovery_LastItemOfAsyncAtomicBatchReportsTheBatch(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	ctx := context.Background()
	bulk := queueLostAtomicBulk(t, b, ds, true, 50, 10)

	// The worker lands the 50.
	require.NoError(t, b.WithQueuedAtomicBatch(ctx, bulk.tasks[0], func(batchFailed bool) error {
		require.False(t, batchFailed)
		_, err := b.ProcessQueuedTransaction(ctx, bulk.tasks[0], false)
		return err
	}))
	assert.Empty(t, bulk.bulkEvents(t), "nothing is reported while an item is still queued")

	require.NoError(t, NewQueuedTransactionRecoveryProcessor(b).processStuckTransaction(ctx, bulk.rows[1]))

	assert.Equal(t, "1000", balanceOf(t, ds, bulk.dsts[1]), "recovery pays the 10")
	assert.Equal(t, []string{"bulk_transaction.applied"}, bulk.bulkEvents(t), "the batch is reported once its last item lands")
}

// Recovery replays stuck rows oldest first. When an item of an atomic batch
// fails, the batch's later items wait for the next pass instead of being
// applied before it; rows of other batches are still recovered.
func TestRecoverWithThreshold_AtomicBatchWaitsForItsFailedItem(t *testing.T) {
	b, _ := newCoreTestBlnk(t)
	mockDS := &dbmocks.MockDataSource{}
	b.datasource = mockDS
	processor := NewQueuedTransactionRecoveryProcessor(b)

	stuckItem := func(batchID, ref string) *model.Transaction {
		txn := positiveStuckTxn()
		txn.TransactionID = model.GenerateUUIDWithSuffix("txn")
		txn.Reference = ref
		txn.MetaData = map[string]interface{}{"atomic": true, "QUEUED_PARENT_TRANSACTION": batchID}
		return txn
	}
	batch, other := model.GenerateUUIDWithSuffix("bulk"), model.GenerateUUIDWithSuffix("bulk")
	first, second, elsewhere := stuckItem(batch, "ref_first"), stuckItem(batch, "ref_second"), stuckItem(other, "ref_elsewhere")
	mockDS.On("GetStuckQueuedTransactions", mock.Anything, mock.Anything, 100).
		Return([]*model.Transaction{first, second, elsewhere}, nil).Once()
	mockDS.On("GetTransactionsByParent", mock.Anything, mock.Anything, 100, int64(0)).Return([]*model.Transaction{}, nil)
	mockDS.On("UpdateTransactionMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	var replayed []string
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
		replayed = append(replayed, txn.Reference)
		if txn.ParentTransaction == first.TransactionID {
			return transactionExecutionResult{}, errors.New("lock contention")
		}
		return transactionExecutionResult{mode: transactionExecutionModeSingle, transaction: txn}, nil
	}

	recovered, err := processor.recoverWithThreshold(context.Background(), time.Hour)
	require.Error(t, err)
	assert.Equal(t, 1, recovered)
	assert.Equal(t, []string{"ref_first_q", "ref_elsewhere_q"}, replayed, "the second item must wait for the first")
}
