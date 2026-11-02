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

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real lifecycle and handler tests for the worker process, against the local
// Postgres, Redis, and Typesense (the suite's hard dependencies).
//
// Not covered on purpose: the max-retry branch of processTransaction needs
// asynq.GetRetryCount, which only carries a value inside a running asynq
// server context; its decision helper hasReachedMaxRetryAttempt is unit-
// tested separately in workers_test.go.

func TestProcessTransaction_AppliesQueuedTransaction(t *testing.T) {
	b := newCmdTestInstance(t)
	src, dst := createBalancePair(t, b)

	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 50, true)
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)
	require.NoError(t, b.processTransaction(context.Background(), task))

	// The money must actually have moved.
	dstAfter, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "5000", dstAfter.Balance.String(), "destination must be credited 50 at precision 100")
}

func TestProcessTransaction_MalformedPayload(t *testing.T) {
	b := newCmdTestInstance(t)

	task := asynq.NewTask("transaction_queue_cmd_test_1", []byte("{not json"))
	err := b.processTransaction(context.Background(), task)
	require.Error(t, err, "a malformed payload must error so asynq can retry/dead-letter it")
}

func TestProcessTransaction_InsufficientFundsRejects(t *testing.T) {
	b := newCmdTestInstance(t)
	// Disable retries so the insufficient-funds branch rejects immediately.
	b.cnf.Queue.InsufficientFundRetries = false

	src, dst := createBalancePair(t, b)

	// No overdraft and an empty source balance: processing must fail with
	// insufficient funds and the transaction must be REJECTED, not retried.
	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 75, false)
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)
	require.NoError(t, b.processTransaction(context.Background(), task), "rejection is a handled outcome, not a handler error")

	recorded, err := b.blnk.GetTransactionByRef(context.Background(), txn.Reference)
	require.NoError(t, err)
	assert.Equal(t, "REJECTED", recorded.Status)

	// The destination must not have been credited.
	dstAfter, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", dstAfter.Balance.String())
}

func TestProcessTransaction_DuplicateReferenceAcks(t *testing.T) {
	b := newCmdTestInstance(t)
	src, dst := createBalancePair(t, b)

	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 50, true)
	original := *txn
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)
	require.NoError(t, b.processTransaction(context.Background(), task))

	replay := original
	replay.TransactionID = model.GenerateUUIDWithSuffix("txn")
	replayPayload, err := json.Marshal(&replay)
	require.NoError(t, err)

	replayTask := asynq.NewTask("transaction_queue_cmd_test_1", replayPayload)
	require.NoError(t, b.processTransaction(context.Background(), replayTask), "duplicate reference is a handled outcome, not a handler error")

	dstAfter, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "5000", dstAfter.Balance.String(), "destination must not be credited twice")
}

// An atomic bulk request in the default queued mode, with and without
// run_async, run by the real worker, which takes the items in whatever order
// asynq hands them out. When one item is rejected, nothing from the batch may
// stay applied, and bulk_transaction.failed must report the rollback it
// actually made, as the only report of the batch.
func TestWorker_AtomicBulkRejectionRollsBackQueuedBatch(t *testing.T) {
	cfg := realInfraConfig(t)
	suffix := strings.ToLower(gofakeit.LetterN(8))
	cfg.Queue.TransactionQueue = "transaction_queue_cmd_test_bulk_" + suffix
	cfg.Queue.WebhookQueue = "webhook_queue_cmd_test_bulk_" + suffix
	cfg.Queue.InsufficientFundRetries = false
	// Webhooks are only enqueued here; nothing serves this queue.
	cfg.Notification.Webhook.Url = "http://127.0.0.1:1/bulk"
	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")
	b := &blnkInstance{blnk: newBlnk, cnf: cfg}
	ctx := context.Background()

	srv, err := initializeWorkerServer(cfg, initializeQueues())
	require.NoError(t, err)
	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)
	require.NoError(t, srv.Start(mux))

	queue := cfg.Queue.TransactionQueue + "_1"
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.Redis.Dns})
	db, err := sql.Open("postgres", cfg.DataSource.Dns)
	require.NoError(t, err)
	t.Cleanup(func() {
		srv.Shutdown()
		_ = inspector.DeleteQueue(queue, true)
		_ = inspector.DeleteQueue(cfg.Queue.WebhookQueue, true)
		_ = inspector.Close()
		_ = db.Close()
	})

	// drain waits until the worker has nothing left to run, refunds included.
	drain := func() {
		deadline := time.Now().Add(30 * time.Second)
		for idle := time.Now(); time.Since(idle) < time.Second; time.Sleep(50 * time.Millisecond) {
			require.True(t, time.Now().Before(deadline), "the transaction queue did not drain")
			info, err := inspector.GetQueueInfo(queue)
			if err != nil || info.Pending+info.Active+info.Scheduled+info.Retry > 0 {
				idle = time.Now()
			}
		}
	}
	failedWebhooks := func(batchID string) []string {
		return bulkFailedWebhooks(t, inspector, cfg.Queue.WebhookQueue, batchID)
	}
	newBalance := func() *model.Balance {
		balance, err := b.blnk.GetDataSource().CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
		require.NoError(t, err)
		return &balance
	}
	balanceOf := func(id string) *model.Balance {
		balance, err := b.blnk.GetDataSource().GetBalanceByIDLite(id)
		require.NoError(t, err)
		return balance
	}

	// The source of each batch holds 100: 500 cannot be paid, 50 and 10 can.
	// Every case is sent five times and all batches go to the worker at once,
	// so the items are applied in varying orders.
	type batch struct {
		name     string
		inflight bool
		amounts  []float64
		id       string
		src      *model.Balance
		dsts     []*model.Balance
	}
	var batches []*batch
	for _, runAsync := range []bool{false, true} {
		for _, inflight := range []bool{false, true} {
			for _, amounts := range [][]float64{{50, 500}, {50, 500, 10}} {
				for run := 0; run < 5; run++ {
					bt := &batch{name: fmt.Sprintf("run_async=%v inflight=%v %v run %d", runAsync, inflight, amounts, run), inflight: inflight, amounts: amounts}
					funding := newBalance()
					bt.src = newBalance()
					funded := queuedTransaction(funding.BalanceID, bt.src.BalanceID, 100, true)
					funded.SkipQueue = true
					_, err := b.blnk.QueueTransaction(ctx, funded)
					require.NoError(t, err)

					var items []*model.Transaction
					for _, amount := range amounts {
						dst := newBalance()
						bt.dsts = append(bt.dsts, dst)
						items = append(items, &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: bt.src.BalanceID, Destination: dst.BalanceID, Amount: amount, Precision: 100, Currency: "USD"})
					}
					result, err := b.blnk.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, Inflight: inflight, RunAsync: runAsync, Transactions: items})
					require.NoError(t, err)
					bt.id = result.BatchID
					batches = append(batches, bt)
				}
			}
		}
	}
	drain()

	for _, bt := range batches {
		srcAfter := balanceOf(bt.src.BalanceID)
		assert.Equal(t, "10000", srcAfter.Balance.String(), "%s: the source must be back at 100", bt.name)
		assert.Equal(t, "0", srcAfter.InflightDebitBalance.String(), "%s: no inflight debit may stay", bt.name)
		for i, dst := range bt.dsts {
			dstAfter := balanceOf(dst.BalanceID)
			assert.Equal(t, "0", dstAfter.Balance.String(), "%s: item %d must not stay applied", bt.name, i)
			assert.Equal(t, "0", dstAfter.InflightCreditBalance.String(), "%s: item %d must not stay inflight", bt.name, i)
		}

		// Items the worker had applied (or put inflight) before the
		// rejection; the webhook must report exactly these as rolled back.
		status, kind, action := "APPLIED", "applied", "refunded"
		if bt.inflight {
			status, kind, action = "INFLIGHT", "inflight", "voided"
		}
		var held int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM blnk.transactions
			WHERE status = $2 AND meta_data->>'QUEUED_PARENT_TRANSACTION' = $1
			AND reference NOT LIKE '%_refund'`, bt.id, status).Scan(&held))
		messages := failedWebhooks(bt.id)
		require.Len(t, messages, 1, "%s: one bulk_transaction.failed per batch", bt.name)
		assert.Len(t, bulkWebhooks(t, inspector, cfg.Queue.WebhookQueue, bt.id), 1, "%s: the batch is reported failed and nothing else", bt.name)
		if held == 0 {
			assert.Contains(t, messages[0], "Nothing from this batch", "%s", bt.name)
		} else {
			assert.Contains(t, messages[0], fmt.Sprintf("%d %s transaction(s) from this batch have been %s", held, kind, action), "%s", bt.name)
		}

		// Every other item ends REJECTED: the one that failed, and the
		// ones the worker reached after it, which were never applied.
		var rejected int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM blnk.transactions
			WHERE status = 'REJECTED' AND meta_data->>'QUEUED_PARENT_TRANSACTION' = $1`, bt.id).Scan(&rejected))
		assert.Equal(t, len(bt.amounts)-held, rejected, "%s: the items not applied must be rejected", bt.name)

		// The refunds (or voids) the rollback wrote are listed under the
		// batch id too, one for each item it reversed.
		reversal := `status = 'APPLIED' AND reference = parent_transaction || '_refund'`
		if bt.inflight {
			reversal = `status = 'VOID'`
		}
		var reversed int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM blnk.transactions
			WHERE meta_data->>'QUEUED_PARENT_TRANSACTION' = $1 AND `+reversal, bt.id).Scan(&reversed))
		assert.Equal(t, held, reversed, "%s: each reversal must be listed under the batch id", bt.name)
	}
}

// bulkFailedWebhooks returns the error text of every bulk_transaction.failed
// webhook waiting in queue for batchID.
func bulkFailedWebhooks(t *testing.T, inspector *asynq.Inspector, queue, batchID string) []string {
	t.Helper()
	var messages []string
	for _, hook := range bulkWebhooks(t, inspector, queue, batchID) {
		if hook.Event == "bulk_transaction.failed" {
			messages = append(messages, hook.Data.Error)
		}
	}
	return messages
}

type bulkWebhook struct {
	Event string `json:"event"`
	Data  struct {
		BatchID          string `json:"batch_id"`
		Error            string `json:"error"`
		TransactionCount int    `json:"transaction_count"`
	} `json:"data"`
}

// bulkWebhooks returns every bulk_transaction webhook waiting in queue for
// batchID.
func bulkWebhooks(t *testing.T, inspector *asynq.Inspector, queue, batchID string) []bulkWebhook {
	t.Helper()
	tasks, err := inspector.ListPendingTasks(queue, asynq.PageSize(1000))
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return nil // nothing has been sent yet
	}
	require.NoError(t, err)
	var hooks []bulkWebhook
	for _, task := range tasks {
		var hook bulkWebhook
		require.NoError(t, json.Unmarshal(task.Payload, &hook))
		if strings.HasPrefix(hook.Event, "bulk_transaction.") && hook.Data.BatchID == batchID {
			hooks = append(hooks, hook)
		}
	}
	return hooks
}

// queuedAtomicBatch is an atomic bulk request whose items wait in the queue
// with no worker serving it, so a test hands them to processTransaction
// itself, in request order.
type queuedAtomicBatch struct {
	b         *blnkInstance
	inspector *asynq.Inspector
	db        *sql.DB
	id        string
	src       *model.Balance
	dsts      []*model.Balance
	tasks     []*asynq.Task
	items     []*model.Transaction
}

// queueAtomicBatch funds a source with 100 and queues an atomic bulk request
// that pays amounts from it, each to a new balance.
func queueAtomicBatch(t *testing.T, inflight bool, amounts ...float64) *queuedAtomicBatch {
	t.Helper()
	return queueBulkRequest(t, model.BulkTransactionRequest{Atomic: true, Inflight: inflight}, amounts...)
}

// queueBulkRequest is queueAtomicBatch for a request with other options.
func queueBulkRequest(t *testing.T, req model.BulkTransactionRequest, amounts ...float64) *queuedAtomicBatch {
	t.Helper()
	cfg := realInfraConfig(t)
	suffix := strings.ToLower(gofakeit.LetterN(8))
	cfg.Queue.TransactionQueue = "transaction_queue_cmd_test_bulk_" + suffix
	cfg.Queue.WebhookQueue = "webhook_queue_cmd_test_bulk_" + suffix
	cfg.Queue.InsufficientFundRetries = false
	// Webhooks are only enqueued here; nothing serves this queue.
	cfg.Notification.Webhook.Url = "http://127.0.0.1:1/bulk"
	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")
	b := &blnkInstance{blnk: newBlnk, cnf: cfg}
	ctx := context.Background()

	queue := cfg.Queue.TransactionQueue + "_1"
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.Redis.Dns})
	db, err := sql.Open("postgres", cfg.DataSource.Dns)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queue, true)
		_ = inspector.DeleteQueue(cfg.Queue.WebhookQueue, true)
		_ = inspector.Close()
		_ = db.Close()
	})

	bt := &queuedAtomicBatch{b: b, inspector: inspector, db: db}
	ds := b.blnk.GetDataSource()
	funding, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	bt.src = &src
	funded := queuedTransaction(funding.BalanceID, src.BalanceID, 100, true)
	funded.SkipQueue = true
	_, err = b.blnk.QueueTransaction(ctx, funded)
	require.NoError(t, err)

	var items []*model.Transaction
	for _, amount := range amounts {
		dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
		require.NoError(t, err)
		bt.dsts = append(bt.dsts, &dst)
		items = append(items, &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: amount, Precision: 100, Currency: "USD"})
	}
	req.Transactions = items
	result, err := b.blnk.CreateBulkTransactions(ctx, &req)
	require.NoError(t, err)
	bt.id = result.BatchID

	// The items are queued in the background; wait for all of them.
	var queued []*asynq.TaskInfo
	deadline := time.Now().Add(10 * time.Second)
	for len(queued) < len(amounts) {
		require.True(t, time.Now().Before(deadline), "the batch's items were not queued")
		time.Sleep(50 * time.Millisecond)
		queued, _ = inspector.ListPendingTasks(queue, asynq.PageSize(1000))
	}
	sequence := make(map[*asynq.TaskInfo]float64, len(queued))
	for _, task := range queued {
		var txn model.Transaction
		require.NoError(t, json.Unmarshal(task.Payload, &txn))
		require.Equal(t, bt.id, txn.MetaData["QUEUED_PARENT_TRANSACTION"])
		sequence[task], _ = txn.MetaData["sequence"].(float64)
	}
	// In request order, with the payloads exactly as queued.
	sort.Slice(queued, func(i, j int) bool { return sequence[queued[i]] < sequence[queued[j]] })
	for _, task := range queued {
		var txn model.Transaction
		require.NoError(t, json.Unmarshal(task.Payload, &txn))
		bt.items = append(bt.items, &txn)
		bt.tasks = append(bt.tasks, asynq.NewTask(task.Type, task.Payload))
	}
	return bt
}

func (bt *queuedAtomicBatch) balance(t *testing.T, id string) *model.Balance {
	t.Helper()
	balance, err := bt.b.blnk.GetDataSource().GetBalanceByIDLite(id)
	require.NoError(t, err)
	return balance
}

// assertRolledBack checks that the source is back at 100 and that nothing
// the batch paid stays applied or inflight.
func (bt *queuedAtomicBatch) assertRolledBack(t *testing.T, when string) {
	t.Helper()
	src := bt.balance(t, bt.src.BalanceID)
	assert.Equal(t, "10000", src.Balance.String(), "%s: the source must be back at 100", when)
	assert.Equal(t, "0", src.InflightDebitBalance.String(), "%s: no inflight debit may stay", when)
	for i, dst := range bt.dsts {
		balance := bt.balance(t, dst.BalanceID)
		assert.Equal(t, "0", balance.Balance.String(), "%s: item %d must not stay applied", when, i)
		assert.Equal(t, "0", balance.InflightCreditBalance.String(), "%s: item %d must not stay inflight", when, i)
	}
}

// underBatch counts the rows listed under the batch id that match where.
func (bt *queuedAtomicBatch) underBatch(t *testing.T, where string) int {
	t.Helper()
	var n int
	require.NoError(t, bt.db.QueryRow(`SELECT count(*) FROM blnk.transactions
		WHERE meta_data->>'QUEUED_PARENT_TRANSACTION' = $1 AND `+where, bt.id).Scan(&n))
	return n
}

// The refund the rollback writes is listed under the batch id, next to the
// payment it reverses, and nothing refunds it in turn: neither a redelivered
// item of the batch nor a refund of the batch id.
func TestWorker_AtomicBulkRefundKeepsTheBatchID(t *testing.T) {
	bt := queueAtomicBatch(t, false, 50, 500)
	ctx := context.Background()
	paid := bt.items[0]

	require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[0]), "the 50 is paid")
	require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]), "the 500 is refused, which fails the batch")
	bt.assertRolledBack(t, "after the rollback")

	refundRef := model.RefundReference(paid.TransactionID)
	refund, err := bt.b.blnk.GetDataSource().GetTransactionByRef(ctx, refundRef)
	require.NoError(t, err, "the 50 must have been refunded")
	assert.Equal(t, "APPLIED", refund.Status)
	assert.Equal(t, bt.id, refund.MetaData["QUEUED_PARENT_TRANSACTION"], "the refund must keep the batch id")
	assert.Equal(t, 1, bt.underBatch(t, `status = 'APPLIED' AND reference = '`+paid.Reference+`'`), "the paid 50 is listed under the batch id")
	assert.Equal(t, 1, bt.underBatch(t, `status = 'APPLIED' AND reference = '`+refundRef+`'`), "its refund must be listed under the batch id with it")

	messages := bulkFailedWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id)
	require.Len(t, messages, 1)
	assert.Contains(t, messages[0], "1 applied transaction(s) from this batch have been refunded")

	// A second pass refunds nothing: the refused item delivered again, and
	// a refund of the whole batch id.
	require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]), "a redelivered item of a failed batch must be handled")
	_, err = bt.b.blnk.ProcessRefundsInBatches(ctx, bt.id, blnk.RefundOptions{SkipQueue: true})
	require.Error(t, err, "everything under the batch id has been refunded already")
	bt.assertRolledBack(t, "after a second pass")
	assert.Equal(t, 0, bt.underBatch(t, `reference LIKE '%\_refund\_refund%'`), "a refund must never be refunded")
	assert.Equal(t, 2, bt.underBatch(t, `status = 'APPLIED'`), "only the payment and its refund are applied under the batch")
	assert.Len(t, bulkFailedWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id), 1, "the batch is reported once")
}

// A rollback that fails after the batch was marked is retried, and the retry
// finishes it: the refused item is not recorded twice, what was paid is
// reversed, and bulk_transaction.failed goes out once, when the rollback is
// complete.
func TestWorker_AtomicBulkRollbackFinishesOnRetry(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(fmt.Sprintf("inflight=%v", inflight), func(t *testing.T) {
			bt := queueAtomicBatch(t, inflight, 50, 500)
			ctx := context.Background()
			paid, refused := bt.items[0], bt.items[1]
			webhooks := func() []string {
				return bulkFailedWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id)
			}

			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[0]), "the 50 is paid")

			// Someone else holds the lock the refund (or void) of the 50
			// needs (keys as in RefundTransactionWithOptions and
			// VoidInflightTransactionWithRef), so the rollback fails after
			// the batch is marked.
			lockKey := "refund:" + paid.TransactionID
			if inflight {
				lockKey = "inflight-commit:" + paid.TransactionID
			}
			rdb := redis.NewClient(&redis.Options{Addr: bt.b.cnf.Redis.Dns})
			t.Cleanup(func() { _ = rdb.Close() })
			require.NoError(t, rdb.Set(ctx, lockKey, "held-by-test", time.Minute).Err())

			err := bt.b.processTransaction(ctx, bt.tasks[1])
			assert.Error(t, err, "a failed rollback must be returned, so the worker retries the item")
			src := bt.balance(t, bt.src.BalanceID)
			if inflight {
				assert.Equal(t, "5000", src.InflightDebitBalance.String(), "the 50 is still inflight")
			} else {
				assert.Equal(t, "5000", src.Balance.String(), "the 50 is still paid")
			}
			assert.Empty(t, webhooks(), "the batch is not reported before its rollback is complete")

			// The lock is free again; the worker's retry finishes the rollback.
			require.NoError(t, rdb.Del(ctx, lockKey).Err())
			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]))
			bt.assertRolledBack(t, "after the retry")

			if inflight {
				assert.Equal(t, 1, bt.underBatch(t, `status = 'VOID'`), "the void must be listed under the batch id")
			} else {
				assert.Equal(t, 1, bt.underBatch(t, `status = 'APPLIED' AND reference = '`+model.RefundReference(paid.TransactionID)+`'`),
					"the refund must be listed under the batch id")
			}
			assert.Equal(t, 1, bt.underBatch(t, `status = 'REJECTED'`), "the refused item is recorded once")
			rejected, err := bt.b.blnk.GetDataSource().GetTransactionByRef(ctx, refused.Reference)
			require.NoError(t, err)
			assert.Contains(t, rejected.MetaData["blnk_rejection_reason"], "insufficient funds", "the item keeps the reason it was refused for")

			messages := webhooks()
			require.Len(t, messages, 1, "one bulk_transaction.failed per batch")
			kind, action := "applied", "refunded"
			if inflight {
				kind, action = "inflight", "voided"
			}
			assert.Contains(t, messages[0], fmt.Sprintf("1 %s transaction(s) from this batch have been %s", kind, action))

			// Another delivery changes nothing and reports nothing new.
			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]))
			bt.assertRolledBack(t, "after another delivery")
			assert.Len(t, webhooks(), 1)
		})
	}
}

// With run_async, a queued atomic batch is reported by the worker, not by the
// request that queued it. A batch the worker fails gets bulk_transaction.failed
// once its rollback is complete, and nothing that says it was applied.
func TestWorker_AsyncAtomicBulkReportsOnlyTheFailure(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(fmt.Sprintf("inflight=%v", inflight), func(t *testing.T) {
			bt := queueBulkRequest(t, model.BulkTransactionRequest{Atomic: true, Inflight: inflight, RunAsync: true}, 50, 500)
			ctx := context.Background()
			webhooks := func() []bulkWebhook {
				return bulkWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id)
			}

			assert.Empty(t, webhooks(), "nothing is reported while the items are queued")
			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[0]), "the 50 is paid")
			assert.Empty(t, webhooks(), "nothing is reported while an item is still queued")

			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]), "the 500 is refused, which fails the batch")
			bt.assertRolledBack(t, "after the rollback")
			hooks := webhooks()
			require.Len(t, hooks, 1, "the batch is reported once")
			assert.Equal(t, "bulk_transaction.failed", hooks[0].Event)
			kind, action := "applied", "refunded"
			if inflight {
				kind, action = "inflight", "voided"
			}
			assert.Contains(t, hooks[0].Data.Error, fmt.Sprintf("1 %s transaction(s) from this batch have been %s", kind, action))

			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]))
			assert.Len(t, webhooks(), 1, "another delivery reports nothing new")
		})
	}
}

// With run_async, a queued atomic batch whose items all land is reported
// applied (or inflight) once, by the worker, when the last item has landed.
func TestWorker_AsyncAtomicBulkReportsAppliedWhenItemsLand(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(fmt.Sprintf("inflight=%v", inflight), func(t *testing.T) {
			bt := queueBulkRequest(t, model.BulkTransactionRequest{Atomic: true, Inflight: inflight, RunAsync: true}, 50, 10)
			ctx := context.Background()
			webhooks := func() []bulkWebhook {
				return bulkWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id)
			}
			event, landed := "bulk_transaction.applied", `status = 'APPLIED'`
			if inflight {
				event, landed = "bulk_transaction.inflight", `status = 'INFLIGHT'`
			}

			assert.Empty(t, webhooks(), "nothing is reported while the items are queued")
			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[0]))
			assert.Equal(t, 1, bt.underBatch(t, landed))
			assert.Empty(t, webhooks(), "nothing is reported while an item is still queued")

			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]))
			assert.Equal(t, 2, bt.underBatch(t, landed))
			hooks := webhooks()
			require.Len(t, hooks, 1, "the batch is reported once its items have landed")
			assert.Equal(t, event, hooks[0].Event)
			assert.Equal(t, 2, hooks[0].Data.TransactionCount)

			require.NoError(t, bt.b.processTransaction(ctx, bt.tasks[1]))
			assert.Len(t, webhooks(), 1, "another delivery reports nothing new")
		})
	}
}

func TestProcessInflightCommitAndExpiry(t *testing.T) {
	b := newCmdTestInstance(t)
	ctx := context.Background()

	startInflight := func() string {
		src, dst := createBalancePair(t, b)
		txn := queuedTransaction(src.BalanceID, dst.BalanceID, 20, true)
		txn.Inflight = true
		recorded, err := b.blnk.RecordTransaction(ctx, txn)
		require.NoError(t, err)
		require.Equal(t, "INFLIGHT", recorded.Status)
		return recorded.TransactionID
	}

	t.Run("commit", func(t *testing.T) {
		txnID := startInflight()
		payload, err := json.Marshal(txnID)
		require.NoError(t, err)

		task := asynq.NewTask("inflight_commit_cmd_test", payload)
		require.NoError(t, b.processInflightCommit(ctx, task))

		committed, err := b.blnk.GetTransaction(ctx, txnID)
		require.NoError(t, err)
		// Committing creates a child COMMIT entry; the parent leaves INFLIGHT.
		assert.NotEqual(t, "VOID", committed.Status)

		// Committing again is idempotent: the already-committed leg is terminal,
		// so the worker acks (nil) instead of double-committing.
		err = b.processInflightCommit(ctx, task)
		require.NoError(t, err, "a replayed commit must be acked, not errored")
	})

	t.Run("expiry voids", func(t *testing.T) {
		txnID := startInflight()
		payload, err := json.Marshal(txnID)
		require.NoError(t, err)

		task := asynq.NewTask("inflight_expiry_cmd_test", payload)
		require.NoError(t, b.processInflightExpiry(ctx, task))

		expired, err := b.blnk.GetTransaction(ctx, txnID)
		require.NoError(t, err)
		afterVoid, err := b.blnk.GetDataSource().GetBalanceByIDLite(expired.Source)
		require.NoError(t, err)
		assert.Equal(t, "0", afterVoid.InflightDebitBalance.String(), "expiry must release the inflight hold")

		// Voiding again is idempotent: the leg is already voided, so the worker
		// acks instead of releasing the amount a second time.
		err = b.processInflightExpiry(ctx, task)
		require.NoError(t, err, "a replayed expiry must be acked, not errored")
		afterReplay, err := b.blnk.GetDataSource().GetBalanceByIDLite(expired.Source)
		require.NoError(t, err)
		assert.Equal(t, "0", afterReplay.InflightDebitBalance.String(), "a replayed expiry must not move inflight again")
	})

	t.Run("malformed payload", func(t *testing.T) {
		task := asynq.NewTask("inflight_commit_cmd_test", []byte("{bad"))
		require.Error(t, b.processInflightCommit(ctx, task))
		require.Error(t, b.processInflightExpiry(ctx, task))
	})
}

// TestProcessInflightExpiry_CommittedQueuedHoldLeavesOtherHold is the regression
// for https://github.com/blnkfinance/blnk/issues/416.
//
// A queued inflight is two rows: the QUEUED placeholder returned to the caller,
// and the INFLIGHT "_q" row the worker applies. The expiry job is scheduled with
// the placeholder id. After the "_q" row has been committed, running that job
// must leave the source's inflight debit untouched, including an unrelated hold
// on the same balance.
func TestProcessInflightExpiry_CommittedQueuedHoldLeavesOtherHold(t *testing.T) {
	b := newCmdTestInstance(t)
	ctx := context.Background()
	stopWorker := startCmdTransactionWorker(t, b)
	defer stopWorker()

	src, dst := createBalancePair(t, b)

	other := queuedTransaction(src.BalanceID, dst.BalanceID, 200, true)
	other.Inflight = true
	other.SkipQueue = true
	otherRecorded, err := b.blnk.QueueTransaction(ctx, other)
	require.NoError(t, err)
	require.Equal(t, "INFLIGHT", otherRecorded.Status)

	expiring := queuedTransaction(src.BalanceID, dst.BalanceID, 100, true)
	expiring.Inflight = true
	expiring.InflightExpiryDate = time.Now().Add(30 * time.Minute)
	placeholder, err := b.blnk.QueueTransaction(ctx, expiring)
	require.NoError(t, err)
	require.Equal(t, "QUEUED", placeholder.Status)

	var holdID string
	require.Eventually(t, func() bool {
		hold, err := b.blnk.GetTransactionByRef(ctx, expiring.Reference+"_q")
		if err != nil || hold.Status != "INFLIGHT" {
			return false
		}
		holdID = hold.TransactionID
		return true
	}, 15*time.Second, 100*time.Millisecond, "worker must apply the queued inflight as the _q row")

	committed, err := b.blnk.CommitInflightTransaction(ctx, holdID, big.NewInt(0))
	require.NoError(t, err)
	require.Equal(t, "APPLIED", committed.Status)

	afterCommit, err := b.blnk.GetDataSource().GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	require.Equal(t, "20000", afterCommit.InflightDebitBalance.String(), "only the unrelated 200.00 hold remains inflight after the commit")
	require.Equal(t, "10000", afterCommit.DebitBalance.String())

	connOpt, err := workerRedisConnOpt(b.cnf)
	require.NoError(t, err)
	inspector := asynq.NewInspector(connOpt)
	t.Cleanup(func() { _ = inspector.Close() })

	info, err := inspector.GetTaskInfo(b.cnf.Queue.InflightExpiryQueue, placeholder.TransactionID)
	require.NoError(t, err)
	var scheduledID string
	require.NoError(t, json.Unmarshal(info.Payload, &scheduledID))
	require.Equal(t, placeholder.TransactionID, scheduledID, "expiry must be scheduled with the placeholder id")

	err = b.processInflightExpiry(ctx, asynq.NewTask(info.Type, info.Payload))
	require.NoError(t, err)

	afterExpiry, err := b.blnk.GetDataSource().GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	require.Equal(t, "20000", afterExpiry.InflightDebitBalance.String(), "expiry must not take the committed amount from the other hold")
	require.Equal(t, "10000", afterExpiry.DebitBalance.String(), "the committed debit must stay applied")

	voided, err := b.blnk.GetDataSource().IsParentTransactionVoid(ctx, placeholder.TransactionID)
	require.NoError(t, err)
	require.False(t, voided, "expiry must not void the placeholder once its _q hold is committed")
}

// startCmdTransactionWorker runs the transaction queue handler until the test ends.
func startCmdTransactionWorker(t *testing.T, b *blnkInstance) func() {
	t.Helper()
	connOpt, err := workerRedisConnOpt(b.cnf)
	require.NoError(t, err)

	queueName := fmt.Sprintf("%s_%d", b.cnf.Queue.TransactionQueue, 1)
	srv := asynq.NewServer(connOpt, asynq.Config{
		Concurrency: 1,
		Queues:      map[string]int{queueName: 1},
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(queueName, b.processTransaction)

	go func() {
		if err := srv.Run(mux); err != nil {
			t.Logf("transaction worker stopped: %v", err)
		}
	}()

	return func() { srv.Shutdown() }
}

func TestIndexData(t *testing.T) {
	b := newCmdTestInstance(t)
	ctx := context.Background()

	t.Run("disabled without typesense DNS", func(t *testing.T) {
		task := asynq.NewTask("index_queue_cmd_test", []byte("ignored"))
		require.NoError(t, b.indexData(ctx, task), "indexing must be a silent no-op when search is not configured")
	})

	b.cnf.TypeSenseKey = "blnk-api-key"
	b.cnf.TypeSense = config.TypeSenseConfig{Dns: "http://localhost:8108"}

	t.Run("malformed payload errors", func(t *testing.T) {
		task := asynq.NewTask("index_queue_cmd_test", []byte("{bad json"))
		require.Error(t, b.indexData(ctx, task))
		require.Error(t, b.indexBatchData(ctx, task))
	})

	t.Run("indexes a document into typesense", func(t *testing.T) {
		payload, err := json.Marshal(blnk.IndexTask{
			Collection: "ledgers",
			Payload: map[string]interface{}{
				"ledger_id":  "ldg_worker_idx_test",
				"name":       "worker indexed ledger",
				"created_at": time.Now().Format(time.RFC3339),
			},
		})
		require.NoError(t, err)

		task := asynq.NewTask("index_queue_cmd_test", payload)
		require.NoError(t, b.indexData(ctx, task))
	})
}

func TestRunWorkers_LifecycleStartsAndShutsDown(t *testing.T) {
	b := newCmdTestInstance(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWorkers(ctx, b, b.cnf) }()

	// The monitoring server proves the worker stack came up.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://localhost:" + b.cnf.Queue.MonitoringPort + "/health")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond, "worker monitoring server must come up")

	// Cancel = SIGTERM equivalent; the full stack must shut down cleanly.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(45 * time.Second):
		t.Fatal("runWorkers did not shut down after context cancellation")
	}

	// Monitoring server must be gone after shutdown.
	_, err := http.Get("http://localhost:" + b.cnf.Queue.MonitoringPort + "/health")
	require.Error(t, err, "monitoring server must stop with the workers")
}
