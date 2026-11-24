package blnk

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// abortTestBatch is a queued atomic bulk request whose items wait in the
// queue with no worker serving it. The test hands them to work, which does
// what the queue worker does with an item, and fails the batch from the
// request side with abortQueuedAtomicBatch.
type abortTestBatch struct {
	b         *Blnk
	cnf       *config.Configuration
	inspector *asynq.Inspector
	db        *sql.DB
	id        string
	src       *model.Balance
	dsts      []*model.Balance
	items     []*model.Transaction
}

// newAbortTestBatch funds a source with 100 and queues an atomic bulk request
// that pays amounts from it, each to a new balance.
func newAbortTestBatch(t *testing.T, inflight bool, amounts ...float64) *abortTestBatch {
	t.Helper()
	suffix := strings.ToLower(gofakeit.LetterN(8))
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_bulk_abort_" + suffix,
			IndexQueue:       "index_queue_bulk_abort_" + suffix,
			TransactionQueue: "transaction_queue_bulk_abort_" + suffix,
			NumberOfQueues:   1,
		},
		Transaction: config.TransactionConfig{
			BatchSize:       100,
			MaxQueueSize:    1000,
			MaxWorkers:      2,
			LockDuration:    30 * time.Second,
			LockWaitTimeout: 100 * time.Millisecond,
		},
		// Webhooks are only enqueued here; nothing serves this queue.
		Notification: config.Notification{Webhook: config.WebhookConfig{Url: "http://127.0.0.1:1/bulk"}},
	}
	config.ConfigStore.Store(cnf)
	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Postgres and Redis must be running")
	b, err := NewBlnk(ds)
	require.NoError(t, err)
	ctx := context.Background()

	queue := cnf.Queue.TransactionQueue + "_1"
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cnf.Redis.Dns})
	db, err := sql.Open("postgres", cnf.DataSource.Dns)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queue, true)
		_ = inspector.DeleteQueue(cnf.Queue.WebhookQueue, true)
		_ = inspector.Close()
		_ = db.Close()
	})

	bt := &abortTestBatch{b: b, cnf: cnf, inspector: inspector, db: db}
	funding, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	bt.src = &src
	_, err = b.QueueTransaction(ctx, &model.Transaction{
		Reference: "ref_" + gofakeit.UUID(), Source: funding.BalanceID, Destination: src.BalanceID,
		Amount: 100, Precision: 100, Currency: "USD", AllowOverdraft: true, SkipQueue: true,
	})
	require.NoError(t, err)

	var items []*model.Transaction
	for _, amount := range amounts {
		dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
		require.NoError(t, err)
		bt.dsts = append(bt.dsts, &dst)
		items = append(items, &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: amount, Precision: 100, Currency: "USD"})
	}
	result, err := b.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, Inflight: inflight, Transactions: items})
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
	for _, task := range queued {
		var txn model.Transaction
		require.NoError(t, json.Unmarshal(task.Payload, &txn))
		require.Equal(t, bt.id, txn.MetaData["QUEUED_PARENT_TRANSACTION"])
		bt.items = append(bt.items, &txn)
	}
	sequence := func(txn *model.Transaction) float64 { s, _ := txn.MetaData["sequence"].(float64); return s }
	sort.Slice(bt.items, func(i, j int) bool { return sequence(bt.items[i]) < sequence(bt.items[j]) })
	return bt
}

// work handles a delivery of item i the way the queue worker does: under the
// batch lock, it applies the item, or rejects it once the batch has failed.
func (bt *abortTestBatch) work(t *testing.T, i int) error {
	t.Helper()
	ctx := context.Background()
	txn := bt.delivery(t, i)
	return bt.b.WithQueuedAtomicBatch(ctx, txn, func(batchFailed bool) error {
		if batchFailed {
			_, err := bt.b.RejectTransaction(ctx, txn, ErrAtomicBatchFailed.Error())
			return err
		}
		_, err := bt.b.ProcessQueuedTransaction(ctx, txn, false)
		return err
	})
}

// delivery returns item i as the worker decodes it from the queue.
func (bt *abortTestBatch) delivery(t *testing.T, i int) *model.Transaction {
	t.Helper()
	payload, err := json.Marshal(bt.items[i])
	require.NoError(t, err)
	var txn model.Transaction
	require.NoError(t, json.Unmarshal(payload, &txn))
	return &txn
}

func (bt *abortTestBatch) balance(t *testing.T, id string) *model.Balance {
	t.Helper()
	balance, err := bt.b.datasource.GetBalanceByIDLite(id)
	require.NoError(t, err)
	return balance
}

// assertRolledBack checks that the source is back at 100 and that nothing the
// batch paid stays applied, inflight or reserved.
func (bt *abortTestBatch) assertRolledBack(t *testing.T, when string) {
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
func (bt *abortTestBatch) underBatch(t *testing.T, where string) int {
	t.Helper()
	var n int
	require.NoError(t, bt.db.QueryRow(`SELECT count(*) FROM blnk.transactions
		WHERE meta_data->>'QUEUED_PARENT_TRANSACTION' = $1 AND `+where, bt.id).Scan(&n))
	return n
}

// failedWebhooks counts the bulk_transaction.failed webhooks queued for the batch.
func (bt *abortTestBatch) failedWebhooks(t *testing.T) int {
	t.Helper()
	tasks, err := bt.inspector.ListPendingTasks(bt.cnf.Queue.WebhookQueue, asynq.PageSize(1000))
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return 0
	}
	require.NoError(t, err)
	n := 0
	for _, task := range tasks {
		var hook struct {
			Event string `json:"event"`
			Data  struct {
				BatchID string `json:"batch_id"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(task.Payload, &hook))
		if hook.Event == "bulk_transaction.failed" && hook.Data.BatchID == bt.id {
			n++
		}
	}
	return n
}

// An item can still be landing when the request fails the batch, holding the
// batch lock past the lock wait. The batch is marked failed anyway, so the
// worker takes no further item, and the rollback waits for the item to land
// and reverses it.
func TestAbortQueuedAtomicBatch_WaitsForAnItemBeingApplied(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(map[bool]string{false: "applied", true: "inflight"}[inflight], func(t *testing.T) {
			bt := newAbortTestBatch(t, inflight, 50, 10)
			ctx := context.Background()

			// The worker has taken the batch lock for item 0 and found the
			// batch not failed: item 0 is being applied.
			workerLock := redlock.NewLocker(bt.b.redis, atomicBatchLockKey(bt.id), "worker")
			require.NoError(t, workerLock.Lock(ctx, bt.cnf.Transaction.LockDuration))

			type abortResult struct {
				count int
				err   error
			}
			aborted := make(chan abortResult, 1)
			go func() {
				count, err := bt.b.abortQueuedAtomicBatch(ctx, bt.id, inflight, nil)
				aborted <- abortResult{count, err}
			}()

			// The lock wait runs out and the batch is marked failed while
			// item 0 is still landing.
			require.Eventually(t, func() bool {
				n, err := bt.b.redis.Exists(ctx, atomicBatchFailedKey(bt.id)).Result()
				return err == nil && n > 0
			}, 10*time.Second, 20*time.Millisecond, "the batch must be marked failed when the lock wait runs out")
			select {
			case res := <-aborted:
				t.Fatalf("the rollback must wait for the item being applied, returned %d, %v", res.count, res.err)
			case <-time.After(200 * time.Millisecond):
			}

			_, err := bt.b.ProcessQueuedTransaction(ctx, bt.delivery(t, 0), false)
			require.NoError(t, err, "item 0 lands")
			require.NoError(t, workerLock.Unlock(ctx))

			var res abortResult
			select {
			case res = <-aborted:
			case <-time.After(20 * time.Second):
				t.Fatal("the rollback did not finish once the lock was free")
			}
			require.NoError(t, res.err)
			assert.Equal(t, 1, res.count, "item 0 is reversed")
			bt.assertRolledBack(t, "after the abort")
			if inflight {
				assert.Equal(t, 1, bt.underBatch(t, `status = 'VOID'`), "the void is listed under the batch id")
			} else {
				assert.Equal(t, 1, bt.underBatch(t, `status = 'APPLIED' AND reference = '`+model.RefundReference(bt.items[0].TransactionID)+`'`),
					"the refund is listed under the batch id")
			}

			// The worker then gets item 1, and item 0 again.
			require.NoError(t, bt.work(t, 1))
			require.NoError(t, bt.work(t, 0))
			bt.assertRolledBack(t, "after the worker")
			assert.Equal(t, 1, bt.underBatch(t, `status = 'REJECTED'`), "item 1 is rejected, once")
			assert.Zero(t, bt.failedWebhooks(t), "the request reports the failure; the worker reports nothing")
		})
	}
}

// A hold already written is voided by the abort, and a hold still queued is
// rejected by the worker: nothing stays reserved on either side.
func TestAbortQueuedAtomicBatch_VoidsWrittenHoldAndRejectsQueuedHold(t *testing.T) {
	bt := newAbortTestBatch(t, true, 50, 10)
	ctx := context.Background()

	require.NoError(t, bt.work(t, 0), "the 50 hold is written")
	src := bt.balance(t, bt.src.BalanceID)
	require.Equal(t, "5000", src.InflightDebitBalance.String())

	count, err := bt.b.abortQueuedAtomicBatch(ctx, bt.id, true, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "the written hold is voided")

	require.NoError(t, bt.work(t, 1), "the queued hold is rejected")
	bt.assertRolledBack(t, "after the worker")
	assert.Equal(t, 1, bt.underBatch(t, `status = 'VOID'`), "the 50 hold is voided")
	assert.Equal(t, 1, bt.underBatch(t, `status = 'REJECTED'`), "the queued hold is rejected")
	rejected, err := bt.b.datasource.GetTransactionByRef(ctx, bt.items[1].Reference)
	require.NoError(t, err)
	assert.Equal(t, StatusRejected, rejected.Status)
	assert.Zero(t, bt.failedWebhooks(t))
}

// When the abort's rollback fails, the batch is left unreported. The first
// rejected item finishes the rollback, retried until it succeeds, and reports
// the batch once; a duplicate delivery changes nothing.
func TestAbortQueuedAtomicBatch_FailedRollbackIsFinishedByTheWorker(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(map[bool]string{false: "applied", true: "inflight"}[inflight], func(t *testing.T) {
			bt := newAbortTestBatch(t, inflight, 50, 10)
			ctx := context.Background()
			paid := bt.items[0]
			require.NoError(t, bt.work(t, 0), "the 50 is paid")

			// Someone else holds the lock the refund (or void) of the 50
			// needs, so the rollback fails after the batch is marked.
			lockKey := "refund:" + paid.TransactionID
			if inflight {
				lockKey = "inflight-commit:" + paid.TransactionID
			}
			require.NoError(t, bt.b.redis.Set(ctx, lockKey, "held-by-test", time.Minute).Err())

			_, err := bt.b.abortQueuedAtomicBatch(ctx, bt.id, inflight, nil)
			require.Error(t, err, "the abort's rollback fails")
			reported, err := bt.b.redis.Exists(ctx, atomicBatchReportedKey(bt.id)).Result()
			require.NoError(t, err)
			assert.Zero(t, reported, "a batch whose rollback failed is not reported")

			// The worker rejects item 1; its rollback fails too, so the item
			// is retried.
			assert.Error(t, bt.work(t, 1), "the worker retries the item while the rollback fails")
			assert.Zero(t, bt.failedWebhooks(t), "nothing is reported before the rollback is complete")

			require.NoError(t, bt.b.redis.Del(ctx, lockKey).Err())
			require.NoError(t, bt.work(t, 1), "the retry finishes the rollback")
			bt.assertRolledBack(t, "after the retry")
			assert.Equal(t, 1, bt.failedWebhooks(t), "the batch is reported once its rollback is complete")

			// A duplicate delivery of either item changes nothing.
			require.NoError(t, bt.work(t, 1))
			require.NoError(t, bt.work(t, 0))
			bt.assertRolledBack(t, "after a duplicate delivery")
			assert.Equal(t, 1, bt.underBatch(t, `status = 'REJECTED'`), "item 1 is recorded once")
			assert.Equal(t, 1, bt.failedWebhooks(t), "the batch is reported once")
		})
	}
}

// retryTasks returns the queued rollback retries of the batch.
func (bt *abortTestBatch) retryTasks(t *testing.T) []*asynq.TaskInfo {
	t.Helper()
	tasks, err := bt.inspector.ListPendingTasks(bt.cnf.Queue.WebhookQueue, asynq.PageSize(1000))
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return nil
	}
	require.NoError(t, err)
	var retries []*asynq.TaskInfo
	for _, task := range tasks {
		var p atomicBatchFailurePayload
		if task.Type == AtomicBatchFailureTask && json.Unmarshal(task.Payload, &p) == nil && p.BatchID == bt.id {
			retries = append(retries, task)
		}
	}
	return retries
}

// A background batch whose every item has been taken fails, and the refund of
// what it applied fails. No queued item is left to finish it, so the request
// side queues a retry, which refunds and reports the batch once.
func TestAsyncAtomicFailure_FailedRefundIsRetriedWithNothingQueued(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(map[bool]string{false: "applied", true: "inflight"}[inflight], func(t *testing.T) {
			bt := newAbortTestBatch(t, inflight, 50)
			ctx := context.Background()
			paid := bt.items[0]
			require.NoError(t, bt.work(t, 0), "the 50 is paid")

			lockKey := "refund:" + paid.TransactionID
			if inflight {
				lockKey = "inflight-commit:" + paid.TransactionID
			}
			require.NoError(t, bt.b.redis.Set(ctx, lockKey, "held-by-test", time.Minute).Err())

			bt.b.handleAsyncBulkTransactionFailure(ctx, errors.New("the next item failed"), bt.id, true, inflight, true)
			assert.Zero(t, bt.failedWebhooks(t), "nothing is reported before the rollback is complete")
			retries := bt.retryTasks(t)
			require.Len(t, retries, 1, "the failed rollback is queued for a retry")
			retry := asynq.NewTask(retries[0].Type, retries[0].Payload)

			assert.Error(t, bt.b.ProcessAtomicBatchFailure(ctx, retry), "the retry fails while the refund still fails")
			assert.Zero(t, bt.failedWebhooks(t))

			require.NoError(t, bt.b.redis.Del(ctx, lockKey).Err())
			require.NoError(t, bt.b.ProcessAtomicBatchFailure(ctx, retry), "the retry finishes the rollback")
			bt.assertRolledBack(t, "after the retry")
			assert.Equal(t, 1, bt.failedWebhooks(t), "the batch is reported once its rollback is complete")

			require.NoError(t, bt.b.ProcessAtomicBatchFailure(ctx, retry))
			require.NoError(t, bt.work(t, 0))
			assert.Equal(t, 1, bt.failedWebhooks(t), "a duplicate retry or delivery reports nothing more")
		})
	}
}

// A background batch that fails and rolls back is reported once, by the
// request side, and the worker reports nothing for the items it then rejects.
func TestAsyncAtomicFailure_ReportedOnceByTheRequest(t *testing.T) {
	bt := newAbortTestBatch(t, false, 50, 10)
	ctx := context.Background()
	require.NoError(t, bt.work(t, 0), "the 50 is paid")

	bt.b.handleAsyncBulkTransactionFailure(ctx, errors.New("the batch failed"), bt.id, true, false, true)
	bt.assertRolledBack(t, "after the request")
	assert.Equal(t, 1, bt.failedWebhooks(t), "the request reports the failure")
	assert.Empty(t, bt.retryTasks(t), "nothing is left to retry")
	reported, err := bt.b.redis.Exists(ctx, atomicBatchReportedKey(bt.id)).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), reported)

	require.NoError(t, bt.work(t, 1))
	require.NoError(t, bt.work(t, 0))
	bt.assertRolledBack(t, "after the worker")
	assert.Equal(t, 1, bt.failedWebhooks(t), "the worker reports nothing more")
}
