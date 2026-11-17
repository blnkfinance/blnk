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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An atomic batch that puts 100 into A and then pays those 100 from A to B
// lands whole when A and B start at 0: the worker runs the second item after
// the first, though the two have different sources and so go to different
// queues. Twelve batches of each kind are queued before the worker starts.
func TestWorker_AtomicBulkRunsItemsInTheOrderSent(t *testing.T) {
	cfg := realInfraConfig(t)
	suffix := strings.ToLower(gofakeit.LetterN(8))
	cfg.Queue.TransactionQueue = "transaction_queue_cmd_test_order_" + suffix
	cfg.Queue.WebhookQueue = "webhook_queue_cmd_test_order_" + suffix
	cfg.Queue.NumberOfQueues = 20
	cfg.Notification.Webhook.Url = "http://127.0.0.1:1/order"
	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")
	b := &blnkInstance{blnk: newBlnk, cnf: cfg}
	ds := b.blnk.GetDataSource()
	ctx := context.Background()

	var queues []string
	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queues = append(queues, fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i))
	}
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.Redis.Dns})
	t.Cleanup(func() {
		for _, queue := range queues {
			_ = inspector.DeleteQueue(queue, true)
		}
		_ = inspector.DeleteQueue(cfg.Queue.WebhookQueue, true)
		_ = inspector.Close()
	})
	newBalance := func() string {
		balance, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
		require.NoError(t, err)
		return balance.BalanceID
	}
	balanceOf := func(id string) string {
		balance, err := ds.GetBalanceByIDLite(id)
		require.NoError(t, err)
		return balance.Balance.String()
	}

	type batch struct {
		name, id, balanceA, balanceB string
		runAsync                     bool
	}
	var batches []*batch
	for _, runAsync := range []bool{false, true} {
		for run := 0; run < 12; run++ {
			bt := &batch{name: fmt.Sprintf("run_async=%v run %d", runAsync, run), balanceA: newBalance(), balanceB: newBalance(), runAsync: runAsync}
			funding, payer := newBalance(), newBalance()
			funded := queuedTransaction(funding, payer, 100, true)
			funded.SkipQueue = true
			_, err := b.blnk.QueueTransaction(ctx, funded)
			require.NoError(t, err)

			items := []*model.Transaction{
				{Reference: "ref_" + gofakeit.UUID(), Source: payer, Destination: bt.balanceA, Amount: 100, Precision: 100, Currency: "USD"},
				{Reference: "ref_" + gofakeit.UUID(), Source: bt.balanceA, Destination: bt.balanceB, Amount: 100, Precision: 100, Currency: "USD"},
			}
			result, err := b.blnk.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, RunAsync: runAsync, Transactions: items})
			require.NoError(t, err)
			bt.id = result.BatchID
			batches = append(batches, bt)
		}
	}

	srv, err := initializeWorkerServer(cfg, initializeQueues())
	require.NoError(t, err)
	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)
	require.NoError(t, srv.Start(mux))
	t.Cleanup(srv.Shutdown)
	deadline := time.Now().Add(60 * time.Second)
	for idle := time.Now(); time.Since(idle) < time.Second; time.Sleep(50 * time.Millisecond) {
		require.True(t, time.Now().Before(deadline), "the transaction queue did not drain")
		for _, queue := range queues {
			info, err := inspector.GetQueueInfo(queue)
			if err == nil && info.Pending+info.Active+info.Scheduled+info.Retry > 0 {
				idle = time.Now()
			}
		}
	}

	var landed int
	for _, bt := range batches {
		if assert.Equal(t, "0", balanceOf(bt.balanceA), "%s: A passes the 100 on", bt.name) &&
			assert.Equal(t, "10000", balanceOf(bt.balanceB), "%s: B receives the 100", bt.name) {
			landed++
		}
		hooks := bulkWebhooks(t, inspector, cfg.Queue.WebhookQueue, bt.id)
		if bt.runAsync {
			if assert.Len(t, hooks, 1, "%s: the batch is reported once", bt.name) {
				assert.Equal(t, "bulk_transaction.applied", hooks[0].Event, "%s", bt.name)
			}
		} else {
			assert.Empty(t, hooks, "%s: a sync batch answers the request and sends no webhook", bt.name)
		}
	}
	t.Logf("%d of %d batches landed whole", landed, len(batches))
}

// A retry of a batch task that stopped half way runs only the items that have
// not landed: nothing is paid twice, and the batch is reported once.
func TestWorker_AtomicBatchRetrySkipsLandedItems(t *testing.T) {
	bt := queueBulkRequest(t, model.BulkTransactionRequest{Atomic: true, RunAsync: true}, 50, 10)
	ctx := context.Background()

	// Someone else holds the lock of the 10's destination, so the first
	// attempt pays the 50 and stops at the 10.
	rdb := redis.NewClient(&redis.Options{Addr: bt.b.cnf.Redis.Dns})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Set(ctx, bt.dsts[1].BalanceID, "held-by-test", time.Minute).Err())

	require.Error(t, bt.b.processQueuedAtomicBatch(ctx, bt.batch), "the 10 cannot be paid while its balance is locked")
	assert.Equal(t, "5000", bt.balance(t, bt.dsts[0].BalanceID).Balance.String(), "the 50 is paid")
	assert.Equal(t, "0", bt.balance(t, bt.dsts[1].BalanceID).Balance.String())
	assert.Empty(t, bulkWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id), "nothing is reported while an item is still queued")

	require.NoError(t, rdb.Del(ctx, bt.dsts[1].BalanceID).Err())
	require.NoError(t, bt.b.processQueuedAtomicBatch(ctx, bt.batch), "the retry")
	require.NoError(t, bt.b.processQueuedAtomicBatch(ctx, bt.batch), "another delivery")

	assert.Equal(t, "4000", bt.balance(t, bt.src.BalanceID).Balance.String(), "the source pays 60 once")
	assert.Equal(t, "5000", bt.balance(t, bt.dsts[0].BalanceID).Balance.String(), "the 50 is paid once")
	assert.Equal(t, "1000", bt.balance(t, bt.dsts[1].BalanceID).Balance.String(), "the 10 is paid once")
	for _, item := range bt.items {
		assert.Equal(t, 1, bt.underBatch(t, `reference = '`+item.Reference+`'`), "%s is recorded once", item.Reference)
	}
	hooks := bulkWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id)
	require.Len(t, hooks, 1, "the batch is reported once")
	assert.Equal(t, "bulk_transaction.applied", hooks[0].Event)
}

// An item the worker refuses fails the batch: what the batch paid before it is
// rolled back, and the items after it are rejected without being applied.
func TestWorker_AtomicBatchRejectionRollsBackEarlierItemsAndRejectsLaterOnes(t *testing.T) {
	for _, inflight := range []bool{false, true} {
		t.Run(fmt.Sprintf("inflight=%v", inflight), func(t *testing.T) {
			bt := queueAtomicBatch(t, inflight, 50, 500, 10)
			ctx := context.Background()

			require.NoError(t, bt.b.processQueuedAtomicBatch(ctx, bt.batch))
			bt.assertRolledBack(t, "after the batch")
			assert.Equal(t, 2, bt.underBatch(t, `status = 'REJECTED'`), "the 500 and the 10 are rejected")
			assert.Equal(t, 0, bt.underBatch(t, `status = 'REJECTED' AND reference = '`+bt.items[0].Reference+`'`), "the 50 is rolled back, not rejected")
			rejected, err := bt.b.blnk.GetDataSource().GetTransactionByRef(ctx, bt.items[2].Reference)
			require.NoError(t, err)
			assert.Contains(t, rejected.MetaData["blnk_rejection_reason"], "another transaction in this atomic batch was rejected")

			messages := bulkFailedWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id)
			require.Len(t, messages, 1, "one bulk_transaction.failed per batch")
			kind, action := "applied", "refunded"
			if inflight {
				kind, action = "inflight", "voided"
			}
			assert.Contains(t, messages[0], fmt.Sprintf("1 %s transaction(s) from this batch have been %s", kind, action))

			require.NoError(t, bt.b.processQueuedAtomicBatch(ctx, bt.batch), "another delivery")
			bt.assertRolledBack(t, "after another delivery")
			assert.Equal(t, 2, bt.underBatch(t, `status = 'REJECTED'`))
			assert.Len(t, bulkFailedWebhooks(t, bt.inspector, bt.b.cnf.Queue.WebhookQueue, bt.id), 1)
		})
	}
}
