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
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
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
