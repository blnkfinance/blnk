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
	"net/http"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
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

		// Voiding again must fail: it has already been voided.
		err = b.processInflightExpiry(ctx, task)
		require.Error(t, err, "double void must surface an error")
	})

	t.Run("malformed payload", func(t *testing.T) {
		task := asynq.NewTask("inflight_commit_cmd_test", []byte("{bad"))
		require.Error(t, b.processInflightCommit(ctx, task))
		require.Error(t, b.processInflightExpiry(ctx, task))
	})
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
		payload, err := json.Marshal(indexData{
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
