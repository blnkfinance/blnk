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
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateBulkTransactions_SemaphoreSaturated asserts that when the async-bulk
// semaphore is fully held, an async request fails fast with a typed rate-limited
// error and a nil result, so the API layer reports 429 instead of dereferencing
// the nil result and panicking.
func TestCreateBulkTransactions_SemaphoreSaturated(t *testing.T) {
	// Hold every slot so the next TryAcquire fails.
	require.NoError(t, asyncBulkSemaphore.Acquire(context.Background(), 100))
	defer asyncBulkSemaphore.Release(100)

	l := &Blnk{}
	result, err := l.CreateBulkTransactions(context.Background(), &model.BulkTransactionRequest{
		RunAsync:     true,
		Transactions: []*model.Transaction{{Reference: "ref_saturated"}},
	})

	require.Error(t, err)
	assert.Nil(t, result, "a saturated semaphore must return a nil result")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrRateLimited, apiErr.Code)
}

// A scheduled item of a queued atomic bulk request keeps a task of its own,
// due when it is scheduled; the other items go to the batch's task.
func TestCreateBulkTransactions_ScheduledAtomicItemKeepsItsOwnTask(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	ctx := context.Background()
	cfg := b.Config()
	cfg.Queue.TransactionQueue = "transaction_queue_bulk_" + strings.ToLower(gofakeit.LetterN(8))
	queue := cfg.Queue.TransactionQueue + "_1"
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.Redis.Dns})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queue, true)
		_ = inspector.Close()
	})

	src, dst := newBalancePair(t, ds)
	now := &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: 10, Precision: 100, Currency: "USD", AllowOverdraft: true}
	later := &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: 20, Precision: 100, Currency: "USD", AllowOverdraft: true, ScheduledFor: time.Now().Add(time.Hour)}
	result, err := b.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, Transactions: []*model.Transaction{now, later}})
	require.NoError(t, err)

	task, err := inspector.GetTaskInfo(queue, result.BatchID)
	require.NoError(t, err)
	var batch QueuedAtomicBatch
	require.NoError(t, json.Unmarshal(task.Payload, &batch))
	require.Len(t, batch.Transactions, 1)
	assert.Equal(t, now.Reference+"_q", batch.Transactions[0].Reference)

	scheduled, err := inspector.ListScheduledTasks(queue)
	require.NoError(t, err)
	require.Len(t, scheduled, 1)
	var txn model.Transaction
	require.NoError(t, json.Unmarshal(scheduled[0].Payload, &txn))
	assert.Equal(t, later.Reference+"_q", txn.Reference)
}
