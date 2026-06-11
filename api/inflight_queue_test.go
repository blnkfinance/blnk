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
package api

import (
	"bytes"
	"context"
	"math/big"
	"net/http"
	"testing"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInflightCommit_QueuedByDefault proves the default (no skip_queue) commit is
// enqueued, not applied: the response is the still-inflight parent with
// queued:true, the DB row stays INFLIGHT (no worker running), and a second
// commit while the first is queued is rejected with 409.
func TestInflightCommit_QueuedByDefault(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	src, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dst, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	txnID := createInflightForBulk(t, router, src.BalanceID, dst.BalanceID, "USD", 100)

	commitBody := func() *bytes.Buffer {
		body, _ := request.ToJsonReq(&model2.InflightUpdate{Status: "commit"})
		return body
	}

	var resp map[string]interface{}
	rec, err := SetUpTestRequest(TestRequest{
		Payload:  commitBody(),
		Response: &resp,
		Method:   "PUT",
		Route:    "/transactions/inflight/" + txnID,
		Router:   router,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, true, resp["queued"], "queued commit must be marked queued")
	assert.Equal(t, "INFLIGHT", resp["status"], "response is the still-inflight parent")

	dbTxn, err := b.GetTransaction(ctx, txnID)
	require.NoError(t, err)
	assert.Equal(t, "INFLIGHT", dbTxn.Status, "no worker running, so the row stays INFLIGHT")

	// Second commit while the first is still queued -> 409.
	var resp2 map[string]interface{}
	rec2, err := SetUpTestRequest(TestRequest{
		Payload:  commitBody(),
		Response: &resp2,
		Method:   "PUT",
		Route:    "/transactions/inflight/" + txnID,
		Router:   router,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, rec2.Code, "a second queued commit must be rejected")
}

// TestRunInflightActionByParent_PartialCommitIdempotent drives the worker
// entrypoint directly: a partial commit replayed with the same ActionID must not
// double-apply — the deterministic per-leg reference collides on the unique index
// and the leg is skipped, leaving balances unchanged.
func TestRunInflightActionByParent_PartialCommitIdempotent(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	src, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dst, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	// Amount 100 @ precision 100 -> precise 10000.
	txnID := createInflightForBulk(t, router, src.BalanceID, dst.BalanceID, "USD", 100)

	actionID := "act_" + gofakeit.UUID()
	partial := big.NewInt(5000)

	// balances reads both legs and asserts the committed (main) balances.
	balances := func(t *testing.T, wantSrc, wantDst string, msg string) {
		t.Helper()
		s, err := b.GetBalanceByID(ctx, src.BalanceID, nil, false)
		require.NoError(t, err)
		d, err := b.GetBalanceByID(ctx, dst.BalanceID, nil, false)
		require.NoError(t, err)
		assert.Equal(t, wantSrc, s.Balance.String(), "source "+msg)
		assert.Equal(t, wantDst, d.Balance.String(), "destination "+msg)
	}

	// First partial commit moves exactly 5000 from source to destination.
	retryable, err := b.RunInflightActionByParent(ctx, txnID, "commit", partial, actionID)
	require.NoError(t, err)
	assert.False(t, retryable)
	balances(t, "-5000", "5000", "after first commit")

	// Replay with the same ActionID and amount: idempotent no-op. Both legs must
	// be unchanged — the deterministic per-leg reference collides on the unique
	// index, so nothing is re-applied.
	retryable2, err := b.RunInflightActionByParent(ctx, txnID, "commit", partial, actionID)
	require.NoError(t, err, "a replayed partial commit must not error out the task")
	assert.False(t, retryable2)
	balances(t, "-5000", "5000", "after replay must be unchanged")
}
