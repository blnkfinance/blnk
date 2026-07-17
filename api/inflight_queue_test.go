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
	"encoding/json"
	"math/big"
	"net/http"
	"testing"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInflightCommit_QueuedMultiSourceParent reproduces the bisect failure:
// a multi-source inflight created with skip_queue:true returns a parent
// transaction_id that sync commit (skip_queue:true) can expand via
// GetInflightTransactionsByParentID, but the default queued commit path
// (PUT {"status":"commit"} with no skip_queue) must also accept that same
// parent id — today QueueInflightAction pre-validates via
// fetchAndValidateInflightTransaction and 404s because the parent row itself
// is never persisted (only the split legs are).
func TestInflightCommit_QueuedMultiSourceParent(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	srcA, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	srcB, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dst, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	createPayload := model2.RecordTransaction{
		Amount:      20,
		Precision:   100,
		Reference:   "queued_ms_" + gofakeit.UUID(),
		Description: "queued multi-source inflight parent commit",
		Currency:    "USD",
		Sources: []model.Distribution{
			{Identifier: srcA.BalanceID, Distribution: "50%"},
			{Identifier: srcB.BalanceID, Distribution: "left"},
		},
		Destination:    dst.BalanceID,
		AllowOverDraft: true,
		Inflight:       true,
		SkipQueue:      true,
	}
	createRec := doJSON(router, "POST", "/transactions", createPayload)
	require.Equal(t, http.StatusCreated, createRec.Code, "create body: %s", createRec.Body.String())
	var created model.Transaction
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	require.Equal(t, "INFLIGHT", created.Status)
	require.NotEmpty(t, created.TransactionID, "create must return a parent id to commit against")

	assertQueuedInflightCommitAccepted(t, router, created.TransactionID)
}

// TestInflightCommit_QueuedMultiDestinationParent is the multi-destination
// counterpart of TestInflightCommit_QueuedMultiSourceParent: commit the create
// response's parent id via the default queued path.
func TestInflightCommit_QueuedMultiDestinationParent(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	src, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dstA, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dstB, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	createPayload := model2.RecordTransaction{
		Amount:      20,
		Precision:   100,
		Reference:   "queued_md_" + gofakeit.UUID(),
		Description: "queued multi-destination inflight parent commit",
		Currency:    "USD",
		Source:      src.BalanceID,
		Destinations: []model.Distribution{
			{Identifier: dstA.BalanceID, Distribution: "50%"},
			{Identifier: dstB.BalanceID, Distribution: "left"},
		},
		AllowOverDraft: true,
		Inflight:       true,
		SkipQueue:      true,
	}
	createRec := doJSON(router, "POST", "/transactions", createPayload)
	require.Equal(t, http.StatusCreated, createRec.Code, "create body: %s", createRec.Body.String())
	var created model.Transaction
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	require.Equal(t, "INFLIGHT", created.Status)
	require.NotEmpty(t, created.TransactionID, "create must return a parent id to commit against")

	assertQueuedInflightCommitAccepted(t, router, created.TransactionID)
}

// TestInflightCommit_QueuedBulkBatchParent commits a sync bulk inflight batch
// by its batch_id on the default queued path. batch_id is never a transaction
// row — only the per-item legs (parent_transaction = batch_id) are.
func TestInflightCommit_QueuedBulkBatchParent(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	srcA, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	srcB, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dstA, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dstB, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	ref := gofakeit.UUID()
	createPayload := model2.BulkTransactionRequest{
		Atomic:    true,
		Inflight:  true,
		RunAsync:  false,
		SkipQueue: true,
		Transactions: []*model2.RecordTransaction{
			{
				Amount: 7, Precision: 100, Reference: "queued_bulk_" + ref + "_1",
				Description: "queued bulk inflight batch commit 1", Currency: "USD",
				Source: srcA.BalanceID, Destination: dstA.BalanceID, AllowOverDraft: true,
			},
			{
				Amount: 8, Precision: 100, Reference: "queued_bulk_" + ref + "_2",
				Description: "queued bulk inflight batch commit 2", Currency: "USD",
				Source: srcB.BalanceID, Destination: dstB.BalanceID, AllowOverDraft: true,
			},
		},
	}
	createRec := doJSON(router, "POST", "/transactions/bulk", createPayload)
	require.Equal(t, http.StatusCreated, createRec.Code, "create body: %s", createRec.Body.String())
	var created map[string]interface{}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	batchID, _ := created["batch_id"].(string)
	require.NotEmpty(t, batchID, "bulk create must return batch_id")
	require.Equal(t, "inflight", created["status"], "bulk create status: %v", created["status"])

	assertQueuedInflightCommitAccepted(t, router, batchID)
}

// assertQueuedInflightCommitAccepted PUTs {"status":"commit"} (no skip_queue)
// and requires HTTP 200 with queued:true — the bisect COMMIT_MODE=queued contract.
func assertQueuedInflightCommitAccepted(t *testing.T, router *gin.Engine, commitID string) {
	t.Helper()
	commitRec := doJSON(router, "PUT", "/transactions/inflight/"+commitID,
		model2.InflightUpdate{Status: "commit"})
	require.Equal(t, http.StatusOK, commitRec.Code,
		"queued commit of parent/batch %s must be accepted (got %d: %s)",
		commitID, commitRec.Code, commitRec.Body.String())
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["queued"], "queued commit must be marked queued")
}

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
