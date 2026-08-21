package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// TestDryRunInflightCommitProjectsSettlement checks a committed hold is shown
// moving from inflight into the settled balance, without settling it.
func TestDryRunInflightCommitProjectsSettlement(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdcommit_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	payload, err := request.ToJsonReq(&map[string]interface{}{"dry_run": true, "status": "commit"})
	require.NoError(t, err)

	var preview model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: payload, Response: &preview, Method: http.MethodPut,
		Route: "/transactions/inflight/" + hold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, preview.DryRun)
	assert.Equal(t, "commit", preview.Operation)
	assert.True(t, preview.WouldApply)
	require.Len(t, preview.Balances, 2)

	// The hold is released and the money actually moves.
	assert.Equal(t, "10000", preview.Balances[0].CurrentInflightDebitBalance)
	assert.Equal(t, "0", preview.Balances[0].ResultingInflightDebitBalance)

	// And the hold is still open afterwards.
	after, err := b.GetTransaction(t.Context(), hold.TransactionID)
	require.NoError(t, err)
	assert.Equal(t, "INFLIGHT", after.Status, "a dry run must not settle the hold")
}

// TestDryRunInflightVoidIgnoresAmount pins that a void always releases the
// whole remaining hold: the endpoint has no partial void, so an amount sent
// with one is reported as ignored rather than silently honoured.
func TestDryRunInflightVoidIgnoresAmount(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdvoid_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	payload, err := request.ToJsonReq(&map[string]interface{}{
		"dry_run": true, "status": "void", "precise_amount": 4000,
	})
	require.NoError(t, err)

	var preview model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: payload, Response: &preview, Method: http.MethodPut,
		Route: "/transactions/inflight/" + hold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "void", preview.Operation)
	assert.True(t, preview.WouldApply)

	// The full 100.00 hold is released, not the 40.00 that was asked for.
	assert.Equal(t, "10000", preview.PreciseAmount)

	found := false
	for _, note := range preview.Notes {
		if note == "amount is ignored when voiding; a void always releases the full remaining hold" {
			found = true
		}
	}
	assert.True(t, found, "the caller must be told the amount was ignored")
}

// TestDryRunInflightWritesNothing checks the projection leaves the hold and the
// balances exactly as they were.
func TestDryRunInflightWritesNothing(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdnone_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	before, err := b.GetBalanceByID(t.Context(), source, nil, false)
	require.NoError(t, err)

	payload, err := request.ToJsonReq(&map[string]interface{}{"dry_run": true, "status": "commit"})
	require.NoError(t, err)

	var preview model.TransactionPreview
	_, err = SetUpTestRequest(TestRequest{
		Payload: payload, Response: &preview, Method: http.MethodPut,
		Route: "/transactions/inflight/" + hold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	after, err := b.GetBalanceByID(t.Context(), source, nil, false)
	require.NoError(t, err)

	assert.Equal(t, before.Balance.String(), after.Balance.String())
	assert.Equal(t, before.InflightDebitBalance.String(), after.InflightDebitBalance.String())
	assert.Equal(t, before.Version, after.Version, "version moves on any persisted balance write")
}

// TestDryRunInflightRejectsUnknownStatus checks the action is validated the
// same way a real settlement validates it.
func TestDryRunInflightRejectsUnknownStatus(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdbad_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	payload, err := request.ToJsonReq(&map[string]interface{}{"dry_run": true, "status": "settle"})
	require.NoError(t, err)

	var body map[string]interface{}
	resp, err := SetUpTestRequest(TestRequest{
		Payload: payload, Response: &body, Method: http.MethodPut,
		Route: "/transactions/inflight/" + hold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.Code, "an unsupported action is a malformed request, not a projection")
}

// TestDryRunInflightRejectsOverLimitAmount pins the first defect reported on
// this PR: a commit for more than the original amount previewed as
// would_apply: true. The dry-run branch passed only precise_amount to the
// projection, so a plain `amount` never reached it, and nil there means
// "commit the full remaining amount" — the oversized value was discarded
// rather than checked.
func TestDryRunInflightRejectsOverLimitAmount(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdover_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	// amount, not precise_amount: the field that used to be dropped.
	payload, err := request.ToJsonReq(&map[string]interface{}{
		"dry_run": true, "status": "commit", "amount": 999999,
	})
	require.NoError(t, err)

	var preview model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: payload, Response: &preview, Method: http.MethodPut,
		Route: "/transactions/inflight/" + hold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.False(t, preview.WouldApply, "committing above the original amount cannot apply")
	require.NotNil(t, preview.Rejection)
	assert.Contains(t, preview.Rejection.Message, "cannot commit more than the original transaction amount")
}

// TestDryRunInflightReportsStatusAndAmount pins the second defect reported on
// this PR: the inflight projection left status and amount at their zero
// values, so the response read as though it had answered "" and 0. Both must
// carry what the real endpoint returns for the same call.
func TestDryRunInflightReportsStatusAndAmount(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	commitHold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdfields_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	payload, err := request.ToJsonReq(&map[string]interface{}{"dry_run": true, "status": "commit"})
	require.NoError(t, err)

	var commitPreview model.TransactionPreview
	_, err = SetUpTestRequest(TestRequest{
		Payload: payload, Response: &commitPreview, Method: http.MethodPut,
		Route: "/transactions/inflight/" + commitHold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	// APPLIED is what a real commit returns, not the internal COMMIT marker.
	assert.Equal(t, "APPLIED", commitPreview.Status)
	assert.Equal(t, float64(100), commitPreview.Amount)
	assert.Equal(t, "10000", commitPreview.PreciseAmount)

	voidHold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "holdfieldsv_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      40,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	voidPayload, err := request.ToJsonReq(&map[string]interface{}{"dry_run": true, "status": "void"})
	require.NoError(t, err)

	var voidPreview model.TransactionPreview
	_, err = SetUpTestRequest(TestRequest{
		Payload: voidPayload, Response: &voidPreview, Method: http.MethodPut,
		Route: "/transactions/inflight/" + voidHold.TransactionID, Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, "VOID", voidPreview.Status)
	assert.Equal(t, float64(40), voidPreview.Amount)
	assert.Equal(t, "4000", voidPreview.PreciseAmount)
}

// TestDryRunBulkInflightWritesNothing pins the most serious defect found on
// this branch: the bulk commit and void endpoints had no dry_run field, and
// Go's decoder drops unknown JSON keys silently, so dry_run: true was not
// rejected — it was ignored, and the batch really settled. The response was an
// ordinary success, so a caller who believed they were previewing got a
// settled batch and no indication otherwise.
func TestDryRunBulkInflightWritesNothing(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	newHold := func(ref string, amount float64) *model.Transaction {
		hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
			Reference:   ref + "_" + model.GenerateUUIDWithSuffix("ref"),
			Source:      source,
			Destination: destination,
			Amount:      amount,
			Precision:   100,
			Currency:    "USD",
			Inflight:    true,
			SkipQueue:   true,
		})
		require.NoError(t, err)
		return hold
	}

	commitHold := newHold("bulkcommit", 50)
	voidHold := newHold("bulkvoid", 30)

	commitPayload, err := request.ToJsonReq(&map[string]interface{}{
		"dry_run": true, "skip_queue": true,
		"transactions": []map[string]interface{}{{"transaction_id": commitHold.TransactionID}},
	})
	require.NoError(t, err)

	var commitPreview model.BulkTransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: commitPayload, Response: &commitPreview, Method: http.MethodPost,
		Route: "/transactions/inflight/bulk/commit", Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, commitPreview.DryRun)
	assert.True(t, commitPreview.WouldApply)
	// Items run on a worker pool with no ordering, so they are projected
	// independently and no combined batch balance is claimed.
	assert.False(t, commitPreview.Cumulative)
	require.Len(t, commitPreview.Results, 1)
	assert.Equal(t, "5000", commitPreview.Results[0].PreciseAmount)

	voidPayload, err := request.ToJsonReq(&map[string]interface{}{
		"dry_run": true, "skip_queue": true,
		"transaction_ids": []string{voidHold.TransactionID},
	})
	require.NoError(t, err)

	var voidPreview model.BulkTransactionPreview
	resp, err = SetUpTestRequest(TestRequest{
		Payload: voidPayload, Response: &voidPreview, Method: http.MethodPost,
		Route: "/transactions/inflight/bulk/void", Router: router,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, voidPreview.WouldApply)

	// The point of the test: both holds are untouched.
	afterCommit, err := b.GetTransaction(t.Context(), commitHold.TransactionID)
	require.NoError(t, err)
	assert.Equal(t, "INFLIGHT", afterCommit.Status, "a dry run must not settle the hold")

	afterVoid, err := b.GetTransaction(t.Context(), voidHold.TransactionID)
	require.NoError(t, err)
	assert.Equal(t, "INFLIGHT", afterVoid.Status, "a dry run must not release the hold")
}

// TestDryRunBulkInflightReportsRepeatedID pins that a transaction id repeated
// in one batch is reported. Each occurrence is projected against the same
// standing hold, so every one of them reports would_apply, while really
// running the batch settles the hold once and fails the rest. Which
// occurrence wins is decided by the worker pool, so the duplicate is reported
// rather than resolved.
func TestDryRunBulkInflightReportsRepeatedID(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	hold, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "bulkdupe_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      50,
		Precision:   100,
		Currency:    "USD",
		Inflight:    true,
		SkipQueue:   true,
	})
	require.NoError(t, err)

	payload, err := request.ToJsonReq(&map[string]interface{}{
		"dry_run": true, "skip_queue": true,
		"transactions": []map[string]interface{}{
			{"transaction_id": hold.TransactionID},
			{"transaction_id": hold.TransactionID},
		},
	})
	require.NoError(t, err)

	var preview model.BulkTransactionPreview
	_, err = SetUpTestRequest(TestRequest{
		Payload: payload, Response: &preview, Method: http.MethodPost,
		Route: "/transactions/inflight/bulk/commit", Router: router,
	})
	require.NoError(t, err)

	var found bool
	for _, note := range preview.Notes {
		if strings.Contains(note, "appears more than once") {
			found = true
			assert.Contains(t, note, hold.TransactionID)
		}
	}
	assert.True(t, found, "a repeated transaction id must be reported: %v", preview.Notes)
}
