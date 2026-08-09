package api

import (
	"net/http"
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
