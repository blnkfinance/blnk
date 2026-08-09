package api

import (
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blnk "github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// newDryRunFixture creates a ledger with a funded source balance and an empty
// destination, and returns their ids.
func newDryRunFixture(t *testing.T, b *blnk.Blnk) (string, string) {
	t.Helper()

	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)

	source, err := b.CreateBalance(t.Context(), model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	destination, err := b.CreateBalance(t.Context(), model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	// Fund the source from the world account so it has something to spend.
	funding := &model.Transaction{
		Reference:      "fund_" + model.GenerateUUIDWithSuffix("ref"),
		Source:         "@World",
		Destination:    source.BalanceID,
		Amount:         500,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: true,
		SkipQueue:      true,
	}
	_, err = b.QueueTransaction(t.Context(), funding)
	require.NoError(t, err)

	return source.BalanceID, destination.BalanceID
}

// TestDryRunTransactionReturns200AndWritesNothing covers the contract at the
// HTTP boundary: a projection answers with 200 rather than 201, and the
// reference it used is still free afterwards.
func TestDryRunTransactionReturns200AndWritesNothing(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)
	reference := "dryrun_" + model.GenerateUUIDWithSuffix("ref")

	payload := model2.RecordTransaction{
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		Source:      source,
		Destination: destination,
		Reference:   reference,
		Description: "dry run projection",
		DryRun:      true,
	}

	body, err := request.ToJsonReq(&payload)
	require.NoError(t, err)

	var preview model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload:  body,
		Response: &preview,
		Method:   http.MethodPost,
		Route:    "/transactions",
		Router:   router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code, "a dry run creates nothing, so it must not answer 201")
	assert.True(t, preview.DryRun)
	assert.True(t, preview.WouldApply)
	require.Len(t, preview.Balances, 2)
	assert.Equal(t, "50000", preview.Balances[0].CurrentBalance)
	assert.Equal(t, "40000", preview.Balances[0].ResultingBalance)
	assert.Equal(t, "10000", preview.Balances[1].ResultingBalance)

	// Nothing was recorded, so the reference is still available.
	_, err = b.GetTransactionByRef(t.Context(), reference)
	assert.Error(t, err, "a dry run must not persist a transaction")

	// And the balances did not move.
	after, err := b.GetBalanceByID(t.Context(), source, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "50000", after.Balance.String(), "a dry run must not change balances")
}

// TestDryRunTransactionDoesNotConsumeReference checks the idempotency promise:
// previewing with a reference leaves it usable for the real post.
func TestDryRunTransactionDoesNotConsumeReference(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)
	reference := "shared_" + model.GenerateUUIDWithSuffix("ref")

	preview := model2.RecordTransaction{
		Amount: 100, Precision: 100, Currency: "USD",
		Source: source, Destination: destination, Reference: reference,
		Description: "dry run projection", DryRun: true,
	}
	body, err := request.ToJsonReq(&preview)
	require.NoError(t, err)

	var previewResp model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: body, Response: &previewResp, Method: http.MethodPost, Route: "/transactions", Router: router,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	// The same reference must still be accepted for a real post.
	real := preview
	real.DryRun = false
	real.SkipQueue = true
	realBody, err := request.ToJsonReq(&real)
	require.NoError(t, err)

	var created model.Transaction
	realResp, err := SetUpTestRequest(TestRequest{
		Payload: realBody, Response: &created, Method: http.MethodPost, Route: "/transactions", Router: router,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, realResp.Code, "the dry run must not have consumed the reference")
}

// TestDryRunTransactionProjectsRejection checks a projected rejection is a 200
// carrying the same error code a real post would have returned.
func TestDryRunTransactionProjectsRejection(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	payload := model2.RecordTransaction{
		Amount: 9999, Precision: 100, Currency: "USD",
		Source: source, Destination: destination,
		Reference:   "reject_" + model.GenerateUUIDWithSuffix("ref"),
		Description: "dry run projection",
		DryRun:      true,
	}
	body, err := request.ToJsonReq(&payload)
	require.NoError(t, err)

	var preview model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: body, Response: &preview, Method: http.MethodPost, Route: "/transactions", Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code, "a projected rejection is a successful answer of \"no\"")
	assert.False(t, preview.WouldApply)
	require.NotNil(t, preview.Rejection)
	assert.Equal(t, "TXN_INSUFFICIENT_FUNDS", preview.Rejection.Code)
	assert.Equal(t, "insufficient_funds", preview.Rejection.Reason)
}

// TestDryRunTransactionOverridesSkipQueue checks the precedence rule: a
// projection is always immediate, whatever queueing the caller asked for.
func TestDryRunTransactionOverridesSkipQueue(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	payload := model2.RecordTransaction{
		Amount: 100, Precision: 100, Currency: "USD",
		Source: source, Destination: destination,
		Reference:   "override_" + model.GenerateUUIDWithSuffix("ref"),
		Description: "dry run projection",
		DryRun:      true,
		SkipQueue:   false,
	}
	body, err := request.ToJsonReq(&payload)
	require.NoError(t, err)

	var preview model.TransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: body, Response: &preview, Method: http.MethodPost, Route: "/transactions", Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, preview.DryRun)
	require.Len(t, preview.Balances, 2)
}

// TestDryRunRefundProjectsReversal covers the refund projection: the reversal
// is described, but the parent is not marked refunded.
func TestDryRunRefundProjectsReversal(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	applied, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "torefund_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		SkipQueue:   true,
	})
	require.NoError(t, err)

	var preview model.TransactionPreview
	body, err := request.ToJsonReq(&map[string]interface{}{"dry_run": true})
	require.NoError(t, err)

	resp, err := SetUpTestRequest(TestRequest{
		Payload:  body,
		Response: &preview,
		Method:   http.MethodPost,
		Route:    "/refund-transaction/" + applied.TransactionID,
		Router:   router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, preview.DryRun)
	require.Len(t, preview.Balances, 2)

	// The reversal runs the other way: the original destination is debited.
	assert.Equal(t, destination, preview.Balances[0].BalanceID)
	assert.Equal(t, model.PreviewRoleSource, preview.Balances[0].Role)

	// The parent is untouched, so a real refund afterwards still succeeds.
	var refund model.Transaction
	realResp, err := SetUpTestRequest(TestRequest{
		Response: &refund,
		Method:   http.MethodPost,
		Route:    "/refund-transaction/" + applied.TransactionID,
		Router:   router,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, realResp.Code, "a dry run must not mark the parent refunded")
}

// TestRefundWithoutBodyStillWorks guards the long-standing bodiless refund
// call against the new optional field.
func TestRefundWithoutBodyStillWorks(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	applied, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference:   "bodiless_" + model.GenerateUUIDWithSuffix("ref"),
		Source:      source,
		Destination: destination,
		Amount:      100,
		Precision:   100,
		Currency:    "USD",
		SkipQueue:   true,
	})
	require.NoError(t, err)

	var created model.Transaction
	resp, err := SetUpTestRequest(TestRequest{
		Response: &created,
		Method:   http.MethodPost,
		Route:    "/refund-transaction/" + applied.TransactionID,
		Router:   router,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.Code, "an empty body must keep meaning \"refund normally\"")
}
