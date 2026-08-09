package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

func bulkItem(source, destination string, amount float64, reference string) *model2.RecordTransaction {
	return &model2.RecordTransaction{
		Amount:      amount,
		Precision:   100,
		Currency:    "USD",
		Source:      source,
		Destination: destination,
		Reference:   reference,
		Description: "bulk dry run",
	}
}

// TestDryRunBulkCumulativeCatchesIntraBatchShortfall covers the case the mode
// exists for: with skip_queue the items really do run one after another, so the
// second item must be judged against what the first one did.
func TestDryRunBulkCumulativeCatchesIntraBatchShortfall(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	req := model2.BulkTransactionRequest{
		DryRun:    true,
		SkipQueue: true,
		Transactions: []*model2.RecordTransaction{
			// Drains almost all of the 500.00 the fixture funded.
			bulkItem(source, destination, 450, "bulk_a_"+model.GenerateUUIDWithSuffix("ref")),
			// Only affordable if the first item is ignored.
			bulkItem(source, destination, 100, "bulk_b_"+model.GenerateUUIDWithSuffix("ref")),
		},
	}

	body, err := request.ToJsonReq(&req)
	require.NoError(t, err)

	var preview model.BulkTransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: body, Response: &preview, Method: http.MethodPost,
		Route: "/transactions/bulk", Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, preview.Cumulative, "skip_queue batches run in order, so the projection must accumulate")
	assert.False(t, preview.WouldApply, "the batch overspends itself and must not be projected as applying")

	require.Len(t, preview.Results, 2)
	assert.True(t, preview.Results[0].WouldApply)
	assert.False(t, preview.Results[1].WouldApply, "the second item must be judged against the first item's effect")
	require.NotNil(t, preview.Results[1].Rejection)
	assert.Equal(t, "TXN_INSUFFICIENT_FUNDS", preview.Results[1].Rejection.Code)
}

// TestDryRunBulkIndependentWhenQueued covers the other half: without skip_queue
// the items are dispatched concurrently, so claiming an order would be wrong.
func TestDryRunBulkIndependentWhenQueued(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	req := model2.BulkTransactionRequest{
		DryRun:    true,
		SkipQueue: false,
		Transactions: []*model2.RecordTransaction{
			bulkItem(source, destination, 450, "bulkq_a_"+model.GenerateUUIDWithSuffix("ref")),
			bulkItem(source, destination, 100, "bulkq_b_"+model.GenerateUUIDWithSuffix("ref")),
		},
	}

	body, err := request.ToJsonReq(&req)
	require.NoError(t, err)

	var preview model.BulkTransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: body, Response: &preview, Method: http.MethodPost,
		Route: "/transactions/bulk", Router: router,
	})
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.False(t, preview.Cumulative)
	assert.True(t, preview.WouldApply, "projected independently, each item fits on its own")
	require.Len(t, preview.Results, 2)
	assert.True(t, preview.Results[0].WouldApply)
	assert.True(t, preview.Results[1].WouldApply)
	assert.NotEmpty(t, preview.Notes, "the caller must be told the items were not ordered")
}

// TestDryRunBulkWritesNothing checks the batch projection leaves no trace.
func TestDryRunBulkWritesNothing(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)
	reference := "bulknone_" + model.GenerateUUIDWithSuffix("ref")

	req := model2.BulkTransactionRequest{
		DryRun:       true,
		SkipQueue:    true,
		Transactions: []*model2.RecordTransaction{bulkItem(source, destination, 100, reference)},
	}

	body, err := request.ToJsonReq(&req)
	require.NoError(t, err)

	var preview model.BulkTransactionPreview
	resp, err := SetUpTestRequest(TestRequest{
		Payload: body, Response: &preview, Method: http.MethodPost,
		Route: "/transactions/bulk", Router: router,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code)

	_, err = b.GetTransactionByRef(t.Context(), reference)
	assert.Error(t, err, "a bulk dry run must not persist its items")

	after, err := b.GetBalanceByID(t.Context(), source, nil, false)
	require.NoError(t, err)
	assert.Equal(t, "50000", after.Balance.String(), "a bulk dry run must not move balances")
}

// TestDryRunBulkAtomicNotesCompensation checks the response says what atomic
// really does, rather than implying a rollback the ledger does not perform.
func TestDryRunBulkAtomicNotesCompensation(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	source, destination := newDryRunFixture(t, b)

	req := model2.BulkTransactionRequest{
		DryRun:       true,
		SkipQueue:    true,
		Atomic:       true,
		Transactions: []*model2.RecordTransaction{bulkItem(source, destination, 100, "bulkatomic_"+model.GenerateUUIDWithSuffix("ref"))},
	}

	body, err := request.ToJsonReq(&req)
	require.NoError(t, err)

	var preview model.BulkTransactionPreview
	_, err = SetUpTestRequest(TestRequest{
		Payload: body, Response: &preview, Method: http.MethodPost,
		Route: "/transactions/bulk", Router: router,
	})
	require.NoError(t, err)

	assert.True(t, preview.Atomic)
	found := false
	for _, note := range preview.Notes {
		if strings.Contains(note, "compensates") {
			found = true
		}
	}
	assert.True(t, found, "an atomic batch must be described as compensating, not rolling back")
}
