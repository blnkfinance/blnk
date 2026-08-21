package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// A rejected projection must not also report a status.
//
// Status is set early, from the status the transaction would carry once
// applied, and the projection only later discovers the balance cannot cover
// it. Left alone the response asserted two things that cannot both be true:
// would_apply false, and status APPLIED.
func TestDryRunRejectedPreviewReportsNoStatus(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	source, destination := newDryRunFixture(t, b) // source holds 500.00

	w := doJSON(router, http.MethodPost, "/transactions", map[string]interface{}{
		"amount": 999999, "precision": 100, "currency": "USD",
		"reference":   model.GenerateUUIDWithSuffix("rejstatus"),
		"description": "more than the source holds",
		"source":      source,
		"destination": destination,
		"dry_run":     true,
	})

	require.Equal(t, http.StatusOK, w.Code, "a projected rejection is a successful answer of \"no\"")

	var preview model.TransactionPreview
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &preview))

	require.False(t, preview.WouldApply)
	require.NotNil(t, preview.Rejection)
	assert.Equal(t, "TXN_INSUFFICIENT_FUNDS", preview.Rejection.Code)

	assert.Empty(t, preview.Status,
		"a rejected projection has no resulting status; the real post writes no transaction")
	assert.NotContains(t, w.Body.String(), `"status"`,
		"status must be omitted from a rejected projection, not emitted empty:\n%s", w.Body.String())
}

// The accepted case must be unaffected: status is exactly what the real post
// would carry.
func TestDryRunAcceptedPreviewStillReportsStatus(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	source, destination := newDryRunFixture(t, b)

	w := doJSON(router, http.MethodPost, "/transactions", map[string]interface{}{
		"amount": 1, "precision": 100, "currency": "USD",
		"reference":   model.GenerateUUIDWithSuffix("okstatus"),
		"description": "within balance",
		"source":      source,
		"destination": destination,
		"dry_run":     true,
	})

	require.Equal(t, http.StatusOK, w.Code)

	var preview model.TransactionPreview
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &preview))
	require.True(t, preview.WouldApply, w.Body.String())
	assert.Equal(t, "APPLIED", preview.Status, "an accepted projection still reports what it would become")
}
