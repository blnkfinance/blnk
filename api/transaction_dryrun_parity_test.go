package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// errorCodeOf pulls the catalog code out of an error response.
func errorCodeOf(t *testing.T, body []byte) string {
	t.Helper()
	var parsed struct {
		ErrorDetail struct {
			Code string `json:"code"`
		} `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed), "response was not an error envelope: %s", string(body))
	return parsed.ErrorDetail.Code
}

// A preview is only useful if it reports what the real post would report. This
// asserts the two agree on a missing balance rather than asserting a specific
// code, deliberately: the code a missing balance produces is owned by the error
// catalog and has changed before -- #362 moves it from the generic not-found to
// a balance-specific one. Pinning the literal here would either fight that
// change or silently bless a divergence.
//
// The invariant that matters is agreement, and it holds either way.
func TestDryRunAndRealAgreeOnMissingBalance(t *testing.T) {
	router, _, err := setupRouter()
	require.NoError(t, err)

	body := func() map[string]interface{} {
		return map[string]interface{}{
			"amount": 100, "precision": 100, "currency": "USD",
			"reference":   model.GenerateUUIDWithSuffix("parity"),
			"description": "parity check",
			"source":      "bln_definitely_absent_src",
			"destination": "bln_definitely_absent_dst",
		}
	}

	previewBody := body()
	previewBody["dry_run"] = true
	preview := doJSON(router, http.MethodPost, "/transactions", previewBody)

	realBody := body()
	realBody["skip_queue"] = true
	real := doJSON(router, http.MethodPost, "/transactions", realBody)

	assert.Equal(t, real.Code, preview.Code,
		"preview and real disagree on HTTP status for a missing balance:\n preview: %s\n real:    %s",
		preview.Body.String(), real.Body.String())

	assert.Equal(t, errorCodeOf(t, real.Body.Bytes()), errorCodeOf(t, preview.Body.Bytes()),
		"preview and real disagree on the error code for a missing balance")
}

// The same agreement must hold for a request the validator rejects, which is
// resolved before either branch is chosen.
func TestDryRunAndRealAgreeOnValidationFailure(t *testing.T) {
	router, _, err := setupRouter()
	require.NoError(t, err)

	body := func() map[string]interface{} {
		return map[string]interface{}{
			"amount": 100, "precision": 100, "currency": "USD",
			"reference":   model.GenerateUUIDWithSuffix("parity"),
			"description": "parity check",
			"source":      "bln_a",
			// destination omitted entirely: invalid on both paths
		}
	}

	previewBody := body()
	previewBody["dry_run"] = true
	preview := doJSON(router, http.MethodPost, "/transactions", previewBody)

	realBody := body()
	realBody["skip_queue"] = true
	real := doJSON(router, http.MethodPost, "/transactions", realBody)

	require.Equal(t, http.StatusBadRequest, preview.Code, preview.Body.String())
	assert.Equal(t, real.Code, preview.Code)
	assert.Equal(t, real.Body.String(), preview.Body.String(),
		"validation runs ahead of the dry_run branch, so both paths must return the identical body")
}
