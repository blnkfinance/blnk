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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/apierror"
)

// TestBulkMissingBalanceReportsBalanceNotFound pins the bulk route to the same
// code the single-transaction route reports for the same condition.
//
// A batch failure is reported to the client as one assembled sentence: the
// failing item's error, plus what happened to the rest of the batch. Choosing
// the response code by matching that sentence made a missing balance answer
// TXN_NOT_FOUND, because the sentence mentions a transaction and the broad
// "transaction ... not found" pattern is the first one to match. The code is
// resolved from the error chain instead, so the item's own BAL_NOT_FOUND
// survives being wrapped.
func TestBulkMissingBalanceReportsBalanceNotFound(t *testing.T) {
	router, _, err := setupRouter()
	require.NoError(t, err)

	// skip_queue forces the batch to apply inline, so the balance lookup —
	// and its failure — happens while the request is still being served.
	// On the queued path the item is handed to a background worker and the
	// request is acknowledged before any balance is read.
	body := `{
		"skip_queue": true,
		"transactions": [{
			"amount": 100,
			"precision": 100,
			"currency": "USD",
			"reference": "ref_bulk_missing_balance_1",
			"description": "missing source balance",
			"source": "bln_does_not_exist_src",
			"destination": "bln_does_not_exist_dst"
		}]
	}`

	req := httptest.NewRequest("POST", "/transactions/bulk", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assertErrorCode(t, w, http.StatusNotFound, apierror.ErrBalNotFound)

	// batch_id stays a top-level sibling of the error payload.
	var payload struct {
		BatchID string `json:"batch_id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
	require.NotEmpty(t, payload.BatchID)
}

// TestResolveErrorCodeUnwrapsTypedError covers the seam directly: the same
// text resolves to different codes depending on whether the error chain is
// consulted or only the flattened message.
func TestResolveErrorCodeUnwrapsTypedError(t *testing.T) {
	balErr := apierror.NewAPIError(apierror.ErrBalNotFound, "Balance with ID 'bln_x' not found", nil)
	wrapped := fmt.Errorf("failed to queue transactions[0] (Reference: ref_1, Source: bln_x, Destination: bln_y, Amount: 1.00): %w. Previous transactions were not rolled back.", balErr)

	code, ok := resolveErrorCode(wrapped)
	require.True(t, ok)
	require.Equal(t, apierror.ErrBalNotFound, code)

	// The message alone cannot tell the two apart; this is the misreport the
	// resolution order above avoids.
	byMessage, ok := classifyMessage(wrapped.Error())
	require.True(t, ok)
	require.Equal(t, apierror.ErrTxnNotFound, byMessage)
}

// TestHookFailureCodeUsesTheErrorChain covers the other place a code was being
// chosen from message text alone. A hook manager that reports a missing hook
// as a typed APIError, in wording no pattern matches, is still reporting a
// missing hook — and used to be answered as an infrastructure failure.
func TestHookFailureCodeUsesTheErrorChain(t *testing.T) {
	typed := apierror.NewAPIError(apierror.ErrHookNotFound, "no such hook", nil)
	require.Equal(t, apierror.ErrHookNotFound, hookFailureCode(typed))

	// Wrapping does not lose it.
	require.Equal(t, apierror.ErrHookNotFound,
		hookFailureCode(fmt.Errorf("delete hook: %w", typed)))

	// The message-matched path still works.
	require.Equal(t, apierror.ErrHookNotFound,
		hookFailureCode(fmt.Errorf("hook not found: hook_1")))

	// Anything that is not a not-found stays an operation failure.
	require.Equal(t, apierror.ErrHookOperationFailed,
		hookFailureCode(fmt.Errorf("redis: connection reset by peer")))
}

// TestSanitizeBindErrorNamesFieldClassOnly checks the rewritten decode error:
// it must name what is wrong and which field class raised it, without
// exposing a Go type or restating the JSON shape that the docs cover.
func TestSanitizeBindErrorNamesFieldClassOnly(t *testing.T) {
	msg := sanitizeBindError(fmt.Errorf(`math/big: cannot unmarshal "\"5000\"" into a *big.Int`))

	require.Equal(t, "invalid numeric value: precise amount must be a JSON number", msg)
	require.NotContains(t, msg, "big.Int")
	require.NotContains(t, msg, "5000")

	// Unrecognised decode errors are passed through untouched.
	other := fmt.Errorf("invalid character 'b' looking for beginning of object key string")
	require.Equal(t, other.Error(), sanitizeBindError(other))
}
