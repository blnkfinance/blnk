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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/apierror"
)

// TestErrorDetailPayloads exercises the standard dual error payload across
// endpoint groups: each response must carry the legacy flat field plus
// error_detail with the catalog code, at the corrected HTTP status.
func TestErrorDetailPayloads(t *testing.T) {
	router, _, err := setupRouter()
	require.NoError(t, err)

	tests := []struct {
		name   string
		method string
		route  string
		body   string
		status int
		code   apierror.ErrorCode
	}{
		{"transaction not found", "GET", "/transactions/txn_nonexistent", "", http.StatusNotFound, apierror.ErrTxnNotFound},
		{"transaction reference not found", "GET", "/transactions/reference/ref_does_not_exist", "", http.StatusNotFound, apierror.ErrTxnNotFound},
		{"ledger not found", "GET", "/ledgers/ldg_nonexistent", "", http.StatusNotFound, apierror.ErrLgrNotFound},
		{"balance not found", "GET", "/balances/bln_nonexistent", "", http.StatusNotFound, apierror.ErrBalNotFound},
		{"identity not found", "GET", "/identities/idt_nonexistent", "", http.StatusNotFound, apierror.ErrIdtNotFound},
		{"malformed transaction body", "POST", "/transactions", "{bad json", http.StatusBadRequest, apierror.ErrGenMalformedRequest},
		{"bulk void empty list", "POST", "/transactions/inflight/bulk/void", `{"transaction_ids": []}`, http.StatusBadRequest, apierror.ErrTxnBulkEmpty},
		{"invalid inflight action", "PUT", "/transactions/inflight/txn_x", `{"status": "explode"}`, http.StatusBadRequest, apierror.ErrTxnInvalidStatusAction},
		{"invalid balance timestamp", "GET", "/balances/bln_x/at?timestamp=not-a-time", "", http.StatusBadRequest, apierror.ErrBalInvalidTimestamp},
		{"reconciliation not found", "GET", "/reconciliation/recon_nonexistent", "", http.StatusNotFound, apierror.ErrReconNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body *bytes.Reader
			if tt.body != "" {
				body = bytes.NewReader([]byte(tt.body))
			} else {
				body = bytes.NewReader(nil)
			}
			req := httptest.NewRequest(tt.method, tt.route, body)
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			assertErrorCode(t, w, tt.status, tt.code)
		})
	}
}
