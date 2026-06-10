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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/tokenization"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	return c, w
}

type errorDetailBody struct {
	Detail apierror.APIError `json:"error_detail"`
}

func decodeDetail(t *testing.T, w *httptest.ResponseRecorder) (map[string]interface{}, apierror.APIError) {
	t.Helper()
	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	var body errorDetailBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return raw, body.Detail
}

func TestRespondCodeDualPayload(t *testing.T) {
	c, w := newTestContext()
	respondCode(c, apierror.ErrTxnNotFound, "transaction not found", nil)

	assert.Equal(t, http.StatusNotFound, w.Code)
	raw, detail := decodeDetail(t, w)
	assert.Equal(t, "transaction not found", raw["error"])
	assert.Equal(t, apierror.ErrTxnNotFound, detail.Code)
	assert.Equal(t, "transaction not found", detail.Message)
}

func TestRespondCodeLegacyKeyAndValue(t *testing.T) {
	c, w := newTestContext()
	parseErrors := []string{"bad filter a", "bad filter b"}
	respondCode(c, apierror.ErrGenValidation, "invalid filters", nil,
		withLegacyKey("errors"), withLegacyValue(parseErrors))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	raw, detail := decodeDetail(t, w)
	assert.Nil(t, raw["error"])
	assert.Len(t, raw["errors"], 2)
	assert.Equal(t, apierror.ErrGenValidation, detail.Code)
}

func TestRespondErrorTypedAPIError(t *testing.T) {
	c, w := newTestContext()
	err := apierror.NewAPIError(apierror.ErrNotFound, "Ledger with ID 'x' not found", nil)
	respondError(c, fmt.Errorf("wrapped: %w", err))

	assert.Equal(t, http.StatusNotFound, w.Code)
	raw, detail := decodeDetail(t, w)
	assert.Equal(t, "Ledger with ID 'x' not found", raw["error"])
	assert.Equal(t, apierror.ErrGenNotFound, detail.Code) // legacy NOT_FOUND normalized
}

func TestRespondErrorUpgrade(t *testing.T) {
	c, w := newTestContext()
	err := apierror.NewAPIError(apierror.ErrNotFound, "Ledger not found", nil)
	respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrLgrNotFound))

	assert.Equal(t, http.StatusNotFound, w.Code)
	_, detail := decodeDetail(t, w)
	assert.Equal(t, apierror.ErrLgrNotFound, detail.Code)
}

func TestRespondErrorSentinels(t *testing.T) {
	tests := []struct {
		err    error
		code   apierror.ErrorCode
		status int
	}{
		{database.ErrAPIKeyNotFound, apierror.ErrAPIKeyNotFound, http.StatusNotFound},
		{database.ErrInvalidAPIKey, apierror.ErrAuthInvalidAPIKey, http.StatusUnauthorized},
		{tokenization.ErrTokenizationDisabled, apierror.ErrIdtTokenizationDisabled, http.StatusForbidden},
		{redlock.ErrLockHeld, apierror.ErrGenResourceLocked, http.StatusLocked},
		{redlock.ErrLockWaitTimeout, apierror.ErrGenResourceLocked, http.StatusLocked},
		{model2.ErrPrecisionMustBeInteger, apierror.ErrTxnPrecisionNotInteger, http.StatusBadRequest},
		{sql.ErrNoRows, apierror.ErrGenNotFound, http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			c, w := newTestContext()
			respondError(c, fmt.Errorf("context: %w", tt.err))
			assert.Equal(t, tt.status, w.Code)
			_, detail := decodeDetail(t, w)
			assert.Equal(t, tt.code, detail.Code)
		})
	}
}

func TestRespondErrorMessagePatterns(t *testing.T) {
	tests := []struct {
		msg    string
		code   apierror.ErrorCode
		status int
	}{
		{"transaction not found", apierror.ErrTxnNotFound, http.StatusNotFound},
		{"transaction txn_123 not found in DB or queue: timeout", apierror.ErrTxnNotFound, http.StatusNotFound},
		{"transaction is not in inflight status", apierror.ErrTxnNotInflight, http.StatusBadRequest},
		{"cannot commit. Transaction already committed", apierror.ErrTxnAlreadyCommitted, http.StatusConflict},
		{"transaction has already been voided", apierror.ErrTxnAlreadyVoided, http.StatusConflict},
		{"transaction txn_1 has already been refunded", apierror.ErrGenConflict, http.StatusConflict},
		{"reference ref_991 has already been used", apierror.ErrTxnDuplicateReference, http.StatusConflict},
		{"cannot commit more than the remaining amount. Available: USD 5.00, Requested: USD 9.00", apierror.ErrTxnCommitAmountExceeded, http.StatusBadRequest},
		{"insufficient funds in source balance", apierror.ErrTxnInsufficientFunds, http.StatusBadRequest},
		{"transaction validation failed", apierror.ErrTxnValidation, http.StatusBadRequest},
		{"no balance data found for time: 2024-01-01", apierror.ErrBalHistoryNotFound, http.StatusNotFound},
		{"balance validation failed: bad currency", apierror.ErrBalValidation, http.StatusBadRequest},
		{"field email_address is not tokenizable", apierror.ErrIdtFieldNotTokenizable, http.StatusBadRequest},
		{"field email_address is already tokenized", apierror.ErrIdtFieldAlreadyTokenized, http.StatusConflict},
		{"field email_address is not tokenized. Metadata: {}", apierror.ErrIdtFieldNotTokenized, http.StatusBadRequest},
		{"field nickname not found", apierror.ErrIdtFieldNotFound, http.StatusBadRequest},
		{"rule name is required", apierror.ErrReconRuleInvalid, http.StatusBadRequest},
		{"at least one matching criteria is required", apierror.ErrReconRuleInvalid, http.StatusBadRequest},
		{"invalid operator", apierror.ErrReconRuleInvalid, http.StatusBadRequest},
		{"drift for amount must be between 0 and 100 (percentage)", apierror.ErrReconRuleInvalid, http.StatusBadRequest},
		{"entity not found", apierror.ErrMetaEntityNotFound, http.StatusNotFound},
		{"unsupported entity type: zone", apierror.ErrMetaUnsupportedEntity, http.StatusBadRequest},
		{"invalid entity ID format: zzz", apierror.ErrMetaInvalidEntityID, http.StatusBadRequest},
		{"failed to acquire lock: contention", apierror.ErrGenResourceLocked, http.StatusLocked},
		{"sql: no rows in result set", apierror.ErrGenNotFound, http.StatusNotFound},
		{"balance monitor not found", apierror.ErrGenNotFound, http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			c, w := newTestContext()
			respondError(c, errors.New(tt.msg))
			assert.Equal(t, tt.status, w.Code)
			raw, detail := decodeDetail(t, w)
			assert.Equal(t, tt.code, detail.Code)
			assert.Equal(t, tt.msg, raw["error"], "legacy message must be preserved verbatim")
		})
	}
}

func TestRespondErrorFallbackSanitizes(t *testing.T) {
	c, w := newTestContext()
	respondError(c, errors.New("pq: connection refused on 10.0.0.5"))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	raw, detail := decodeDetail(t, w)
	assert.Equal(t, apierror.ErrGenInternal, detail.Code)
	assert.Equal(t, sanitizedInternalMessage, raw["error"])
	assert.Equal(t, sanitizedInternalMessage, detail.Message)
}

func TestRespondErrorWithDefault(t *testing.T) {
	c, w := newTestContext()
	respondError(c, errors.New("some opaque reconciliation failure"), withDefault(apierror.ErrReconStartFailed))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	raw, detail := decodeDetail(t, w)
	assert.Equal(t, apierror.ErrReconStartFailed, detail.Code)
	// 4xx/explicit-default paths keep the original message.
	assert.Equal(t, "some opaque reconciliation failure", raw["error"])
}

func TestRespondBareAPIErrorShape(t *testing.T) {
	c, w := newTestContext()
	legacy := apierror.APIError{Code: apierror.ErrInvalidInput, Message: "invalid hook data", Details: nil}
	respondBareAPIError(c, legacy, apierror.ErrHookInvalid)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	raw, detail := decodeDetail(t, w)
	assert.Equal(t, string(apierror.ErrInvalidInput), raw["code"], "historical top-level code preserved")
	assert.Equal(t, "invalid hook data", raw["message"])
	assert.Equal(t, apierror.ErrHookInvalid, detail.Code)
}

func TestRespondNestedAPIErrorShape(t *testing.T) {
	c, w := newTestContext()
	legacy := apierror.APIError{Code: apierror.ErrInternalServer, Message: "Failed to backup database", Details: nil}
	respondNestedAPIError(c, legacy, apierror.ErrAdminBackupFailed)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	raw, detail := decodeDetail(t, w)
	legacyErr, ok := raw["error"].(map[string]interface{})
	require.True(t, ok, "legacy error must remain an object on admin endpoints")
	assert.Equal(t, string(apierror.ErrInternalServer), legacyErr["code"])
	assert.Equal(t, apierror.ErrAdminBackupFailed, detail.Code)
}
