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

package apierror

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestStatusForCode(t *testing.T) {
	tests := []struct {
		code   ErrorCode
		status int
	}{
		{ErrGenMalformedRequest, http.StatusBadRequest},
		{ErrGenValidation, http.StatusBadRequest},
		{ErrGenMissingParameter, http.StatusBadRequest},
		{ErrGenBadRequest, http.StatusBadRequest},
		{ErrGenNotFound, http.StatusNotFound},
		{ErrGenConflict, http.StatusConflict},
		{ErrGenResourceLocked, http.StatusLocked},
		{ErrGenRateLimited, http.StatusTooManyRequests},
		{ErrGenInternal, http.StatusInternalServerError},
		{ErrAuthMissingAPIKey, http.StatusUnauthorized},
		{ErrAuthInvalidAPIKey, http.StatusUnauthorized},
		{ErrAuthExpiredAPIKey, http.StatusUnauthorized},
		{ErrAuthMissingPrincipal, http.StatusUnauthorized},
		{ErrAuthInsufficientPermissions, http.StatusForbidden},
		{ErrAuthUnknownResource, http.StatusForbidden},
		{ErrAuthMasterKeyRequired, http.StatusForbidden},
		{ErrAuthCrossOwnerAccess, http.StatusForbidden},
		{ErrAuthScopeEscalation, http.StatusForbidden},
		{ErrAuthMetricsTokenRequired, http.StatusUnauthorized},
		{ErrAuthInvalidBearerToken, http.StatusUnauthorized},
		{ErrAuthMetricsDisabled, http.StatusForbidden},
		{ErrAPIKeyNotFound, http.StatusNotFound},
		{ErrAPIKeyOwnerRequired, http.StatusBadRequest},
		{ErrAPIKeyInvalid, http.StatusBadRequest},
		{ErrTxnNotFound, http.StatusNotFound},
		{ErrTxnInsufficientFunds, http.StatusBadRequest},
		{ErrTxnInvalidAmount, http.StatusBadRequest},
		{ErrTxnPrecisionNotInteger, http.StatusBadRequest},
		{ErrTxnInvalidDistribution, http.StatusBadRequest},
		{ErrTxnDuplicateReference, http.StatusConflict},
		{ErrTxnNotInflight, http.StatusBadRequest},
		{ErrTxnAlreadyCommitted, http.StatusConflict},
		{ErrTxnAlreadyVoided, http.StatusConflict},
		{ErrTxnCommitAmountExceeded, http.StatusBadRequest},
		{ErrTxnInvalidStatusAction, http.StatusBadRequest},
		{ErrTxnBulkEmpty, http.StatusBadRequest},
		{ErrTxnBulkLimitExceeded, http.StatusBadRequest},
		{ErrTxnValidation, http.StatusBadRequest},
		{ErrBalNotFound, http.StatusNotFound},
		{ErrBalHistoryNotFound, http.StatusNotFound},
		{ErrBalInvalidTimestamp, http.StatusBadRequest},
		{ErrBalValidation, http.StatusBadRequest},
		{ErrBalMonitorNotFound, http.StatusNotFound},
		{ErrLgrNotFound, http.StatusNotFound},
		{ErrLgrDuplicate, http.StatusConflict},
		{ErrAccNotFound, http.StatusNotFound},
		{ErrAccDuplicate, http.StatusConflict},
		{ErrAccGenerationFailed, http.StatusInternalServerError},
		{ErrIdtNotFound, http.StatusNotFound},
		{ErrIdtValidation, http.StatusBadRequest},
		{ErrIdtFieldNotTokenizable, http.StatusBadRequest},
		{ErrIdtFieldAlreadyTokenized, http.StatusConflict},
		{ErrIdtFieldNotTokenized, http.StatusBadRequest},
		{ErrIdtFieldNotFound, http.StatusBadRequest},
		{ErrIdtTokenizationDisabled, http.StatusForbidden},
		{ErrReconNotFound, http.StatusNotFound},
		{ErrReconRuleNotFound, http.StatusNotFound},
		{ErrReconUploadFailed, http.StatusBadRequest},
		{ErrReconUploadProcessingFailed, http.StatusInternalServerError},
		{ErrReconRuleInvalid, http.StatusBadRequest},
		{ErrReconMatchingRulesRequired, http.StatusBadRequest},
		{ErrReconExternalTxnsRequired, http.StatusBadRequest},
		{ErrReconStartFailed, http.StatusInternalServerError},
		{ErrMetaEntityNotFound, http.StatusNotFound},
		{ErrMetaUnsupportedEntity, http.StatusBadRequest},
		{ErrMetaInvalidEntityID, http.StatusBadRequest},
		{ErrHookNotFound, http.StatusNotFound},
		{ErrHookInvalid, http.StatusBadRequest},
		{ErrHookOperationFailed, http.StatusInternalServerError},
		{ErrSrchQueryInvalid, http.StatusBadRequest},
		{ErrSrchFailed, http.StatusInternalServerError},
		{ErrSrchReindexInProgress, http.StatusConflict},
		{ErrSrchReindexNotStarted, http.StatusNotFound},
		{ErrAdminBackupFailed, http.StatusInternalServerError},
		// Legacy codes, including the BAD_REQUEST fix (was 500).
		{ErrNotFound, http.StatusNotFound},
		{ErrConflict, http.StatusConflict},
		{ErrBadRequest, http.StatusBadRequest},
		{ErrInvalidInput, http.StatusBadRequest},
		{ErrInternalServer, http.StatusInternalServerError},
		{ErrRateLimited, http.StatusTooManyRequests},
	}
	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			if got := StatusForCode(tt.code); got != tt.status {
				t.Errorf("StatusForCode(%s) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}

func TestStatusForCodeUnknown(t *testing.T) {
	if got := StatusForCode("NO_SUCH_CODE"); got != http.StatusInternalServerError {
		t.Errorf("StatusForCode(unknown) = %d, want 500", got)
	}
}

func TestNormalize(t *testing.T) {
	legacy := map[ErrorCode]ErrorCode{
		ErrNotFound:       ErrGenNotFound,
		ErrConflict:       ErrGenConflict,
		ErrBadRequest:     ErrGenBadRequest,
		ErrInvalidInput:   ErrGenValidation,
		ErrInternalServer: ErrGenInternal,
		ErrRateLimited:    ErrGenRateLimited,
	}
	for old, canonical := range legacy {
		if got := Normalize(old); got != canonical {
			t.Errorf("Normalize(%s) = %s, want %s", old, got, canonical)
		}
	}
	// Canonical codes pass through.
	if got := Normalize(ErrTxnNotFound); got != ErrTxnNotFound {
		t.Errorf("Normalize(%s) = %s, want unchanged", ErrTxnNotFound, got)
	}
}

func TestAllCodesHaveStatus(t *testing.T) {
	// Every catalog code must resolve via the status map, and normalization
	// must never produce a code without a status.
	for code := range statusByCode {
		normalized := Normalize(code)
		if _, ok := statusByCode[normalized]; !ok {
			t.Errorf("Normalize(%s) = %s has no status mapping", code, normalized)
		}
	}
}

func TestCodeNamingConvention(t *testing.T) {
	legacySet := map[ErrorCode]bool{
		ErrNotFound: true, ErrConflict: true, ErrBadRequest: true,
		ErrInvalidInput: true, ErrInternalServer: true, ErrRateLimited: true,
	}
	prefixes := []string{"GEN_", "AUTH_", "APIKEY_", "TXN_", "BAL_", "LGR_", "ACC_", "IDT_", "RECON_", "META_", "HOOK_", "SRCH_", "ADMIN_"}
	for code := range statusByCode {
		if legacySet[code] {
			continue
		}
		ok := false
		for _, p := range prefixes {
			if strings.HasPrefix(string(code), p) {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("code %s does not use a known domain prefix", code)
		}
	}
}

func TestMapErrorToHTTPStatusUnwrapping(t *testing.T) {
	base := NewAPIError(ErrTxnDuplicateReference, "reference used", nil)
	wrapped := fmt.Errorf("queueing failed: %w", base)
	if got := MapErrorToHTTPStatus(wrapped); got != http.StatusConflict {
		t.Errorf("MapErrorToHTTPStatus(wrapped) = %d, want 409", got)
	}
	if got := MapErrorToHTTPStatus(fmt.Errorf("plain error")); got != http.StatusInternalServerError {
		t.Errorf("MapErrorToHTTPStatus(plain) = %d, want 500", got)
	}
	// Legacy BAD_REQUEST now maps to 400 (previously fell through to 500).
	if got := MapErrorToHTTPStatus(NewAPIError(ErrBadRequest, "bad", nil)); got != http.StatusBadRequest {
		t.Errorf("MapErrorToHTTPStatus(BAD_REQUEST) = %d, want 400", got)
	}
}

func TestNewErrorResponseNormalizes(t *testing.T) {
	resp := NewErrorResponse(ErrNotFound, "missing", nil)
	if resp.Error.Code != ErrGenNotFound {
		t.Errorf("NewErrorResponse code = %s, want %s", resp.Error.Code, ErrGenNotFound)
	}
	if resp.Error.Message != "missing" {
		t.Errorf("NewErrorResponse message = %q", resp.Error.Message)
	}
}
