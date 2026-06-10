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

import "net/http"

// Domain-prefixed error codes. These are the canonical, client-facing codes
// returned in the `error_detail.code` field of every error response.
// Each code maps to exactly one default HTTP status (see statusByCode).
//
// The six legacy codes in apierror.go (NOT_FOUND, CONFLICT, ...) remain valid
// for internal construction — they are normalized to their GEN_* equivalents
// at the response boundary via Normalize.
const (
	// GEN — generic / cross-cutting
	ErrGenMalformedRequest ErrorCode = "GEN_MALFORMED_REQUEST"
	ErrGenValidation       ErrorCode = "GEN_VALIDATION_ERROR"
	ErrGenMissingParameter ErrorCode = "GEN_MISSING_PARAMETER"
	ErrGenBadRequest       ErrorCode = "GEN_BAD_REQUEST"
	ErrGenNotFound         ErrorCode = "GEN_NOT_FOUND"
	ErrGenConflict         ErrorCode = "GEN_CONFLICT"
	ErrGenResourceLocked   ErrorCode = "GEN_RESOURCE_LOCKED"
	ErrGenRateLimited      ErrorCode = "GEN_RATE_LIMITED"
	ErrGenInternal         ErrorCode = "GEN_INTERNAL"

	// AUTH — authentication / authorization
	ErrAuthMissingAPIKey           ErrorCode = "AUTH_MISSING_API_KEY"
	ErrAuthInvalidAPIKey           ErrorCode = "AUTH_INVALID_API_KEY"
	ErrAuthExpiredAPIKey           ErrorCode = "AUTH_EXPIRED_API_KEY"
	ErrAuthMissingPrincipal        ErrorCode = "AUTH_MISSING_PRINCIPAL"
	ErrAuthInsufficientPermissions ErrorCode = "AUTH_INSUFFICIENT_PERMISSIONS"
	ErrAuthUnknownResource         ErrorCode = "AUTH_UNKNOWN_RESOURCE"
	ErrAuthMasterKeyRequired       ErrorCode = "AUTH_MASTER_KEY_REQUIRED"
	ErrAuthCrossOwnerAccess        ErrorCode = "AUTH_CROSS_OWNER_ACCESS"
	ErrAuthScopeEscalation         ErrorCode = "AUTH_SCOPE_ESCALATION"
	ErrAuthMetricsTokenRequired    ErrorCode = "AUTH_METRICS_TOKEN_REQUIRED"
	ErrAuthInvalidBearerToken      ErrorCode = "AUTH_INVALID_BEARER_TOKEN"
	ErrAuthMetricsDisabled         ErrorCode = "AUTH_METRICS_DISABLED"

	// APIKEY — API-key resource management
	ErrAPIKeyNotFound      ErrorCode = "APIKEY_NOT_FOUND"
	ErrAPIKeyOwnerRequired ErrorCode = "APIKEY_OWNER_REQUIRED"
	ErrAPIKeyInvalid       ErrorCode = "APIKEY_INVALID"

	// TXN — transactions
	ErrTxnNotFound             ErrorCode = "TXN_NOT_FOUND"
	ErrTxnInsufficientFunds    ErrorCode = "TXN_INSUFFICIENT_FUNDS"
	ErrTxnInvalidAmount        ErrorCode = "TXN_INVALID_AMOUNT"
	ErrTxnPrecisionNotInteger  ErrorCode = "TXN_PRECISION_NOT_INTEGER"
	ErrTxnInvalidDistribution  ErrorCode = "TXN_INVALID_DISTRIBUTION"
	ErrTxnDuplicateReference   ErrorCode = "TXN_DUPLICATE_REFERENCE"
	ErrTxnNotInflight          ErrorCode = "TXN_NOT_INFLIGHT"
	ErrTxnAlreadyCommitted     ErrorCode = "TXN_ALREADY_COMMITTED"
	ErrTxnAlreadyVoided        ErrorCode = "TXN_ALREADY_VOIDED"
	ErrTxnCommitAmountExceeded ErrorCode = "TXN_COMMIT_AMOUNT_EXCEEDED"
	ErrTxnInvalidStatusAction  ErrorCode = "TXN_INVALID_STATUS_ACTION"
	ErrTxnBulkEmpty            ErrorCode = "TXN_BULK_EMPTY"
	ErrTxnBulkLimitExceeded    ErrorCode = "TXN_BULK_LIMIT_EXCEEDED"
	ErrTxnValidation           ErrorCode = "TXN_VALIDATION_ERROR"

	// BAL — balances & monitors
	ErrBalNotFound         ErrorCode = "BAL_NOT_FOUND"
	ErrBalHistoryNotFound  ErrorCode = "BAL_HISTORY_NOT_FOUND"
	ErrBalInvalidTimestamp ErrorCode = "BAL_INVALID_TIMESTAMP"
	ErrBalValidation       ErrorCode = "BAL_VALIDATION_ERROR"
	ErrBalMonitorNotFound  ErrorCode = "BAL_MONITOR_NOT_FOUND"

	// LGR — ledgers
	ErrLgrNotFound  ErrorCode = "LGR_NOT_FOUND"
	ErrLgrDuplicate ErrorCode = "LGR_DUPLICATE"

	// ACC — accounts
	ErrAccNotFound         ErrorCode = "ACC_NOT_FOUND"
	ErrAccDuplicate        ErrorCode = "ACC_DUPLICATE"
	ErrAccGenerationFailed ErrorCode = "ACC_GENERATION_FAILED"

	// IDT — identities & tokenization
	ErrIdtNotFound              ErrorCode = "IDT_NOT_FOUND"
	ErrIdtValidation            ErrorCode = "IDT_VALIDATION_ERROR"
	ErrIdtFieldNotTokenizable   ErrorCode = "IDT_FIELD_NOT_TOKENIZABLE"
	ErrIdtFieldAlreadyTokenized ErrorCode = "IDT_FIELD_ALREADY_TOKENIZED"
	ErrIdtFieldNotTokenized     ErrorCode = "IDT_FIELD_NOT_TOKENIZED"
	ErrIdtFieldNotFound         ErrorCode = "IDT_FIELD_NOT_FOUND"
	ErrIdtTokenizationDisabled  ErrorCode = "IDT_TOKENIZATION_DISABLED"

	// RECON — reconciliation
	ErrReconNotFound               ErrorCode = "RECON_NOT_FOUND"
	ErrReconRuleNotFound           ErrorCode = "RECON_RULE_NOT_FOUND"
	ErrReconUploadFailed           ErrorCode = "RECON_UPLOAD_FAILED"
	ErrReconUploadProcessingFailed ErrorCode = "RECON_UPLOAD_PROCESSING_FAILED"
	ErrReconRuleInvalid            ErrorCode = "RECON_RULE_INVALID"
	ErrReconMatchingRulesRequired  ErrorCode = "RECON_MATCHING_RULES_REQUIRED"
	ErrReconExternalTxnsRequired   ErrorCode = "RECON_EXTERNAL_TXNS_REQUIRED"
	ErrReconStartFailed            ErrorCode = "RECON_START_FAILED"

	// META — entity metadata
	ErrMetaEntityNotFound    ErrorCode = "META_ENTITY_NOT_FOUND"
	ErrMetaUnsupportedEntity ErrorCode = "META_UNSUPPORTED_ENTITY"
	ErrMetaInvalidEntityID   ErrorCode = "META_INVALID_ENTITY_ID"

	// HOOK — webhook management
	ErrHookNotFound        ErrorCode = "HOOK_NOT_FOUND"
	ErrHookInvalid         ErrorCode = "HOOK_INVALID"
	ErrHookOperationFailed ErrorCode = "HOOK_OPERATION_FAILED"

	// SRCH — search & reindex
	ErrSrchQueryInvalid       ErrorCode = "SRCH_QUERY_INVALID"
	ErrSrchFailed             ErrorCode = "SRCH_FAILED"
	ErrSrchReindexInProgress  ErrorCode = "SRCH_REINDEX_IN_PROGRESS"
	ErrSrchReindexNotStarted  ErrorCode = "SRCH_REINDEX_NOT_STARTED"

	// ADMIN — administrative operations
	ErrAdminBackupFailed ErrorCode = "ADMIN_BACKUP_FAILED"
)

// statusByCode is the single source of truth for the default HTTP status of
// every error code, including the six legacy codes.
var statusByCode = map[ErrorCode]int{
	ErrGenMalformedRequest: http.StatusBadRequest,
	ErrGenValidation:       http.StatusBadRequest,
	ErrGenMissingParameter: http.StatusBadRequest,
	ErrGenBadRequest:       http.StatusBadRequest,
	ErrGenNotFound:         http.StatusNotFound,
	ErrGenConflict:         http.StatusConflict,
	ErrGenResourceLocked:   http.StatusLocked,
	ErrGenRateLimited:      http.StatusTooManyRequests,
	ErrGenInternal:         http.StatusInternalServerError,

	ErrAuthMissingAPIKey:           http.StatusUnauthorized,
	ErrAuthInvalidAPIKey:           http.StatusUnauthorized,
	ErrAuthExpiredAPIKey:           http.StatusUnauthorized,
	ErrAuthMissingPrincipal:        http.StatusUnauthorized,
	ErrAuthInsufficientPermissions: http.StatusForbidden,
	ErrAuthUnknownResource:         http.StatusForbidden,
	ErrAuthMasterKeyRequired:       http.StatusForbidden,
	ErrAuthCrossOwnerAccess:        http.StatusForbidden,
	ErrAuthScopeEscalation:         http.StatusForbidden,
	ErrAuthMetricsTokenRequired:    http.StatusUnauthorized,
	ErrAuthInvalidBearerToken:      http.StatusUnauthorized,
	ErrAuthMetricsDisabled:         http.StatusForbidden,

	ErrAPIKeyNotFound:      http.StatusNotFound,
	ErrAPIKeyOwnerRequired: http.StatusBadRequest,
	ErrAPIKeyInvalid:       http.StatusBadRequest,

	ErrTxnNotFound:             http.StatusNotFound,
	ErrTxnInsufficientFunds:    http.StatusBadRequest,
	ErrTxnInvalidAmount:        http.StatusBadRequest,
	ErrTxnPrecisionNotInteger:  http.StatusBadRequest,
	ErrTxnInvalidDistribution:  http.StatusBadRequest,
	ErrTxnDuplicateReference:   http.StatusConflict,
	ErrTxnNotInflight:          http.StatusBadRequest,
	ErrTxnAlreadyCommitted:     http.StatusConflict,
	ErrTxnAlreadyVoided:        http.StatusConflict,
	ErrTxnCommitAmountExceeded: http.StatusBadRequest,
	ErrTxnInvalidStatusAction:  http.StatusBadRequest,
	ErrTxnBulkEmpty:            http.StatusBadRequest,
	ErrTxnBulkLimitExceeded:    http.StatusBadRequest,
	ErrTxnValidation:           http.StatusBadRequest,

	ErrBalNotFound:         http.StatusNotFound,
	ErrBalHistoryNotFound:  http.StatusNotFound,
	ErrBalInvalidTimestamp: http.StatusBadRequest,
	ErrBalValidation:       http.StatusBadRequest,
	ErrBalMonitorNotFound:  http.StatusNotFound,

	ErrLgrNotFound:  http.StatusNotFound,
	ErrLgrDuplicate: http.StatusConflict,

	ErrAccNotFound:         http.StatusNotFound,
	ErrAccDuplicate:        http.StatusConflict,
	ErrAccGenerationFailed: http.StatusInternalServerError,

	ErrIdtNotFound:              http.StatusNotFound,
	ErrIdtValidation:            http.StatusBadRequest,
	ErrIdtFieldNotTokenizable:   http.StatusBadRequest,
	ErrIdtFieldAlreadyTokenized: http.StatusConflict,
	ErrIdtFieldNotTokenized:     http.StatusBadRequest,
	ErrIdtFieldNotFound:         http.StatusBadRequest,
	ErrIdtTokenizationDisabled:  http.StatusForbidden,

	ErrReconNotFound:               http.StatusNotFound,
	ErrReconRuleNotFound:           http.StatusNotFound,
	ErrReconUploadFailed:           http.StatusBadRequest,
	ErrReconUploadProcessingFailed: http.StatusInternalServerError,
	ErrReconRuleInvalid:            http.StatusBadRequest,
	ErrReconMatchingRulesRequired:  http.StatusBadRequest,
	ErrReconExternalTxnsRequired:   http.StatusBadRequest,
	ErrReconStartFailed:            http.StatusInternalServerError,

	ErrMetaEntityNotFound:    http.StatusNotFound,
	ErrMetaUnsupportedEntity: http.StatusBadRequest,
	ErrMetaInvalidEntityID:   http.StatusBadRequest,

	ErrHookNotFound:        http.StatusNotFound,
	ErrHookInvalid:         http.StatusBadRequest,
	ErrHookOperationFailed: http.StatusInternalServerError,

	ErrSrchQueryInvalid:      http.StatusBadRequest,
	ErrSrchFailed:            http.StatusInternalServerError,
	ErrSrchReindexInProgress: http.StatusConflict,
	ErrSrchReindexNotStarted: http.StatusNotFound,

	ErrAdminBackupFailed: http.StatusInternalServerError,

	// Legacy codes — same statuses MapErrorToHTTPStatus implied, with the
	// BAD_REQUEST omission fixed (it previously fell through to 500).
	ErrNotFound:       http.StatusNotFound,
	ErrConflict:       http.StatusConflict,
	ErrBadRequest:     http.StatusBadRequest,
	ErrInvalidInput:   http.StatusBadRequest,
	ErrInternalServer: http.StatusInternalServerError,
	ErrRateLimited:    http.StatusTooManyRequests,
}

// legacyToCanonical maps the pre-catalog generic codes to their canonical
// GEN_* replacements. Internal layers (notably database/) keep constructing
// errors with the legacy codes; responses always surface canonical ones.
var legacyToCanonical = map[ErrorCode]ErrorCode{
	ErrNotFound:       ErrGenNotFound,
	ErrConflict:       ErrGenConflict,
	ErrBadRequest:     ErrGenBadRequest,
	ErrInvalidInput:   ErrGenValidation,
	ErrInternalServer: ErrGenInternal,
	ErrRateLimited:    ErrGenRateLimited,
}

// Normalize converts a legacy error code to its canonical equivalent.
// Canonical codes pass through unchanged.
func Normalize(code ErrorCode) ErrorCode {
	if canonical, ok := legacyToCanonical[code]; ok {
		return canonical
	}
	return code
}

// StatusForCode returns the default HTTP status for an error code.
// Unknown codes default to 500.
func StatusForCode(code ErrorCode) int {
	if status, ok := statusByCode[code]; ok {
		return status
	}
	return http.StatusInternalServerError
}
