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
	"errors"
	"strings"

	"github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/tokenization"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Every error response carries two payloads during the transition period:
// the legacy field clients depend on today (flat "error"/"errors" string,
// preserved verbatim) and the structured "error_detail" object with the
// canonical error code. The legacy field is removed — and error_detail
// renamed to error — at the next major release.
//
// The code catalog and its stability guarantee are published at
// https://docs.blnkfinance.com/advanced/error-codes (source:
// blnkfinance/blnk-docs, advanced/error-codes.mdx). error_detail.code is the
// stable field clients branch on; error, errors and error_detail.message are
// for display and may change between releases.

const errorDetailKey = "error_detail"

// sanitizedInternalMessage replaces raw internal error text on unclassified
// 5xx responses so database/driver details never leak to clients.
const sanitizedInternalMessage = "internal server error"

// sanitizeBindError rewrites a request-decoding error into a message about
// the request, rather than about Blnk's internals.
//
// encoding/json and math/big report decode failures in terms of Go types.
// A precise_amount sent as a JSON string, for example, surfaces verbatim as:
//
//	math/big: cannot unmarshal "\"5000\"" into a *big.Int
//
// The Go type name means nothing to an API client and exposes an
// implementation detail, so the known shape is rewritten to name the field
// class at fault and the type it expects. The JSON shape itself is the
// documentation's job, so no example is inlined here. Anything unrecognised
// is returned unchanged — this narrows a leak, it does not hide decode
// failures.
func sanitizeBindError(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "big.Int") || strings.HasPrefix(msg, "math/big:") {
		return "invalid numeric value: precise amount must be a JSON number"
	}
	return msg
}

type respondOptions struct {
	defaultCode     apierror.ErrorCode
	fallbackMessage string
	upgrades        map[apierror.ErrorCode]apierror.ErrorCode
	legacyKey       string
	legacyValue     interface{}
}

type respondOpt func(*respondOptions)

// withDefault sets the fallback code used when respondError cannot classify
// the error, instead of GEN_INTERNAL.
func withDefault(code apierror.ErrorCode) respondOpt {
	return func(o *respondOptions) { o.defaultCode = code }
}

// withFallbackMessage overrides the client-facing message when respondError
// falls back to the default code, for handlers that historically returned a
// fixed generic message instead of the raw error text.
func withFallbackMessage(msg string) respondOpt {
	return func(o *respondOptions) { o.fallbackMessage = msg }
}

// withUpgrade replaces a resolved generic code with a domain-specific one,
// e.g. withUpgrade(apierror.ErrGenNotFound, apierror.ErrLgrNotFound) in the
// ledger handlers.
func withUpgrade(from, to apierror.ErrorCode) respondOpt {
	return func(o *respondOptions) {
		if o.upgrades == nil {
			o.upgrades = map[apierror.ErrorCode]apierror.ErrorCode{}
		}
		o.upgrades[from] = to
	}
}

// withLegacyKey overrides the legacy field name; endpoints that historically
// responded with a plural "errors" key use this to stay byte-compatible.
func withLegacyKey(key string) respondOpt {
	return func(o *respondOptions) { o.legacyKey = key }
}

// withLegacyValue overrides the legacy field value when it was not a plain
// message string historically (e.g. filter parse-error slices).
func withLegacyValue(v interface{}) respondOpt {
	return func(o *respondOptions) { o.legacyValue = v }
}

func buildOptions(opts []respondOpt) *respondOptions {
	o := &respondOptions{legacyKey: "error"}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// respondCode writes the dual error payload for a known error condition.
func respondCode(c *gin.Context, code apierror.ErrorCode, message string, details interface{}, opts ...respondOpt) {
	writeError(c, code, message, details, buildOptions(opts))
}

// respondError resolves err to a catalog code and writes the dual payload.
// Resolution order: typed APIError → known sentinels → message patterns →
// fallback (withDefault or GEN_INTERNAL with a sanitized message).
func respondError(c *gin.Context, err error, opts ...respondOpt) {
	o := buildOptions(opts)
	if err == nil {
		writeError(c, apierror.ErrGenInternal, sanitizedInternalMessage, nil, o)
		return
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		writeError(c, apiErr.Code, apiErr.Message, apiErr.Details, o)
		return
	}

	if code, ok := classifySentinel(err); ok {
		writeError(c, code, err.Error(), nil, o)
		return
	}

	if code, ok := classifyMessage(err.Error()); ok {
		writeError(c, code, err.Error(), nil, o)
		return
	}

	if o.defaultCode != "" {
		msg := err.Error()
		if o.fallbackMessage != "" {
			msg = o.fallbackMessage
			logrus.WithError(err).Error("API error masked by fallback message")
		}
		writeError(c, o.defaultCode, msg, nil, o)
		return
	}

	logrus.WithError(err).Error("unclassified error in API response")
	writeError(c, apierror.ErrGenInternal, sanitizedInternalMessage, nil, o)
}

func writeError(c *gin.Context, code apierror.ErrorCode, message string, details interface{}, o *respondOptions) {
	code = apierror.Normalize(code)
	if to, ok := o.upgrades[code]; ok {
		code = to
	}
	legacyValue := o.legacyValue
	if legacyValue == nil {
		legacyValue = message
	}
	c.JSON(apierror.StatusForCode(code), gin.H{
		o.legacyKey:    legacyValue,
		errorDetailKey: apierror.APIError{Code: code, Message: message, Details: details},
	})
}

// classifySentinel maps known typed sentinel errors to catalog codes.
func classifySentinel(err error) (apierror.ErrorCode, bool) {
	switch {
	case errors.Is(err, database.ErrAPIKeyNotFound):
		return apierror.ErrAPIKeyNotFound, true
	case errors.Is(err, database.ErrInvalidAPIKey):
		return apierror.ErrAuthInvalidAPIKey, true
	case errors.Is(err, tokenization.ErrTokenizationDisabled):
		return apierror.ErrIdtTokenizationDisabled, true
	case errors.Is(err, redlock.ErrLockHeld), errors.Is(err, redlock.ErrLockWaitTimeout):
		return apierror.ErrGenResourceLocked, true
	case errors.Is(err, model2.ErrPrecisionMustBeInteger):
		return apierror.ErrTxnPrecisionNotInteger, true
	case errors.Is(err, blnk.ErrEntityNotFound):
		return apierror.ErrMetaEntityNotFound, true
	case errors.Is(err, blnk.ErrQueueBackpressure):
		return apierror.ErrQueueBackpressure, true
	case errors.Is(err, sql.ErrNoRows):
		return apierror.ErrGenNotFound, true
	}
	return "", false
}

// messagePattern entries are evaluated in order; more specific patterns must
// precede broader ones (e.g. "field ... not found" before bare "not found").
// This table bridges the core packages' unstructured fmt.Errorf messages to
// catalog codes without editing ~280 core call sites. Any core error later
// converted to a typed APIError bypasses this table entirely (it resolves in
// respondError's errors.As step), so entries here can be retired
// incrementally as core adopts typed errors.
type messagePattern struct {
	contains []string // all substrings must match
	code     apierror.ErrorCode
}

var messagePatterns = []messagePattern{
	// Transactions
	{[]string{"transaction", "not found"}, apierror.ErrTxnNotFound},
	{[]string{"not in inflight status"}, apierror.ErrTxnNotInflight},
	{[]string{"Transaction already committed"}, apierror.ErrTxnAlreadyCommitted},
	{[]string{"has already been voided"}, apierror.ErrTxnAlreadyVoided},
	{[]string{"has already been refunded"}, apierror.ErrTxnAlreadyRefunded},
	{[]string{"has already been used"}, apierror.ErrTxnDuplicateReference},
	{[]string{"cannot commit more than"}, apierror.ErrTxnCommitAmountExceeded},
	{[]string{"insufficient funds"}, apierror.ErrTxnInsufficientFunds},
	// UpdateBalances reports a settlement that outruns its hold as
	// "insufficient inflight debit balance" / "insufficient inflight credit
	// balance", which the pattern above does not match. Unclassified, the
	// condition inherited whichever fallback the calling handler happened to
	// set — GEN_BAD_REQUEST from the inflight route, GEN_INTERNAL elsewhere —
	// so the same failure surfaced under different codes depending on where
	// it was raised. It is an insufficient-funds condition against the
	// inflight balance, so it is classified as one.
	{[]string{"insufficient inflight"}, apierror.ErrTxnInsufficientFunds},
	{[]string{"transaction amount must be positive"}, apierror.ErrTxnInvalidAmount},
	{[]string{"transaction validation failed"}, apierror.ErrTxnValidation},
	{[]string{"reference is required"}, apierror.ErrTxnValidation},
	// Balances
	{[]string{"no balance data found for time"}, apierror.ErrBalHistoryNotFound},
	{[]string{"invalid timestamp format"}, apierror.ErrBalInvalidTimestamp},
	{[]string{"balance validation failed"}, apierror.ErrBalValidation},
	{[]string{"identity validation failed"}, apierror.ErrBalValidation},
	// Identity tokenization
	{[]string{"is not tokenizable"}, apierror.ErrIdtFieldNotTokenizable},
	{[]string{"is already tokenized"}, apierror.ErrIdtFieldAlreadyTokenized},
	{[]string{"is not tokenized"}, apierror.ErrIdtFieldNotTokenized},
	{[]string{"field", "not found"}, apierror.ErrIdtFieldNotFound},
	// Reconciliation rules
	{[]string{"rule name is required"}, apierror.ErrReconRuleInvalid},
	{[]string{"matching criteria is required"}, apierror.ErrReconRuleInvalid},
	{[]string{"field and operator are required"}, apierror.ErrReconRuleInvalid},
	{[]string{"invalid operator"}, apierror.ErrReconRuleInvalid},
	{[]string{"invalid field"}, apierror.ErrReconRuleInvalid},
	{[]string{"drift for"}, apierror.ErrReconRuleInvalid},
	// Metadata
	{[]string{"entity not found"}, apierror.ErrMetaEntityNotFound},
	{[]string{"unsupported entity type"}, apierror.ErrMetaUnsupportedEntity},
	{[]string{"invalid entity ID"}, apierror.ErrMetaInvalidEntityID},
	// Lineage
	{[]string{"does not have fund lineage tracking enabled"}, apierror.ErrGenBadRequest},
	// Infrastructure
	{[]string{"failed to acquire lock"}, apierror.ErrGenResourceLocked},
	{[]string{"no rows in result set"}, apierror.ErrGenNotFound},
	// Broad catch-all; must stay last.
	{[]string{"not found"}, apierror.ErrGenNotFound},
}

// resolveErrorCode picks the catalog code for err using the same resolution
// order respondError applies: typed APIError, then known sentinels, then
// message patterns. Handlers that write a body shape of their own — and so
// cannot call respondError — use it to reach the same code respondError
// would have chosen for the same error.
//
// Resolving from the error rather than from a message string matters: a
// wrapped APIError still carries its own code, whereas the flattened text
// only matches whichever pattern happens to fire first.
func resolveErrorCode(err error) (apierror.ErrorCode, bool) {
	if err == nil {
		return "", false
	}
	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return apierror.Normalize(apiErr.Code), true
	}
	if code, ok := classifySentinel(err); ok {
		return code, true
	}
	return classifyMessage(err.Error())
}

func classifyMessage(msg string) (apierror.ErrorCode, bool) {
	for _, p := range messagePatterns {
		matched := true
		for _, sub := range p.contains {
			if !strings.Contains(msg, sub) {
				matched = false
				break
			}
		}
		if matched {
			return p.code, true
		}
	}
	return "", false
}

// respondBareAPIError preserves the hooks endpoints' historical body shape —
// a top-level {code, message, details} object — while adding error_detail.
func respondBareAPIError(c *gin.Context, legacy apierror.APIError, code apierror.ErrorCode) {
	code = apierror.Normalize(code)
	c.JSON(apierror.StatusForCode(code), gin.H{
		"code":         legacy.Code,
		"message":      legacy.Message,
		"details":      legacy.Details,
		errorDetailKey: apierror.APIError{Code: code, Message: legacy.Message, Details: legacy.Details},
	})
}

// respondNestedAPIError preserves the admin endpoints' historical shape —
// the APIError object under "error" — while adding error_detail.
func respondNestedAPIError(c *gin.Context, legacy apierror.APIError, code apierror.ErrorCode) {
	code = apierror.Normalize(code)
	c.JSON(apierror.StatusForCode(code), gin.H{
		"error":        legacy,
		errorDetailKey: apierror.APIError{Code: code, Message: legacy.Message, Details: legacy.Details},
	})
}
