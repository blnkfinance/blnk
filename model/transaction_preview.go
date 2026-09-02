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
package model

// Balance roles reported by a projection.
const (
	PreviewRoleSource      = "source"
	PreviewRoleDestination = "destination"
)

// TransactionPreview is the projected effect of a transaction that was
// evaluated but never applied.
//
// Monetary values are strings in minor units, matching precise_amount, so no
// precision is lost in transit.
type TransactionPreview struct {
	DryRun bool `json:"dry_run"`

	// WouldApply reports whether a real post of this transaction would be
	// accepted against the balances as they currently stand. When false,
	// Rejection carries the reason.
	WouldApply bool              `json:"would_apply"`
	Rejection  *PreviewRejection `json:"rejection,omitempty"`

	// Operation names the settlement being projected on the inflight endpoint:
	// "commit" or "void". Empty for ordinary transaction projections.
	Operation string `json:"operation,omitempty"`

	// Status is the status the transaction would carry once applied. It is
	// omitted when WouldApply is false: a rejected projection has no resulting
	// status, because the real endpoint returns an error and writes no
	// transaction at all. See Finalize.
	Status        string              `json:"status,omitempty"`
	Reference     string              `json:"reference,omitempty"`
	Currency      string              `json:"currency"`
	Amount        float64             `json:"amount"`
	PreciseAmount string              `json:"precise_amount"`
	Precision     float64             `json:"precision"`
	Balances      []BalanceProjection `json:"balances"`

	// Legs is populated for multi-source/destination transactions: one entry
	// per split, using the same distribution math a real post would use.
	Legs []LegProjection `json:"legs,omitempty"`

	// Notes carry advisory information that is not a rejection — an ignored
	// field, or a condition worth surfacing before the caller posts for real.
	Notes []string `json:"notes,omitempty"`
}

// PreviewRejection describes why a projected transaction would not apply.
//
// Code is the same error code a real post would return, so existing
// client-side handling for e.g. TXN_INSUFFICIENT_FUNDS works against a
// preview unchanged.
type PreviewRejection struct {
	Code    string `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// BalanceProjection is one balance's state before and after the projected
// transaction. Current* is the snapshot the projection started from;
// Resulting* is that snapshot with the transaction applied in memory.
type BalanceProjection struct {
	BalanceID string `json:"balance_id"`
	Role      string `json:"role"`
	Currency  string `json:"currency"`

	// Virtual marks a balance that does not exist yet — an @indicator that a
	// real post would create on demand. It is projected against zero and is
	// not created by the preview.
	Virtual bool `json:"virtual,omitempty"`

	CurrentBalance               string `json:"current_balance"`
	CurrentAvailable             string `json:"current_available"`
	CurrentCreditBalance         string `json:"current_credit_balance"`
	CurrentDebitBalance          string `json:"current_debit_balance"`
	CurrentInflightDebitBalance  string `json:"current_inflight_debit_balance"`
	CurrentInflightCreditBalance string `json:"current_inflight_credit_balance"`

	ResultingBalance               string `json:"resulting_balance"`
	ResultingAvailable             string `json:"resulting_available"`
	ResultingCreditBalance         string `json:"resulting_credit_balance"`
	ResultingDebitBalance          string `json:"resulting_debit_balance"`
	ResultingInflightDebitBalance  string `json:"resulting_inflight_debit_balance"`
	ResultingInflightCreditBalance string `json:"resulting_inflight_credit_balance"`
}

// LegProjection is one split of a multi-source/destination transaction.
type LegProjection struct {
	Identifier    string  `json:"identifier"`
	Role          string  `json:"role"`
	PreciseAmount string  `json:"precise_amount"`
	Amount        float64 `json:"amount"`
}

// AddNote appends an advisory note to the projection.
func (preview *TransactionPreview) AddNote(note string) {
	preview.Notes = append(preview.Notes, note)
}

// BulkTransactionPreview is the projected effect of a batch that was evaluated
// but never applied.
type BulkTransactionPreview struct {
	DryRun bool `json:"dry_run"`

	// WouldApply is false when any item in the batch would be rejected.
	WouldApply bool `json:"would_apply"`

	// Cumulative reports whether items were projected against each other's
	// effects. That mirrors how the batch would really run: items are applied
	// one after another only when skip_queue is set, and are otherwise
	// dispatched concurrently with no guaranteed order.
	Cumulative bool `json:"cumulative"`
	Atomic     bool `json:"atomic"`

	Results []TransactionPreview `json:"results"`

	// Balances is the batch's combined effect per balance, and is reported only
	// in cumulative mode — without a guaranteed order there is no single
	// combined outcome to state.
	Balances []BalanceProjection `json:"balances,omitempty"`

	Notes []string `json:"notes,omitempty"`
}

// AddNote appends an advisory note to the batch projection.
func (preview *BulkTransactionPreview) AddNote(note string) {
	preview.Notes = append(preview.Notes, note)
}

// Finalize reconciles fields that only describe a transaction that would
// actually apply.
//
// Status is set early, from the status the transaction would carry once
// applied, and the projection only later discovers it would be rejected -- an
// insufficient balance, a failing leg, a validation failure downstream. Left
// alone, the response then asserts two things that cannot both be true:
//
//	"would_apply": false,
//	"rejection":   { "code": "TXN_INSUFFICIENT_FUNDS", ... },
//	"status":      "APPLIED"
//
// A rejected projection has no resulting status. The real endpoint returns an
// error and writes no transaction, so there is no status to report; clearing
// it lets the omitempty tag drop the field rather than name an outcome that
// cannot happen. Callers read would_apply and rejection for that answer.
func (preview *TransactionPreview) Finalize() {
	if !preview.WouldApply {
		preview.Status = ""
	}
}

// Finalize applies the same reconciliation to a batch and to each of its
// items, so a rejected item does not report a status either.
func (preview *BulkTransactionPreview) Finalize() {
	for i := range preview.Results {
		preview.Results[i].Finalize()
	}
}
