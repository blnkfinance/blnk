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

import (
	"math/big"
	"time"

	"github.com/blnkfinance/blnk/model"
)

type RecordTransaction struct {
	Amount             float64                `json:"amount"`
	Precision          float64                `json:"precision"`
	OverdraftLimit     float64                `json:"overdraft_limit"`
	PreciseAmount      *big.Int               `json:"precise_amount"`
	AllowOverDraft     bool                   `json:"allow_overdraft"`
	Inflight           bool                   `json:"inflight"`
	SkipQueue          bool                   `json:"skip_queue"`
	Atomic             bool                   `json:"atomic"`
	Source             string                 `json:"source"`
	Reference          string                 `json:"reference"`
	Destination        string                 `json:"destination"`
	Description        string                 `json:"description"`
	Currency           string                 `json:"currency"`
	BalanceId          string                 `json:"balance_id"`
	ScheduledFor       string                 `json:"scheduled_for"`
	InflightExpiryDate string                 `json:"inflight_expiry_date,omitempty"`
	InflightCommitDate string                 `json:"inflight_commit_date,omitempty"`
	Sources            []model.Distribution   `json:"sources"`
	Destinations       []model.Distribution   `json:"destinations"`
	MetaData           map[string]interface{} `json:"meta_data"`
	EffectiveDate      *time.Time             `json:"effective_date,omitempty"`
}

// BulkTransactionRequest is the public API request shape for creating a batch
// of transactions. Each item mirrors the single transaction create payload.
type BulkTransactionRequest struct {
	Transactions []*RecordTransaction `json:"transactions"`
	Inflight     bool                 `json:"inflight"`
	Atomic       bool                 `json:"atomic"`
	RunAsync     bool                 `json:"run_async"`
	SkipQueue    bool                 `json:"skip_queue"`
}

func (r *BulkTransactionRequest) ToBulkTransactionRequest() *model.BulkTransactionRequest {
	transactions := make([]*model.Transaction, len(r.Transactions))
	for i, transaction := range r.Transactions {
		if transaction != nil {
			transactions[i] = transaction.ToTransaction()
		}
	}

	return &model.BulkTransactionRequest{
		Transactions: transactions,
		Inflight:     r.Inflight,
		Atomic:       r.Atomic,
		RunAsync:     r.RunAsync,
		SkipQueue:    r.SkipQueue,
	}
}

type InflightUpdate struct {
	Status        string   `json:"status"`
	Amount        float64  `json:"amount"`
	PreciseAmount *big.Int `json:"precise_amount,omitempty"`
	// SkipQueue processes the commit/void synchronously instead of routing it
	// through the inflight-commit queue (the default).
	SkipQueue bool `json:"skip_queue"`
}

// MaxBulkInflightItems caps the number of transactions accepted in a single
// bulk commit or bulk void call. Bulk calls are processed synchronously, so
// the cap exists to keep request latency and lock-holding bounded.
const MaxBulkInflightItems = 100

// MaxBulkTransactionItems caps the number of transactions accepted in a single
// CreateBulkTransactions request. The whole payload is held in memory, so the
// cap bounds memory use; it is larger than the inflight cap because bulk
// creates can run asynchronously.
const MaxBulkTransactionItems = 10000

// MaxInstantReconciliationItems caps the number of external_transactions
// accepted in a single instant-reconciliation request. The whole array is
// held in memory, so the cap bounds memory use.
const MaxInstantReconciliationItems = 10000

// BulkInflightVoidRequest voids many independently-created inflight
// transactions in one call. Each id is processed independently; partial
// failures are reported per-item in the response and do not abort the rest
// of the batch.
type BulkInflightVoidRequest struct {
	TransactionIDs []string `json:"transaction_ids"`
	// SkipQueue processes every item synchronously instead of routing them
	// through the inflight-commit queue (the default).
	SkipQueue bool `json:"skip_queue"`
}

// BulkInflightCommitItem describes one transaction in a bulk commit request.
// Amount/PreciseAmount carry the same semantics as the single-tx endpoint:
// zero means commit the full remaining inflight amount; non-zero performs a
// partial commit. PreciseAmount, when set, wins over Amount.
type BulkInflightCommitItem struct {
	TransactionID string   `json:"transaction_id"`
	Amount        float64  `json:"amount,omitempty"`
	PreciseAmount *big.Int `json:"precise_amount,omitempty"`
}

// BulkInflightCommitRequest commits many independently-created inflight
// transactions in one call. Unlike the void variant, each item can carry
// its own amount for partial commits.
type BulkInflightCommitRequest struct {
	Transactions []BulkInflightCommitItem `json:"transactions"`
	// SkipQueue processes every item synchronously instead of routing them
	// through the inflight-commit queue (the default).
	SkipQueue bool `json:"skip_queue"`
}

// BulkInflightResult is the per-item outcome reported in BulkInflightResponse.
// On success Status == "succeeded" and Code is empty. On failure Status ==
// "failed" with a stable Code (e.g. "ALREADY_VOIDED", "NOT_FOUND") that
// callers can branch on, plus a human-readable Message.
type BulkInflightResult struct {
	TransactionID string `json:"transaction_id"`
	Status        string `json:"status"`
	Code          string `json:"code,omitempty"`
	Message       string `json:"message,omitempty"`
}

// BulkInflightResponse is the envelope returned by both bulk endpoints.
// Succeeded + Failed == len(Results).
type BulkInflightResponse struct {
	Succeeded int                  `json:"succeeded"`
	Failed    int                  `json:"failed"`
	Results   []BulkInflightResult `json:"results"`
}
