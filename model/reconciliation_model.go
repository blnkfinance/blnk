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

import "time"

type Match struct {
	ExternalTransactionID string
	InternalTransactionID string
	ReconciliationID      string
	Amount                float64
	Date                  time.Time
}

type ExternalTransaction struct {
	ID          string    `json:"id"`
	Amount      float64   `json:"amount"`
	Reference   string    `json:"reference"`
	Currency    string    `json:"currency"`
	Description string    `json:"description"`
	Date        time.Time `json:"date"`
	Source      string    `json:"source"`
}

type Reconciliation struct {
	ID                    int64      `json:"-"`
	ReconciliationID      string     `json:"reconciliation_id"`
	UploadID              string     `json:"upload_id"`
	Status                string     `json:"status"`
	MatchedTransactions   int        `json:"matched_transactions"`
	UnmatchedTransactions int        `json:"unmatched_transactions"`
	IsDryRun              bool       `json:"is_dry_run"`
	StartedAt             time.Time  `json:"started_at"`
	CompletedAt           *time.Time `json:"completed_at"`
}

type ReconciliationProgress struct {
	LastProcessedExternalTxnID string `json:"last_processed_external_txn_id"`
	ProcessedCount             int    `json:"processed_count"`
}

type ReconciliationResults struct {
	ReconciliationID      string     `json:"reconciliation_id"`
	Status                string     `json:"status"`
	StartedAt             time.Time  `json:"started_at"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
	MatchedTransactions   []Match    `json:"matched_transactions"`
	UnmatchedTransactions []string   `json:"unmatched_transactions"`
}

type MatchingRule struct {
	ID          int64              `json:"-"`
	RuleID      string             `json:"rule_id"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Criteria    []MatchingCriteria `json:"criteria"`
}

type MatchingCriteria struct {
	Field          string  `json:"field"`
	Operator       string  `json:"operator"`
	Value          string  `json:"value"`
	Pattern        string  `json:"pattern"`
	AllowableDrift float64 `json:"allowable_drift"`
}
