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

package blnk

import (
	"math/big"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// LineageProviderKey is the metadata key used to identify the provider of funds in a transaction.
const LineageProviderKey = "BLNK_LINEAGE_PROVIDER"

// LineageFundAllocation is the metadata key used to store fund allocation details in a transaction.
const LineageFundAllocation = "BLNK_FUND_ALLOCATION"

// Allocation strategies for fund lineage debit processing.
const (
	AllocationFIFO = "FIFO"
	AllocationLIFO = "LIFO"
	AllocationProp = "PROPORTIONAL"
)

// LineageSource represents a source of funds available for allocation during lineage debit processing.
//
// Fields:
// - BalanceID string: The ID of the shadow balance holding the funds.
// - Balance *big.Int: The available balance amount.
// - CreatedAt time.Time: The creation time of the lineage mapping, used for FIFO/LIFO ordering.
type LineageSource struct {
	BalanceID string
	Balance   *big.Int
	CreatedAt time.Time
}

// Allocation represents the amount allocated from a specific shadow balance during debit processing.
//
// Fields:
// - BalanceID string: The ID of the shadow balance from which funds are allocated.
// - Amount *big.Int: The amount allocated from this balance.
type Allocation struct {
	BalanceID string
	Amount    *big.Int
}

// LineageOutboxPayload contains the transaction data needed for deferred lineage processing.
type LineageOutboxPayload struct {
	Amount        float64 `json:"amount"`
	PreciseAmount string  `json:"precise_amount"`
	Currency      string  `json:"currency"`
	Precision     float64 `json:"precision"`
	Reference     string  `json:"reference"`
	SkipQueue     bool    `json:"skip_queue"`
	Inflight      bool    `json:"inflight"`
}

// ProviderBreakdown represents the fund breakdown for a specific provider in a balance's lineage.
//
// Fields:
// - Provider string: The name/identifier of the fund provider.
// - Amount *big.Int: The total amount received from this provider.
// - Available *big.Int: The amount still available (not yet spent).
// - Spent *big.Int: The amount that has been debited.
// - BalanceID string: The ID of the shadow balance tracking this provider's funds.
type ProviderBreakdown struct {
	Provider  string   `json:"provider"`
	Amount    *big.Int `json:"amount"`
	Available *big.Int `json:"available"`
	Spent     *big.Int `json:"spent"`
	BalanceID string   `json:"shadow_balance_id"`
}

// BalanceLineage represents the complete fund lineage for a balance.
//
// Fields:
// - BalanceID string: The ID of the balance being queried.
// - TotalWithLineage *big.Int: The total funds tracked across all providers.
// - AggregateBalanceID string: The ID of the aggregate shadow balance.
// - Providers []ProviderBreakdown: The breakdown of funds by provider.
type BalanceLineage struct {
	BalanceID          string              `json:"balance_id"`
	TotalWithLineage   *big.Int            `json:"total_with_lineage"`
	AggregateBalanceID string              `json:"aggregate_balance_id"`
	Providers          []ProviderBreakdown `json:"providers"`
}

type destinationLineageInfo struct {
	shadowBalance    *model.Balance
	aggregateBalance *model.Balance
}

// processLineage handles fund lineage tracking for a transaction.
// It processes both credit (incoming funds with provider tracking) and debit (fund allocation from shadow balances).
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The transaction being processed.
// - sourceBalance *model.Balance: The source balance for the transaction.
// - destinationBalance *model.Balance: The destination balance for the transaction.
