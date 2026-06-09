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
	"context"
	"fmt"
	"math/big"

	"github.com/blnkfinance/blnk/model"
)

func (l *Blnk) GetBalanceLineage(ctx context.Context, balanceID string) (*BalanceLineage, error) {
	ctx, span := tracer.Start(ctx, "GetBalanceLineage")
	defer span.End()

	balance, err := l.datasource.GetBalanceByID(balanceID, nil, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}

	if !balance.TrackFundLineage {
		return nil, fmt.Errorf("balance %s does not have fund lineage tracking enabled", balanceID)
	}

	mappings, err := l.datasource.GetLineageMappings(ctx, balanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get lineage mappings: %w", err)
	}

	lineage := &BalanceLineage{
		BalanceID:        balanceID,
		Providers:        make([]ProviderBreakdown, 0),
		TotalWithLineage: big.NewInt(0),
	}

	l.populateLineageProviders(lineage, mappings)

	return lineage, nil
}

// populateLineageProviders populates the provider breakdown in a balance lineage.
//
// Parameters:
// - lineage *BalanceLineage: The lineage to populate.
// - mappings []model.LineageMapping: The lineage mappings.
func (l *Blnk) populateLineageProviders(lineage *BalanceLineage, mappings []model.LineageMapping) {
	for _, mapping := range mappings {
		breakdown, err := l.calculateProviderBreakdown(mapping)
		if err != nil {
			continue
		}

		lineage.Providers = append(lineage.Providers, *breakdown)
		lineage.TotalWithLineage = new(big.Int).Add(lineage.TotalWithLineage, breakdown.Available)

		if lineage.AggregateBalanceID == "" {
			lineage.AggregateBalanceID = mapping.AggregateBalanceID
		}
	}
}

// calculateProviderBreakdown calculates the fund breakdown for a provider.
//
// Parameters:
// - mapping model.LineageMapping: The lineage mapping for the provider.
//
// Returns:
// - *ProviderBreakdown: The calculated breakdown.
// - error: An error if the breakdown could not be calculated.
func (l *Blnk) calculateProviderBreakdown(mapping model.LineageMapping) (*ProviderBreakdown, error) {
	shadowBalance, err := l.datasource.GetBalanceByIDLite(mapping.ShadowBalanceID)
	if err != nil {
		return nil, err
	}

	debit := big.NewInt(0)
	credit := big.NewInt(0)

	if shadowBalance.DebitBalance != nil {
		debit = new(big.Int).Set(shadowBalance.DebitBalance)
	}
	if shadowBalance.CreditBalance != nil {
		credit = new(big.Int).Set(shadowBalance.CreditBalance)
	}

	available := new(big.Int).Sub(debit, credit)

	return &ProviderBreakdown{
		Provider:  mapping.Provider,
		Amount:    debit,
		Available: available,
		Spent:     credit,
		BalanceID: mapping.ShadowBalanceID,
	}, nil
}

// TransactionLineage represents the lineage information for a specific transaction.
//
// Fields:
// - TransactionID string: The ID of the transaction being queried.
// - FundAllocation []map[string]interface{}: The allocation of funds by provider for debit transactions.
// - ShadowTransactions []model.Transaction: The shadow transactions created for this transaction.
type TransactionLineage struct {
	TransactionID      string                   `json:"transaction_id"`
	FundAllocation     []map[string]interface{} `json:"fund_allocation,omitempty"`
	ShadowTransactions []model.Transaction      `json:"shadow_transactions"`
}

// GetTransactionLineage retrieves the lineage information for a transaction.
// It returns the fund allocation details and any shadow transactions created for the transaction.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transactionID string: The ID of the transaction to get lineage for.
//
// Returns:
// - *TransactionLineage: The lineage information for the transaction.
// - error: An error if the lineage could not be retrieved.
func (l *Blnk) GetTransactionLineage(ctx context.Context, transactionID string) (*TransactionLineage, error) {
	ctx, span := tracer.Start(ctx, "GetTransactionLineage")
	defer span.End()

	txn, err := l.GetTransaction(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	lineage := &TransactionLineage{
		TransactionID:      transactionID,
		FundAllocation:     l.extractFundAllocation(txn.MetaData),
		ShadowTransactions: make([]model.Transaction, 0),
	}

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, transactionID)
	if err == nil {
		lineage.ShadowTransactions = shadowTxns
	}

	return lineage, nil
}

// extractFundAllocation extracts fund allocation data from transaction metadata.
//
// Parameters:
// - metadata map[string]interface{}: The transaction metadata.
//
// Returns:
// - []map[string]interface{}: The fund allocation data, or nil if not present.
func (l *Blnk) extractFundAllocation(metadata map[string]interface{}) []map[string]interface{} {
	if metadata == nil {
		return nil
	}

	allocation, ok := metadata[LineageFundAllocation]
	if !ok {
		return nil
	}

	alloc, ok := allocation.([]interface{})
	if !ok {
		return nil
	}

	result := make([]map[string]interface{}, 0, len(alloc))
	for _, a := range alloc {
		if m, ok := a.(map[string]interface{}); ok {
			result = append(result, m)
		}
	}

	return result
}

// commitShadowTransactions commits all inflight shadow transactions for a parent transaction.
// It attempts to commit all shadows and returns an error if any fail (for outbox retry).
// Already-committed shadows return "already committed" error which is treated as success.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID.
// - amount *big.Int: The amount to commit (unused, shadow transactions use their own amounts).
//
// Returns:
// - error: An error if any shadow transaction failed to commit (excluding already-committed).
