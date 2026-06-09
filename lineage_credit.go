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
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) processLineageCredit(ctx context.Context, txn *model.Transaction, destBalance *model.Balance, provider string) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageCredit")
	defer span.End()

	identityID := destBalance.IdentityID
	if identityID == "" {
		return fmt.Errorf("destination balance %s has no identity_id for lineage tracking", destBalance.BalanceID)
	}

	// Get or create the shadow and aggregate balances first (before locking)
	shadowBalance, aggregateBalance, err := l.getOrCreateLineageBalances(ctx, identityID, provider, txn.Currency)
	if err != nil {
		return err
	}

	// Use MultiLocker to lock both shadow and aggregate balances
	locker, err := l.acquireLineageLocks(ctx, []string{shadowBalance.BalanceID, aggregateBalance.BalanceID})
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer l.releaseLock(ctx, locker)

	if err := l.queueShadowCreditTransaction(ctx, txn, destBalance, provider, shadowBalance, aggregateBalance, identityID); err != nil {
		return err
	}

	if err := l.upsertCreditLineageMapping(ctx, destBalance, provider, shadowBalance, aggregateBalance, identityID); err != nil {
		logrus.Errorf("failed to create lineage mapping after shadow transaction: %v (txn: %s, provider: %s)", err, txn.TransactionID, provider)
		span.RecordError(err)
		// Don't return error - shadow transaction succeeded, mapping is for optimization
	}

	span.AddEvent("Lineage credit processed", trace.WithAttributes(
		attribute.String("provider", provider),
		attribute.String("shadow_balance", shadowBalance.BalanceID),
	))
	return nil
}

// getOrCreateLineageBalances retrieves or creates the shadow and aggregate balances for lineage tracking.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - identityID string: The identity ID associated with the balance.
// - provider string: The fund provider identifier.
// - currency string: The currency for the balances.
//
// Returns:
// - *model.Balance: The shadow balance for the provider.
// - *model.Balance: The aggregate balance for all providers.
// - error: An error if the balances could not be retrieved or created.
func (l *Blnk) getOrCreateLineageBalances(ctx context.Context, identityID, provider, currency string) (*model.Balance, *model.Balance, error) {
	identity, err := l.datasource.GetIdentityByID(identityID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get identity %s: %w", identityID, err)
	}

	identifier := l.getIdentityIdentifier(identity)
	shadowBalanceIndicator := fmt.Sprintf("@%s_%s_lineage", provider, identifier)
	aggregateBalanceIndicator := fmt.Sprintf("@%s_lineage", identifier)

	shadowBalance, err := l.getOrCreateBalanceByIndicator(ctx, shadowBalanceIndicator, currency)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get/create shadow balance: %w", err)
	}

	aggregateBalance, err := l.getOrCreateBalanceByIndicator(ctx, aggregateBalanceIndicator, currency)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get/create aggregate balance: %w", err)
	}

	return shadowBalance, aggregateBalance, nil
}

// upsertCreditLineageMapping creates or updates the lineage mapping for a credit transaction.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - destBalance *model.Balance: The destination balance.
// - provider string: The fund provider identifier.
// - shadowBalance *model.Balance: The shadow balance for the provider.
// - aggregateBalance *model.Balance: The aggregate balance.
// - identityID string: The identity ID associated with the balance.
//
// Returns:
// - error: An error if the mapping could not be created.
func (l *Blnk) upsertCreditLineageMapping(ctx context.Context, destBalance *model.Balance, provider string, shadowBalance, aggregateBalance *model.Balance, identityID string) error {
	mapping := model.LineageMapping{
		BalanceID:          destBalance.BalanceID,
		Provider:           provider,
		ShadowBalanceID:    shadowBalance.BalanceID,
		AggregateBalanceID: aggregateBalance.BalanceID,
		IdentityID:         identityID,
	}

	if err := l.datasource.UpsertLineageMapping(ctx, mapping); err != nil {
		return fmt.Errorf("failed to upsert lineage mapping: %w", err)
	}

	return nil
}

// queueShadowCreditTransaction queues a shadow transaction to track credited funds from a provider.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The original credit transaction.
// - destBalance *model.Balance: The destination balance.
// - provider string: The fund provider identifier.
// - shadowBalance *model.Balance: The shadow balance for the provider.
// - aggregateBalance *model.Balance: The aggregate balance.
// - identityID string: The identity ID associated with the balance.
//
// Returns:
// - error: An error if the shadow transaction could not be queued.
func (l *Blnk) queueShadowCreditTransaction(ctx context.Context, txn *model.Transaction, destBalance *model.Balance, provider string, shadowBalance, aggregateBalance *model.Balance, identityID string) error {
	shadowTxn := &model.Transaction{
		Source:        shadowBalance.BalanceID,
		Destination:   aggregateBalance.BalanceID,
		Amount:        txn.Amount,
		PreciseAmount: new(big.Int).Set(txn.PreciseAmount),
		Currency:      destBalance.Currency,
		Precision:     txn.Precision,
		Reference:     fmt.Sprintf("%s_shadow_%s", txn.Reference, provider),
		Description:   fmt.Sprintf("Shadow credit from %s", provider),
		MetaData: map[string]interface{}{
			"_shadow_for":   txn.TransactionID,
			"_provider":     provider,
			"_identity_id":  identityID,
			"_lineage_type": "credit",
			"_main_balance": destBalance.BalanceID,
		},
		AllowOverdraft: true,
		SkipQueue:      true,
		Inflight:       txn.Inflight,
	}

	_, err := l.QueueTransaction(ctx, shadowTxn)
	if err != nil {
		return fmt.Errorf("failed to queue shadow credit transaction: %w", err)
	}

	return nil
}

// processLineageDebit processes a debit transaction for fund lineage tracking.
// It allocates funds from shadow balances based on the configured allocation strategy.
// Uses MultiLocker to lock ALL involved shadow balances (source AND destination) atomically,
// preventing both race conditions and deadlocks from nested lock acquisition.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The debit transaction being processed.
// - sourceBalance *model.Balance: The source balance being debited.
// - destinationBalance *model.Balance: The destination balance receiving the funds.
//
// Returns:
// - error: An error if the debit processing fails.
