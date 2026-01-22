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
	"sort"
	"strings"
	"time"

	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

// processLineage handles fund lineage tracking for a transaction.
// It processes both credit (incoming funds with provider tracking) and debit (fund allocation from shadow balances).
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The transaction being processed.
// - sourceBalance *model.Balance: The source balance for the transaction.
// - destinationBalance *model.Balance: The destination balance for the transaction.
func (l *Blnk) processLineage(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) {
	ctx, span := tracer.Start(ctx, "ProcessLineage")
	defer span.End()

	provider := l.getLineageProvider(txn)

	// Credit processing requires a provider to know the source of funds
	if provider != "" && destinationBalance != nil && destinationBalance.TrackFundLineage {
		if err := l.processLineageCredit(ctx, txn, destinationBalance, provider); err != nil {
			span.RecordError(err)
			logrus.Errorf("lineage credit processing failed: %v", err)
			notification.NotifyError(err)
		}
	}

	// Debit processing doesn't require a provider - it allocates from existing shadow balances
	if sourceBalance != nil && sourceBalance.TrackFundLineage {
		if err := l.processLineageDebit(ctx, txn, sourceBalance, destinationBalance); err != nil {
			span.RecordError(err)
			logrus.Errorf("lineage debit processing failed: %v", err)
			notification.NotifyError(err)
		}
	}

	span.AddEvent("Lineage processing completed")
}

// getLineageProvider extracts the fund provider from the transaction metadata.
//
// Parameters:
// - txn *model.Transaction: The transaction to extract the provider from.
//
// Returns:
// - string: The provider identifier, or empty string if not set.
func (l *Blnk) getLineageProvider(txn *model.Transaction) string {
	if txn.MetaData == nil {
		return ""
	}
	provider, ok := txn.MetaData[LineageProviderKey].(string)
	if !ok {
		return ""
	}
	return provider
}

// processLineageCredit processes a credit transaction for fund lineage tracking.
// It creates shadow balances and queues a shadow transaction to track the provider's funds.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The credit transaction being processed.
// - destBalance *model.Balance: The destination balance receiving the funds.
// - provider string: The identifier of the fund provider.
//
// Returns:
// - error: An error if the credit processing fails.
func (l *Blnk) processLineageCredit(ctx context.Context, txn *model.Transaction, destBalance *model.Balance, provider string) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageCredit")
	defer span.End()

	identityID := destBalance.IdentityID
	if identityID == "" {
		return fmt.Errorf("destination balance %s has no identity_id for lineage tracking", destBalance.BalanceID)
	}

	locker, err := l.acquireLineageCreditLock(ctx, identityID, provider, txn.Currency)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer l.releaseSingleLock(ctx, locker)

	shadowBalance, aggregateBalance, err := l.getOrCreateLineageBalances(ctx, identityID, provider, txn.Currency)
	if err != nil {
		return err
	}

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
		SkipQueue:      txn.SkipQueue,
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
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The debit transaction being processed.
// - sourceBalance *model.Balance: The source balance being debited.
// - destinationBalance *model.Balance: The destination balance receiving the funds.
//
// Returns:
// - error: An error if the debit processing fails.
func (l *Blnk) processLineageDebit(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageDebit")
	defer span.End()

	locker, err := l.acquireLineageDebitLock(ctx, sourceBalance.BalanceID)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer l.releaseSingleLock(ctx, locker)

	mappings, err := l.datasource.GetLineageMappings(ctx, sourceBalance.BalanceID)
	if err != nil {
		return fmt.Errorf("failed to get lineage mappings: %w", err)
	}

	if len(mappings) == 0 {
		return nil
	}

	sources, err := l.getLineageSources(ctx, mappings)
	if err != nil {
		return fmt.Errorf("failed to get lineage sources: %w", err)
	}

	allocations := l.calculateAllocation(sources, txn.PreciseAmount, sourceBalance.AllocationStrategy)

	sourceAggBalance, err := l.getSourceAggregateBalance(ctx, sourceBalance)
	if err != nil {
		return err
	}

	l.processAllocations(ctx, txn, allocations, mappings, sourceBalance, destinationBalance, sourceAggBalance)
	l.updateFundAllocationMetadata(ctx, txn, allocations, mappings)

	span.AddEvent("Lineage debit processed", trace.WithAttributes(attribute.Int("allocations", len(allocations))))
	return nil
}

// acquireLineageDebitLock acquires a distributed lock for lineage debit processing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The balance ID to lock.
//
// Returns:
// - *redlock.Locker: The acquired lock.
// - error: An error if the lock could not be acquired.
func (l *Blnk) acquireLineageDebitLock(ctx context.Context, balanceID string) (*redlock.Locker, error) {
	lockKey := fmt.Sprintf("lineage-debit:%s", balanceID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for lineage debit: %w", err)
	}

	return locker, nil
}

// acquireLineageCreditLock acquires a distributed lock for lineage credit processing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - identityID string: The identity ID.
// - provider string: The fund provider identifier.
// - currency string: The currency.
//
// Returns:
// - *redlock.Locker: The acquired lock.
// - error: An error if the lock could not be acquired.
func (l *Blnk) acquireLineageCreditLock(ctx context.Context, identityID, provider, currency string) (*redlock.Locker, error) {
	lockKey := fmt.Sprintf("lineage-credit:%s:%s:%s", identityID, provider, currency)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for lineage credit: %w", err)
	}

	return locker, nil
}

// acquireDestinationLineageLock acquires a distributed lock for destination lineage processing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - identityID string: The identity ID.
// - provider string: The fund provider identifier.
// - currency string: The currency.
//
// Returns:
// - *redlock.Locker: The acquired lock.
// - error: An error if the lock could not be acquired.
func (l *Blnk) acquireDestinationLineageLock(ctx context.Context, identityID, provider, currency string) (*redlock.Locker, error) {
	lockKey := fmt.Sprintf("lineage-dest:%s:%s:%s", identityID, provider, currency)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for destination lineage: %w", err)
	}

	return locker, nil
}

// getSourceAggregateBalance retrieves or creates the aggregate balance for the source identity.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - sourceBalance *model.Balance: The source balance.
//
// Returns:
// - *model.Balance: The aggregate balance.
// - error: An error if the balance could not be retrieved or created.
func (l *Blnk) getSourceAggregateBalance(ctx context.Context, sourceBalance *model.Balance) (*model.Balance, error) {
	sourceIdentity, err := l.datasource.GetIdentityByID(sourceBalance.IdentityID)
	if err != nil {
		return nil, fmt.Errorf("failed to get source identity: %w", err)
	}

	sourceIdentifier := l.getIdentityIdentifier(sourceIdentity)
	sourceAggIndicator := fmt.Sprintf("@%s_lineage", sourceIdentifier)

	sourceAggBalance, err := l.getOrCreateBalanceByIndicator(ctx, sourceAggIndicator, sourceBalance.Currency)
	if err != nil {
		return nil, fmt.Errorf("failed to get source aggregate balance: %w", err)
	}

	return sourceAggBalance, nil
}

// processAllocations processes fund allocations by queuing release and receive transactions.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The original transaction.
// - allocations []Allocation: The calculated allocations.
// - mappings []model.LineageMapping: The lineage mappings.
// - sourceBalance *model.Balance: The source balance.
// - destinationBalance *model.Balance: The destination balance.
// - sourceAggBalance *model.Balance: The source aggregate balance.
func (l *Blnk) processAllocations(ctx context.Context, txn *model.Transaction, allocations []Allocation, mappings []model.LineageMapping, sourceBalance, destinationBalance, sourceAggBalance *model.Balance) {
	for i, alloc := range allocations {
		if alloc.Amount.Cmp(big.NewInt(0)) == 0 {
			continue
		}

		mapping := l.findMappingByShadowID(mappings, alloc.BalanceID)
		if mapping == nil {
			continue
		}

		allocAmount := l.preciseAmountToFloat(alloc.Amount, txn.Precision)

		if err := l.queueReleaseTransaction(ctx, txn, alloc, mapping, sourceBalance, sourceAggBalance, allocAmount, i); err != nil {
			logrus.Errorf("failed to queue release transaction: %v", err)
			continue
		}

		l.processDestinationLineage(ctx, txn, alloc, mapping, sourceBalance, destinationBalance, allocAmount, i)
	}
}

// preciseAmountToFloat converts a precise amount to a float based on the precision.
//
// Parameters:
// - amount *big.Int: The precise amount.
// - precision float64: The precision multiplier.
//
// Returns:
// - float64: The converted float amount.
func (l *Blnk) preciseAmountToFloat(amount *big.Int, precision float64) float64 {
	floatAmount, _ := new(big.Float).SetInt(amount).Float64()
	return floatAmount / precision
}

// queueReleaseTransaction queues a transaction to release funds from the aggregate balance back to a shadow balance.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The original transaction.
// - alloc Allocation: The allocation details.
// - mapping *model.LineageMapping: The lineage mapping for the provider.
// - sourceBalance *model.Balance: The source balance.
// - sourceAggBalance *model.Balance: The source aggregate balance.
// - allocAmount float64: The allocation amount as a float.
// - index int: The allocation index for reference uniqueness.
//
// Returns:
// - error: An error if the transaction could not be queued.
func (l *Blnk) queueReleaseTransaction(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, sourceAggBalance *model.Balance, allocAmount float64, index int) error {
	releaseTxn := &model.Transaction{
		Source:        sourceAggBalance.BalanceID,
		Destination:   alloc.BalanceID,
		Amount:        allocAmount,
		PreciseAmount: new(big.Int).Set(alloc.Amount),
		Currency:      sourceBalance.Currency,
		Precision:     txn.Precision,
		Reference:     fmt.Sprintf("%s_release_%s_%d", txn.Reference, mapping.Provider, index),
		Description:   fmt.Sprintf("Release %s funds", mapping.Provider),
		MetaData: map[string]interface{}{
			"_shadow_for":   txn.TransactionID,
			"_provider":     mapping.Provider,
			"_lineage_type": "release",
			"_main_balance": sourceBalance.BalanceID,
			"_allocation":   sourceBalance.AllocationStrategy,
		},
		AllowOverdraft: true,
		SkipQueue:      txn.SkipQueue,
		Inflight:       txn.Inflight,
	}

	_, err := l.QueueTransaction(ctx, releaseTxn)
	return err
}

// processDestinationLineage processes lineage tracking for the destination balance when it also tracks fund lineage.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The original transaction.
// - alloc Allocation: The allocation details.
// - mapping *model.LineageMapping: The lineage mapping for the provider.
// - sourceBalance *model.Balance: The source balance.
// - destinationBalance *model.Balance: The destination balance.
// - allocAmount float64: The allocation amount as a float.
// - index int: The allocation index for reference uniqueness.
func (l *Blnk) processDestinationLineage(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, destinationBalance *model.Balance, allocAmount float64, index int) {
	if destinationBalance == nil || !destinationBalance.TrackFundLineage || destinationBalance.IdentityID == "" {
		return
	}

	locker, err := l.acquireDestinationLineageLock(ctx, destinationBalance.IdentityID, mapping.Provider, destinationBalance.Currency)
	if err != nil {
		logrus.Errorf("failed to acquire destination lineage lock: %v", err)
		return
	}
	defer l.releaseSingleLock(ctx, locker)

	destShadowBalance, destAggBalance, err := l.getOrCreateDestinationLineageBalances(ctx, mapping.Provider, destinationBalance)
	if err != nil {
		logrus.Errorf("failed to create destination lineage balances: %v", err)
		return
	}

	if err := l.queueReceiveTransaction(ctx, txn, alloc, mapping, sourceBalance, destinationBalance, destShadowBalance, destAggBalance, allocAmount, index); err != nil {
		logrus.Errorf("failed to queue receive transaction: %v", err)
		return
	}

	destMapping := model.LineageMapping{
		BalanceID:          destinationBalance.BalanceID,
		Provider:           mapping.Provider,
		ShadowBalanceID:    destShadowBalance.BalanceID,
		AggregateBalanceID: destAggBalance.BalanceID,
		IdentityID:         destinationBalance.IdentityID,
	}
	if err := l.datasource.UpsertLineageMapping(ctx, destMapping); err != nil {
		logrus.Errorf("failed to create destination lineage mapping after receive transaction: %v (txn: %s, provider: %s)", err, txn.TransactionID, mapping.Provider)
	}
}

// getOrCreateDestinationLineageBalances retrieves or creates shadow and aggregate balances for the destination.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - provider string: The fund provider identifier.
// - destinationBalance *model.Balance: The destination balance.
//
// Returns:
// - *model.Balance: The destination shadow balance.
// - *model.Balance: The destination aggregate balance.
// - error: An error if the balances could not be retrieved or created.
func (l *Blnk) getOrCreateDestinationLineageBalances(ctx context.Context, provider string, destinationBalance *model.Balance) (*model.Balance, *model.Balance, error) {
	destIdentity, err := l.datasource.GetIdentityByID(destinationBalance.IdentityID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get destination identity: %w", err)
	}

	destIdentifier := l.getIdentityIdentifier(destIdentity)
	destShadowIndicator := fmt.Sprintf("@%s_%s_lineage", provider, destIdentifier)
	destAggIndicator := fmt.Sprintf("@%s_lineage", destIdentifier)

	destShadowBalance, err := l.getOrCreateBalanceByIndicator(ctx, destShadowIndicator, destinationBalance.Currency)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create destination shadow balance: %w", err)
	}

	destAggBalance, err := l.getOrCreateBalanceByIndicator(ctx, destAggIndicator, destinationBalance.Currency)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create destination aggregate balance: %w", err)
	}

	return destShadowBalance, destAggBalance, nil
}

// queueReceiveTransaction queues a transaction to receive funds into the destination's shadow balance.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The original transaction.
// - alloc Allocation: The allocation details.
// - mapping *model.LineageMapping: The lineage mapping for the provider.
// - sourceBalance *model.Balance: The source balance.
// - destinationBalance *model.Balance: The destination balance.
// - destShadowBalance *model.Balance: The destination shadow balance.
// - destAggBalance *model.Balance: The destination aggregate balance.
// - allocAmount float64: The allocation amount as a float.
// - index int: The allocation index for reference uniqueness.
//
// Returns:
// - error: An error if the transaction could not be queued.
func (l *Blnk) queueReceiveTransaction(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, destinationBalance, destShadowBalance, destAggBalance *model.Balance, allocAmount float64, index int) error {
	receiveTxn := &model.Transaction{
		Source:        destShadowBalance.BalanceID,
		Destination:   destAggBalance.BalanceID,
		Amount:        allocAmount,
		PreciseAmount: new(big.Int).Set(alloc.Amount),
		Currency:      destinationBalance.Currency,
		Precision:     txn.Precision,
		Reference:     fmt.Sprintf("%s_receive_%s_%d", txn.Reference, mapping.Provider, index),
		Description:   fmt.Sprintf("Receive %s funds", mapping.Provider),
		MetaData: map[string]interface{}{
			"_shadow_for":   txn.TransactionID,
			"_provider":     mapping.Provider,
			"_lineage_type": "receive",
			"_main_balance": destinationBalance.BalanceID,
			"_from_balance": sourceBalance.BalanceID,
		},
		AllowOverdraft: true,
		SkipQueue:      txn.SkipQueue,
		Inflight:       txn.Inflight,
	}

	_, err := l.QueueTransaction(ctx, receiveTxn)
	return err
}

// updateFundAllocationMetadata updates the transaction metadata with fund allocation details.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The transaction to update.
// - allocations []Allocation: The calculated allocations.
// - mappings []model.LineageMapping: The lineage mappings.
func (l *Blnk) updateFundAllocationMetadata(ctx context.Context, txn *model.Transaction, allocations []Allocation, mappings []model.LineageMapping) {
	if len(allocations) == 0 {
		return
	}

	fundAllocation := l.buildFundAllocationList(allocations, mappings, txn.Precision)
	if len(fundAllocation) == 0 {
		return
	}

	newMetadata := map[string]interface{}{
		LineageFundAllocation: fundAllocation,
	}
	if err := l.datasource.UpdateTransactionMetadata(ctx, txn.TransactionID, newMetadata); err != nil {
		logrus.Errorf("failed to update transaction with fund allocation: %v", err)
	}
}

// buildFundAllocationList builds a list of fund allocations for metadata storage.
//
// Parameters:
// - allocations []Allocation: The calculated allocations.
// - mappings []model.LineageMapping: The lineage mappings.
// - precision float64: The precision multiplier.
//
// Returns:
// - []map[string]interface{}: The fund allocation list for metadata.
func (l *Blnk) buildFundAllocationList(allocations []Allocation, mappings []model.LineageMapping, precision float64) []map[string]interface{} {
	fundAllocation := make([]map[string]interface{}, 0, len(allocations))

	for _, alloc := range allocations {
		mapping := l.findMappingByShadowID(mappings, alloc.BalanceID)
		if mapping == nil {
			continue
		}

		allocAmount := l.preciseAmountToFloat(alloc.Amount, precision)
		fundAllocation = append(fundAllocation, map[string]interface{}{
			"provider": mapping.Provider,
			"amount":   allocAmount,
		})
	}

	return fundAllocation
}

// getIdentityIdentifier generates a unique identifier string for an identity.
// It uses the identity's name (first/last or organization) combined with a portion of the ID.
//
// Parameters:
// - identity *model.Identity: The identity to generate an identifier for.
//
// Returns:
// - string: The generated identifier.
func (l *Blnk) getIdentityIdentifier(identity *model.Identity) string {
	var namePart string

	if identity.FirstName != "" && identity.LastName != "" {
		namePart = strings.ToLower(fmt.Sprintf("%s_%s", identity.FirstName, identity.LastName))
	} else if identity.OrganizationName != "" {
		namePart = strings.ToLower(strings.ReplaceAll(identity.OrganizationName, " ", "_"))
	} else {
		// No name available, use full ID
		return identity.IdentityID
	}

	// Use first 8 characters of ID for uniqueness
	idPart := identity.IdentityID
	if len(idPart) > 8 {
		idPart = idPart[:8]
	}

	return fmt.Sprintf("%s_%s", namePart, idPart)
}

// getLineageSources retrieves the available fund sources from shadow balances for allocation.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - mappings []model.LineageMapping: The lineage mappings to get sources from.
//
// Returns:
// - []LineageSource: The available fund sources.
// - error: An error if the sources could not be retrieved.
func (l *Blnk) getLineageSources(ctx context.Context, mappings []model.LineageMapping) ([]LineageSource, error) {
	var sources []LineageSource

	for _, mapping := range mappings {
		balance, err := l.datasource.GetBalanceByIDLite(mapping.ShadowBalanceID)
		if err != nil {
			continue
		}

		if balance.DebitBalance != nil && balance.DebitBalance.Cmp(big.NewInt(0)) > 0 {
			available := new(big.Int).Sub(balance.DebitBalance, balance.CreditBalance)
			if available.Cmp(big.NewInt(0)) > 0 {
				sources = append(sources, LineageSource{
					BalanceID: mapping.ShadowBalanceID,
					Balance:   available,
					CreatedAt: mapping.CreatedAt,
				})
			}
		}
	}

	return sources, nil
}

// calculateAllocation calculates fund allocations based on the specified strategy.
// Supported strategies are FIFO, LIFO, and PROPORTIONAL.
//
// Parameters:
// - sources []LineageSource: The available fund sources.
// - amount *big.Int: The amount to allocate.
// - strategy string: The allocation strategy (FIFO, LIFO, or PROPORTIONAL).
//
// Returns:
// - []Allocation: The calculated allocations.
func (l *Blnk) calculateAllocation(sources []LineageSource, amount *big.Int, strategy string) []Allocation {
	if len(sources) == 0 {
		return nil
	}

	switch strategy {
	case AllocationLIFO:
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].CreatedAt.After(sources[j].CreatedAt)
		})
		return l.sequentialAllocation(sources, amount)
	case AllocationProp:
		return l.proportionalAllocation(sources, amount)
	default:
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].CreatedAt.Before(sources[j].CreatedAt)
		})
		return l.sequentialAllocation(sources, amount)
	}
}

// sequentialAllocation allocates funds sequentially from sources (used for FIFO/LIFO).
//
// Parameters:
// - sources []LineageSource: The available fund sources in order.
// - amount *big.Int: The amount to allocate.
//
// Returns:
// - []Allocation: The calculated allocations.
func (l *Blnk) sequentialAllocation(sources []LineageSource, amount *big.Int) []Allocation {
	var allocations []Allocation

	// Skip if amount is zero or negative
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}

	remaining := new(big.Int).Set(amount)

	for _, source := range sources {
		if remaining.Cmp(big.NewInt(0)) <= 0 {
			break
		}

		alloc := new(big.Int)
		if source.Balance.Cmp(remaining) >= 0 {
			alloc.Set(remaining)
		} else {
			alloc.Set(source.Balance)
		}

		// Skip zero allocations
		if alloc.Cmp(big.NewInt(0)) <= 0 {
			continue
		}

		allocations = append(allocations, Allocation{
			BalanceID: source.BalanceID,
			Amount:    alloc,
		})

		remaining.Sub(remaining, alloc)
	}

	// Log warning if we couldn't allocate the full amount
	if remaining.Cmp(big.NewInt(0)) > 0 {
		logrus.Warnf("sequential allocation: could not allocate full amount, %s remaining unallocated", remaining.String())
	}

	return allocations
}

// proportionalAllocation allocates funds proportionally across all sources.
//
// Parameters:
// - sources []LineageSource: The available fund sources.
// - amount *big.Int: The amount to allocate.
//
// Returns:
// - []Allocation: The calculated allocations.
func (l *Blnk) proportionalAllocation(sources []LineageSource, amount *big.Int) []Allocation {
	var allocations []Allocation

	// Skip if amount is zero or negative
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}

	total := big.NewInt(0)
	for _, source := range sources {
		total.Add(total, source.Balance)
	}

	if total.Cmp(big.NewInt(0)) == 0 {
		return nil
	}

	// Cap amount at total available to prevent over-allocation
	effectiveAmount := new(big.Int).Set(amount)
	if effectiveAmount.Cmp(total) > 0 {
		logrus.Warnf("proportional allocation: requested amount %s exceeds total available %s, capping at available", amount.String(), total.String())
		effectiveAmount.Set(total)
	}

	remaining := new(big.Int).Set(effectiveAmount)

	for i, source := range sources {
		var alloc *big.Int

		if i == len(sources)-1 {
			// Last source gets the remainder to handle rounding
			alloc = new(big.Int).Set(remaining)
		} else {
			// Calculate proportional share: (effectiveAmount * source.Balance) / total
			proportion := new(big.Int).Mul(effectiveAmount, source.Balance)
			alloc = new(big.Int).Div(proportion, total)
		}

		// Cap at source's available balance
		if alloc.Cmp(source.Balance) > 0 {
			alloc.Set(source.Balance)
		}

		// Skip zero allocations
		if alloc.Cmp(big.NewInt(0)) <= 0 {
			continue
		}

		allocations = append(allocations, Allocation{
			BalanceID: source.BalanceID,
			Amount:    alloc,
		})

		remaining.Sub(remaining, alloc)
	}

	return allocations
}

// findMappingByShadowID finds a lineage mapping by its shadow balance ID.
//
// Parameters:
// - mappings []model.LineageMapping: The lineage mappings to search.
// - shadowBalanceID string: The shadow balance ID to find.
//
// Returns:
// - *model.LineageMapping: The matching mapping, or nil if not found.
func (l *Blnk) findMappingByShadowID(mappings []model.LineageMapping, shadowBalanceID string) *model.LineageMapping {
	for _, mapping := range mappings {
		if mapping.ShadowBalanceID == shadowBalanceID {
			return &mapping
		}
	}
	return nil
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

// GetBalanceLineage retrieves the fund lineage for a balance.
// It returns a breakdown of funds by provider, showing how much was received and spent from each source.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The ID of the balance to get lineage for.
//
// Returns:
// - *BalanceLineage: The lineage information for the balance.
// - error: An error if the lineage could not be retrieved.
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
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID.
// - amount *big.Int: The amount to commit (unused, shadow transactions use their own amounts).
//
// Returns:
// - error: An error if shadow transactions could not be retrieved.
func (l *Blnk) commitShadowTransactions(ctx context.Context, parentTransactionID string, amount *big.Int) error {
	ctx, span := tracer.Start(ctx, "CommitShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	for _, shadow := range shadowTxns {
		if shadow.Status != StatusInflight {
			continue
		}

		_, err := l.CommitInflightTransaction(ctx, shadow.TransactionID, shadow.PreciseAmount)
		if err != nil {
			logrus.Errorf("failed to commit shadow transaction %s: %v", shadow.TransactionID, err)
			continue
		}
		span.AddEvent("Shadow transaction committed", trace.WithAttributes(
			attribute.String("shadow.id", shadow.TransactionID),
			attribute.String("parent.id", parentTransactionID),
		))
	}

	return nil
}

// voidShadowTransactions voids all inflight shadow transactions for a parent transaction.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID.
//
// Returns:
// - error: An error if shadow transactions could not be retrieved.
func (l *Blnk) voidShadowTransactions(ctx context.Context, parentTransactionID string) error {
	ctx, span := tracer.Start(ctx, "VoidShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	for _, shadow := range shadowTxns {
		if shadow.Status != StatusInflight {
			continue
		}

		_, err := l.VoidInflightTransaction(ctx, shadow.TransactionID)
		if err != nil {
			logrus.Errorf("failed to void shadow transaction %s: %v", shadow.TransactionID, err)
			continue
		}
		span.AddEvent("Shadow transaction voided", trace.WithAttributes(
			attribute.String("shadow.id", shadow.TransactionID),
			attribute.String("parent.id", parentTransactionID),
		))
	}

	return nil
}
