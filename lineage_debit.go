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

	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) processLineageDebit(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageDebit")
	defer span.End()

	// Get mappings first (before locking) to know which shadow balances we need
	mappings, err := l.datasource.GetLineageMappings(ctx, sourceBalance.BalanceID)
	if err != nil {
		return fmt.Errorf("failed to get lineage mappings: %w", err)
	}

	if len(mappings) == 0 {
		// Check if there are pending credit outbox entries for this balance
		// If so, retry later after those credits are processed
		hasPending, err := l.datasource.HasPendingCreditOutbox(ctx, sourceBalance.BalanceID)
		if err != nil {
			logrus.Warnf("failed to check pending credit outbox for balance %s: %v", sourceBalance.BalanceID, err)
			return nil
		}
		if hasPending {
			return fmt.Errorf("pending credit outbox entries exist for balance %s, retry later", sourceBalance.BalanceID)
		}
		return nil
	}

	// Collect all balance IDs for locking - source shadows + source aggregate
	lockKeys := make([]string, 0, len(mappings)+1)
	for _, m := range mappings {
		lockKeys = append(lockKeys, m.ShadowBalanceID)
	}
	lockKeys = append(lockKeys, mappings[0].AggregateBalanceID)

	// Pre-create destination lineage balances (if destination tracks lineage) BEFORE acquiring locks
	var destLineageBalances map[string]*destinationLineageInfo
	if destinationBalance != nil && destinationBalance.TrackFundLineage && destinationBalance.IdentityID != "" {
		destLineageBalances, err = l.prepareDestinationLineageBalances(ctx, mappings, destinationBalance)
		if err != nil {
			logrus.Warnf("failed to prepare destination lineage balances: %v", err)
		} else {
			for _, info := range destLineageBalances {
				lockKeys = append(lockKeys, info.shadowBalance.BalanceID)
				lockKeys = append(lockKeys, info.aggregateBalance.BalanceID)
			}
		}
	}

	locker, err := l.acquireLineageLocks(ctx, lockKeys)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer l.releaseLock(ctx, locker)

	sources, err := l.getLineageSources(ctx, mappings)
	if err != nil {
		return fmt.Errorf("failed to get lineage sources: %w", err)
	}

	allocations := l.calculateAllocation(sources, txn.PreciseAmount, sourceBalance.AllocationStrategy)

	sourceAggBalance, err := l.getSourceAggregateBalance(ctx, sourceBalance)
	if err != nil {
		return err
	}

	l.processAllocations(ctx, txn, allocations, mappings, sourceBalance, destinationBalance, sourceAggBalance, destLineageBalances)
	l.updateFundAllocationMetadata(ctx, txn, allocations, mappings)

	span.AddEvent("Lineage debit processed", trace.WithAttributes(attribute.Int("allocations", len(allocations))))
	return nil
}

// prepareDestinationLineageBalances pre-creates destination shadow and aggregate balances for all providers.
// This is called BEFORE acquiring locks to avoid nested lock acquisition deadlocks.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - mappings []model.LineageMapping: The source lineage mappings (one per provider).
// - destinationBalance *model.Balance: The destination balance.
//
// Returns:
// - map[string]*destinationLineageInfo: Map of provider to destination lineage balances.
// - error: An error if any balance could not be created.
func (l *Blnk) prepareDestinationLineageBalances(ctx context.Context, mappings []model.LineageMapping, destinationBalance *model.Balance) (map[string]*destinationLineageInfo, error) {
	result := make(map[string]*destinationLineageInfo, len(mappings))

	for _, mapping := range mappings {
		shadowBalance, aggBalance, err := l.getOrCreateDestinationLineageBalances(ctx, mapping.Provider, destinationBalance)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare destination lineage for provider %s: %w", mapping.Provider, err)
		}
		result[mapping.Provider] = &destinationLineageInfo{
			shadowBalance:    shadowBalance,
			aggregateBalance: aggBalance,
		}
	}

	return result, nil
}

// acquireLineageLocks acquires distributed locks for multiple shadow balances using MultiLocker.
// MultiLocker handles sorting (prevents deadlock) and deduplication automatically.
// This is used for both credit and debit lineage processing to prevent race conditions.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceIDs []string: The balance IDs to lock.
//
// Returns:
// - *redlock.MultiLocker: The acquired multi-lock.
// - error: An error if the locks could not be acquired.
func (l *Blnk) acquireLineageLocks(ctx context.Context, balanceIDs []string) (*redlock.MultiLocker, error) {
	// Prefix all keys to avoid collision with main transaction locks
	lockKeys := make([]string, 0, len(balanceIDs))
	for _, id := range balanceIDs {
		lockKeys = append(lockKeys, fmt.Sprintf("lineage:%s", id))
	}

	// MultiLocker handles deduplication and sorts keys lexicographically
	locker := redlock.NewMultiLocker(l.redis, lockKeys, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lineage locks: %w", err)
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
// - destLineageBalances map[string]*destinationLineageInfo: Pre-created destination lineage balances (may be nil).
func (l *Blnk) processAllocations(ctx context.Context, txn *model.Transaction, allocations []Allocation, mappings []model.LineageMapping, sourceBalance, destinationBalance, sourceAggBalance *model.Balance, destLineageBalances map[string]*destinationLineageInfo) {
	for i, alloc := range allocations {
		if alloc.Amount.Cmp(big.NewInt(0)) == 0 {
			continue
		}

		mapping := l.findMappingByShadowID(mappings, alloc.BalanceID)
		if mapping == nil {
			continue
		}

		if err := l.queueReleaseTransaction(ctx, txn, alloc, mapping, sourceBalance, sourceAggBalance, i); err != nil {
			logrus.Errorf("failed to queue release transaction: %v", err)
			continue
		}

		if err := l.processDestinationLineage(ctx, txn, alloc, mapping, sourceBalance, destinationBalance, destLineageBalances, i); err != nil {
			logrus.Errorf("failed to process destination lineage: %v", err)
			// Continue processing other allocations even if one fails
		}
	}
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
// - index int: The allocation index for reference uniqueness.
//
// Returns:
// - error: An error if the transaction could not be queued.
func (l *Blnk) queueReleaseTransaction(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, sourceAggBalance *model.Balance, index int) error {
	releaseTxn := &model.Transaction{
		Source:        sourceAggBalance.BalanceID,
		Destination:   alloc.BalanceID,
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
		SkipQueue: true,
		Inflight:  txn.Inflight,
	}

	_, err := l.QueueTransaction(ctx, releaseTxn)
	return err
}

// processDestinationLineage processes lineage tracking for the destination balance when it also tracks fund lineage.
// Uses pre-created destination balances to avoid nested lock acquisition (locks are already held by caller).
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The original transaction.
// - alloc Allocation: The allocation details.
// - mapping *model.LineageMapping: The lineage mapping for the provider.
// - sourceBalance *model.Balance: The source balance.
// - destinationBalance *model.Balance: The destination balance.
// - destLineageBalances map[string]*destinationLineageInfo: Pre-created destination lineage balances (may be nil).
// - index int: The allocation index for reference uniqueness.
//
// Returns:
// - error: An error if destination lineage processing fails.
func (l *Blnk) processDestinationLineage(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, destinationBalance *model.Balance, destLineageBalances map[string]*destinationLineageInfo, index int) error {
	if destinationBalance == nil || !destinationBalance.TrackFundLineage || destinationBalance.IdentityID == "" {
		return nil
	}

	// Use pre-created destination balances (locks already held by caller)
	if destLineageBalances == nil {
		return nil
	}

	destInfo, ok := destLineageBalances[mapping.Provider]
	if !ok || destInfo == nil {
		return nil
	}

	destShadowBalance := destInfo.shadowBalance
	destAggBalance := destInfo.aggregateBalance

	if err := l.queueReceiveTransaction(ctx, txn, alloc, mapping, sourceBalance, destinationBalance, destShadowBalance, destAggBalance, index); err != nil {
		return fmt.Errorf("failed to queue receive transaction: %w", err)
	}

	destMapping := model.LineageMapping{
		BalanceID:          destinationBalance.BalanceID,
		Provider:           mapping.Provider,
		ShadowBalanceID:    destShadowBalance.BalanceID,
		AggregateBalanceID: destAggBalance.BalanceID,
		IdentityID:         destinationBalance.IdentityID,
	}
	if err := l.datasource.UpsertLineageMapping(ctx, destMapping); err != nil {
		return fmt.Errorf("failed to upsert destination lineage mapping: %w", err)
	}

	return nil
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
func (l *Blnk) queueReceiveTransaction(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, destinationBalance, destShadowBalance, destAggBalance *model.Balance, index int) error {
	receiveTxn := &model.Transaction{
		Source:        destShadowBalance.BalanceID,
		Destination:   destAggBalance.BalanceID,
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
		SkipQueue:      true,
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
