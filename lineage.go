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
	"encoding/json"
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
func (l *Blnk) processLineage(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) {
	ctx, span := tracer.Start(ctx, "ProcessLineage")
	defer span.End()

	provider := l.getLineageProvider(txn)

	// Validate provider against source balance
	// If source tracks lineage but doesn't have the provider, ignore it
	validatedProvider, err := l.validateLineageProvider(ctx, provider, sourceBalance)
	if err != nil {
		span.RecordError(err)
		logrus.Errorf("lineage provider validation failed: %v", err)
		notification.NotifyError(err)
		validatedProvider = ""
	}

	if provider != "" && validatedProvider == "" && sourceBalance != nil {
		span.AddEvent("Provider validation failed", trace.WithAttributes(
			attribute.String("requested_provider", provider),
			attribute.String("source_balance_id", sourceBalance.BalanceID),
		))
	}

	// Credit processing requires a validated provider to know the source of funds
	if validatedProvider != "" && destinationBalance != nil && destinationBalance.TrackFundLineage {
		if err := l.processLineageCredit(ctx, txn, destinationBalance, validatedProvider); err != nil {
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

// validateLineageProvider checks if the specified provider exists on the source balance.
// If the source tracks fund lineage but doesn't have the specified provider,
// returns empty string (provider should be ignored).
// If the source doesn't track lineage (e.g., @world), any provider is valid.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - provider string: The provider specified in transaction metadata.
// - sourceBalance *model.Balance: The source balance (may be nil or not track lineage).
//
// Returns:
// - string: The validated provider name (empty if invalid/should be ignored).
// - error: An error if validation fails (database errors only, not for invalid providers).
func (l *Blnk) validateLineageProvider(ctx context.Context, provider string, sourceBalance *model.Balance) (string, error) {
	if provider == "" {
		return "", nil
	}

	// Source is nil or doesn't track lineage - provider is valid
	if sourceBalance == nil || !sourceBalance.TrackFundLineage {
		return provider, nil
	}

	// Source tracks lineage - verify provider exists
	mapping, err := l.datasource.GetLineageMappingByProvider(ctx, sourceBalance.BalanceID, provider)
	if err != nil {
		return "", fmt.Errorf("failed to validate provider on source: %w", err)
	}

	if mapping == nil {
		logrus.Warnf("lineage provider validation: provider %q does not exist on source balance %s - ignoring provider",
			provider, sourceBalance.BalanceID)
		return "", nil
	}

	return provider, nil
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

		fundAllocation = append(fundAllocation, map[string]interface{}{
			"provider": mapping.Provider,
			"amount":   alloc.Amount,
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
// Uses a single batch query to fetch all shadow balances instead of N individual queries.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - mappings []model.LineageMapping: The lineage mappings to get sources from.
//
// Returns:
// - []LineageSource: The available fund sources.
// - error: An error if the sources could not be retrieved.
func (l *Blnk) getLineageSources(ctx context.Context, mappings []model.LineageMapping) ([]LineageSource, error) {
	if len(mappings) == 0 {
		return nil, nil
	}

	// Collect all shadow balance IDs for batch query
	shadowBalanceIDs := make([]string, 0, len(mappings))
	for _, mapping := range mappings {
		shadowBalanceIDs = append(shadowBalanceIDs, mapping.ShadowBalanceID)
	}

	// Fetch all shadow balances in a single query
	balances, err := l.datasource.GetBalancesByIDsLite(ctx, shadowBalanceIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shadow balances: %w", err)
	}

	// Build sources from the fetched balances
	var sources []LineageSource
	for _, mapping := range mappings {
		balance, exists := balances[mapping.ShadowBalanceID]
		if !exists {
			continue
		}

		if balance.DebitBalance != nil && balance.CreditBalance != nil && balance.DebitBalance.Cmp(big.NewInt(0)) > 0 {
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
	for i := range mappings {
		if mappings[i].ShadowBalanceID == shadowBalanceID {
			return &mappings[i]
		}
	}
	return nil
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
func (l *Blnk) commitShadowTransactions(ctx context.Context, parentTransactionID string, amount *big.Int) error {
	ctx, span := tracer.Start(ctx, "CommitShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	var failedShadows []string
	for _, shadow := range shadowTxns {
		_, err := l.CommitInflightTransaction(ctx, shadow.TransactionID, shadow.PreciseAmount)
		if err != nil {
			if strings.Contains(err.Error(), "already committed") ||
				strings.Contains(err.Error(), "not in inflight status") {
				span.AddEvent("Shadow transaction already processed, skipping", trace.WithAttributes(
					attribute.String("shadow.id", shadow.TransactionID),
				))
				continue
			}
			logrus.Errorf("failed to commit shadow transaction %s: %v", shadow.TransactionID, err)
			failedShadows = append(failedShadows, shadow.TransactionID)
			continue
		}
		span.AddEvent("Shadow transaction committed", trace.WithAttributes(
			attribute.String("shadow.id", shadow.TransactionID),
			attribute.String("parent.id", parentTransactionID),
		))
	}

	if len(failedShadows) > 0 {
		return fmt.Errorf("failed to commit %d shadow transactions: %v", len(failedShadows), failedShadows)
	}

	return nil
}

// voidShadowTransactions voids all inflight shadow transactions for a parent transaction.
// It attempts to void all shadows and returns an error if any fail (for outbox retry).
// Already-voided/committed shadows return "already committed" error which is treated as success.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID.
//
// Returns:
// - error: An error if any shadow transaction failed to void (excluding already-processed).
func (l *Blnk) voidShadowTransactions(ctx context.Context, parentTransactionID string) error {
	ctx, span := tracer.Start(ctx, "VoidShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	var failedShadows []string
	for _, shadow := range shadowTxns {
		_, err := l.VoidInflightTransaction(ctx, shadow.TransactionID)
		if err != nil {
			if strings.Contains(err.Error(), "already committed") ||
				strings.Contains(err.Error(), "not in inflight status") {
				span.AddEvent("Shadow transaction already processed, skipping", trace.WithAttributes(
					attribute.String("shadow.id", shadow.TransactionID),
				))
				continue
			}
			logrus.Errorf("failed to void shadow transaction %s: %v", shadow.TransactionID, err)
			failedShadows = append(failedShadows, shadow.TransactionID)
			continue
		}
		span.AddEvent("Shadow transaction voided", trace.WithAttributes(
			attribute.String("shadow.id", shadow.TransactionID),
			attribute.String("parent.id", parentTransactionID),
		))
	}

	if len(failedShadows) > 0 {
		return fmt.Errorf("failed to void %d shadow transactions: %v", len(failedShadows), failedShadows)
	}

	return nil
}

// queueShadowWork processes shadow commit or void work synchronously first, and queues
// to outbox for retry only if there are failures. This provides both immediate processing
// and guaranteed delivery for failed operations.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID whose shadows need processing.
// - lineageType string: Either LineageTypeShadowCommit or LineageTypeShadowVoid.
//
// Returns:
// - error: An error if all processing attempts failed.
func (l *Blnk) queueShadowWork(ctx context.Context, parentTransactionID string, lineageType string) error {
	ctx, span := tracer.Start(ctx, "QueueShadowWork")
	defer span.End()

	var processingErr error

	// Try to process shadows synchronously first
	switch lineageType {
	case model.LineageTypeShadowCommit:
		processingErr = l.commitShadowTransactions(ctx, parentTransactionID, nil)
	case model.LineageTypeShadowVoid:
		processingErr = l.voidShadowTransactions(ctx, parentTransactionID)
	}

	// If synchronous processing succeeded, we're done
	if processingErr == nil {
		span.AddEvent("Shadow work processed synchronously", trace.WithAttributes(
			attribute.String("parent.id", parentTransactionID),
			attribute.String("lineage.type", lineageType),
		))
		return nil
	}

	// Synchronous processing failed - queue to outbox for retry
	logrus.Warnf("Shadow %s failed for %s, queueing for retry: %v", lineageType, parentTransactionID, processingErr)

	// Create outbox entry for shadow work retry
	// Use a distinct ID to avoid conflict with regular lineage entries for same transaction
	shadowWorkID := fmt.Sprintf("%s_%s", parentTransactionID, lineageType)
	outbox := &model.LineageOutbox{
		TransactionID: shadowWorkID,
		LineageType:   lineageType,
		Payload:       []byte(fmt.Sprintf(`{"parent_transaction_id":"%s"}`, parentTransactionID)),
		MaxAttempts:   5,
	}

	if err := l.datasource.InsertLineageOutbox(ctx, outbox); err != nil {
		span.RecordError(err)
		// Log but don't fail - the original error is more important
		logrus.Errorf("failed to queue shadow work for retry: %v", err)
		return processingErr
	}

	span.AddEvent("Shadow work queued for retry via outbox", trace.WithAttributes(
		attribute.String("parent.id", parentTransactionID),
		attribute.String("lineage.type", lineageType),
		attribute.String("original.error", processingErr.Error()),
	))

	// Return original error since processing failed
	return processingErr
}

// PrepareLineageOutbox creates a LineageOutbox entry for atomic insertion with the transaction.
// This ensures lineage processing intent is captured in the same database transaction,
// guaranteeing no lineage work is lost even if subsequent async operations fail.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The transaction being processed.
// - sourceBalance *model.Balance: The source balance (may be nil).
// - destinationBalance *model.Balance: The destination balance (may be nil).
//
// Returns:
// - *model.LineageOutbox: The outbox entry to insert, or nil if no lineage processing needed.
func (l *Blnk) PrepareLineageOutbox(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.LineageOutbox {
	_, span := tracer.Start(ctx, "PrepareLineageOutbox")
	defer span.End()

	provider := l.getLineageProvider(txn)

	// Determine what type of lineage processing is needed
	needsCredit := provider != "" && destinationBalance != nil && destinationBalance.TrackFundLineage
	needsDebit := sourceBalance != nil && sourceBalance.TrackFundLineage

	if !needsCredit && !needsDebit {
		span.AddEvent("No lineage processing needed")
		return nil
	}

	lineageType := "both"
	if needsCredit && !needsDebit {
		lineageType = "credit"
	} else if needsDebit && !needsCredit {
		lineageType = "debit"
	}

	// Build the payload with transaction data needed for processing
	payload := LineageOutboxPayload{
		Amount:        txn.Amount,
		PreciseAmount: txn.PreciseAmount.String(),
		Currency:      txn.Currency,
		Precision:     txn.Precision,
		Reference:     txn.Reference,
		SkipQueue:     true,
		Inflight:      txn.Inflight,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		logrus.Errorf("failed to marshal lineage outbox payload: %v", err)
		span.RecordError(err)
		return nil
	}

	var srcID, dstID string
	if sourceBalance != nil {
		srcID = sourceBalance.BalanceID
	}
	if destinationBalance != nil {
		dstID = destinationBalance.BalanceID
	}

	outbox := &model.LineageOutbox{
		TransactionID:        txn.TransactionID,
		SourceBalanceID:      srcID,
		DestinationBalanceID: dstID,
		Provider:             provider,
		LineageType:          lineageType,
		Payload:              payloadBytes,
		MaxAttempts:          5,
		Inflight:             txn.Inflight, // explicit column, don't rely on JSON payload
	}

	span.AddEvent("Lineage outbox entry prepared", trace.WithAttributes(
		attribute.String("transaction_id", txn.TransactionID),
		attribute.String("lineage_type", lineageType),
		attribute.String("provider", provider),
	))

	return outbox
}

// ProcessLineageFromOutbox processes a lineage outbox entry.
// This is called by the outbox worker to perform deferred lineage processing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - entry model.LineageOutbox: The outbox entry to process.
//
// Returns:
// - error: An error if processing fails.
func (l *Blnk) ProcessLineageFromOutbox(ctx context.Context, entry model.LineageOutbox) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageFromOutbox")
	defer span.End()

	span.SetAttributes(
		attribute.String("outbox.id", fmt.Sprintf("%d", entry.ID)),
		attribute.String("outbox.transaction_id", entry.TransactionID),
		attribute.String("outbox.lineage_type", entry.LineageType),
	)

	// Handle shadow commit/void operations
	switch entry.LineageType {
	case model.LineageTypeShadowCommit, model.LineageTypeShadowVoid:
		// Extract parent transaction ID from payload
		var payload struct {
			ParentTransactionID string `json:"parent_transaction_id"`
		}
		if err := json.Unmarshal(entry.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal shadow work payload: %w", err)
		}
		parentTxnID := payload.ParentTransactionID
		if parentTxnID == "" {
			return fmt.Errorf("parent_transaction_id missing in shadow work payload")
		}

		if entry.LineageType == model.LineageTypeShadowCommit {
			span.AddEvent("Processing shadow commit from outbox", trace.WithAttributes(
				attribute.String("parent.transaction_id", parentTxnID),
			))
			if err := l.commitShadowTransactions(ctx, parentTxnID, nil); err != nil {
				return fmt.Errorf("failed to commit shadow transactions: %w", err)
			}
			span.AddEvent("Shadow commit completed from outbox")
		} else {
			span.AddEvent("Processing shadow void from outbox", trace.WithAttributes(
				attribute.String("parent.transaction_id", parentTxnID),
			))
			if err := l.voidShadowTransactions(ctx, parentTxnID); err != nil {
				return fmt.Errorf("failed to void shadow transactions: %w", err)
			}
			span.AddEvent("Shadow void completed from outbox")
		}
		return nil
	}

	// Handle regular lineage processing (credit, debit, both)
	// Fetch the transaction
	txn, err := l.GetTransaction(ctx, entry.TransactionID)
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}

	txn.Inflight = entry.Inflight

	// Fetch balances
	var sourceBalance, destinationBalance *model.Balance
	if entry.SourceBalanceID != "" {
		sourceBalance, err = l.datasource.GetBalanceByIDLite(entry.SourceBalanceID)
		if err != nil {
			logrus.Warnf("failed to get source balance %s for lineage processing: %v", entry.SourceBalanceID, err)
		}
	}
	if entry.DestinationBalanceID != "" {
		destinationBalance, err = l.datasource.GetBalanceByIDLite(entry.DestinationBalanceID)
		if err != nil {
			logrus.Warnf("failed to get destination balance %s for lineage processing: %v", entry.DestinationBalanceID, err)
		}
	}

	// Process lineage using the existing logic
	l.processLineage(ctx, txn, sourceBalance, destinationBalance)

	span.AddEvent("Lineage processing completed from outbox")
	return nil
}
