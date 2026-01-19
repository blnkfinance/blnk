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

const (
	LineageProviderKey    = "BLNK_LINEAGE_PROVIDER"
	LineageFundAllocation = "BLNK_FUND_ALLOCATION"
	AllocationFIFO        = "FIFO"
	AllocationLIFO        = "LIFO"
	AllocationProp        = "PROPORTIONAL"
)

type LineageSource struct {
	BalanceID string
	Balance   *big.Int
	CreatedAt time.Time
}

type Allocation struct {
	BalanceID string
	Amount    *big.Int
}

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

func (l *Blnk) processLineageCredit(ctx context.Context, txn *model.Transaction, destBalance *model.Balance, provider string) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageCredit")
	defer span.End()

	identityID := destBalance.IdentityID
	if identityID == "" {
		return fmt.Errorf("destination balance %s has no identity_id for lineage tracking", destBalance.BalanceID)
	}

	shadowBalance, aggregateBalance, err := l.getOrCreateLineageBalances(ctx, identityID, provider, txn.Currency)
	if err != nil {
		return err
	}

	if err := l.upsertCreditLineageMapping(ctx, destBalance, provider, shadowBalance, aggregateBalance, identityID); err != nil {
		return err
	}

	if err := l.queueShadowCreditTransaction(ctx, txn, destBalance, provider, shadowBalance, aggregateBalance, identityID); err != nil {
		return err
	}

	span.AddEvent("Lineage credit processed", trace.WithAttributes(
		attribute.String("provider", provider),
		attribute.String("shadow_balance", shadowBalance.BalanceID),
	))
	return nil
}

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

func (l *Blnk) processLineageDebit(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageDebit")
	defer span.End()

	locker, err := l.acquireLineageDebitLock(ctx, sourceBalance.BalanceID)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer l.releaseLock(ctx, locker)

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

func (l *Blnk) acquireLineageDebitLock(ctx context.Context, balanceID string) (*redlock.Locker, error) {
	lockKey := fmt.Sprintf("lineage-debit:%s", balanceID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for lineage debit: %w", err)
	}

	return locker, nil
}

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

func (l *Blnk) preciseAmountToFloat(amount *big.Int, precision float64) float64 {
	floatAmount, _ := new(big.Float).SetInt(amount).Float64()
	return floatAmount / precision
}

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

func (l *Blnk) processDestinationLineage(ctx context.Context, txn *model.Transaction, alloc Allocation, mapping *model.LineageMapping, sourceBalance, destinationBalance *model.Balance, allocAmount float64, index int) {
	if destinationBalance == nil || !destinationBalance.TrackFundLineage || destinationBalance.IdentityID == "" {
		return
	}

	destShadowBalance, destAggBalance, err := l.getOrCreateDestinationLineageBalances(ctx, mapping.Provider, destinationBalance)
	if err != nil {
		logrus.Errorf("failed to create destination lineage balances: %v", err)
		return
	}

	destMapping := model.LineageMapping{
		BalanceID:          destinationBalance.BalanceID,
		Provider:           mapping.Provider,
		ShadowBalanceID:    destShadowBalance.BalanceID,
		AggregateBalanceID: destAggBalance.BalanceID,
		IdentityID:         destinationBalance.IdentityID,
	}
	_ = l.datasource.UpsertLineageMapping(ctx, destMapping)

	if err := l.queueReceiveTransaction(ctx, txn, alloc, mapping, sourceBalance, destinationBalance, destShadowBalance, destAggBalance, allocAmount, index); err != nil {
		logrus.Errorf("failed to queue receive transaction: %v", err)
	}
}

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

func (l *Blnk) sequentialAllocation(sources []LineageSource, amount *big.Int) []Allocation {
	var allocations []Allocation
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

		allocations = append(allocations, Allocation{
			BalanceID: source.BalanceID,
			Amount:    alloc,
		})

		remaining.Sub(remaining, alloc)
	}

	return allocations
}

func (l *Blnk) proportionalAllocation(sources []LineageSource, amount *big.Int) []Allocation {
	var allocations []Allocation

	total := big.NewInt(0)
	for _, source := range sources {
		total.Add(total, source.Balance)
	}

	if total.Cmp(big.NewInt(0)) == 0 {
		return nil
	}

	remaining := new(big.Int).Set(amount)

	for i, source := range sources {
		var alloc *big.Int

		if i == len(sources)-1 {
			alloc = new(big.Int).Set(remaining)
		} else {
			proportion := new(big.Int).Mul(amount, source.Balance)
			alloc = new(big.Int).Div(proportion, total)
		}

		if alloc.Cmp(source.Balance) > 0 {
			alloc.Set(source.Balance)
		}

		allocations = append(allocations, Allocation{
			BalanceID: source.BalanceID,
			Amount:    alloc,
		})

		remaining.Sub(remaining, alloc)
	}

	return allocations
}

func (l *Blnk) findMappingByShadowID(mappings []model.LineageMapping, shadowBalanceID string) *model.LineageMapping {
	for _, mapping := range mappings {
		if mapping.ShadowBalanceID == shadowBalanceID {
			return &mapping
		}
	}
	return nil
}

type ProviderBreakdown struct {
	Provider  string   `json:"provider"`
	Amount    *big.Int `json:"amount"`
	Available *big.Int `json:"available"`
	Spent     *big.Int `json:"spent"`
	BalanceID string   `json:"shadow_balance_id"`
}

type BalanceLineage struct {
	BalanceID          string              `json:"balance_id"`
	TotalWithLineage   *big.Int            `json:"total_with_lineage"`
	AggregateBalanceID string              `json:"aggregate_balance_id"`
	Providers          []ProviderBreakdown `json:"providers"`
}

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

type TransactionLineage struct {
	TransactionID      string                   `json:"transaction_id"`
	FundAllocation     []map[string]interface{} `json:"fund_allocation,omitempty"`
	ShadowTransactions []model.Transaction      `json:"shadow_transactions"`
}

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
