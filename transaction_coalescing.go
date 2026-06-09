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
	"sort"

	blnkhooks "github.com/blnkfinance/blnk/internal/hooks"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) TryRecordQueuedTransactionBatch(ctx context.Context, transaction *model.Transaction) (handled bool, err error) {
	return l.tryRecordQueuedTransactionBatch(ctx, transaction, false)
}

func (l *Blnk) TryRecordQueuedTransactionBatchForHotLane(ctx context.Context, transaction *model.Transaction) (handled bool, err error) {
	return l.tryRecordQueuedTransactionBatch(ctx, transaction, true)
}

// tryRecordQueuedTransactionBatch builds and persists a coalesced queued batch when it is
// safe and useful, otherwise it returns handled=false so callers can fail open.
func (l *Blnk) tryRecordQueuedTransactionBatch(ctx context.Context, transaction *model.Transaction, force bool) (handled bool, err error) {
	ctx, span := tracer.Start(ctx, "TryRecordQueuedTransactionBatch")
	defer span.End()

	if eligible, reason := l.canCoalesceQueuedTransaction(transaction); !eligible {
		logrus.WithFields(logrus.Fields{
			"transaction_id": transaction.TransactionID,
			"parent":         transaction.ParentTransaction,
			"source":         transaction.Source,
			"destination":    transaction.Destination,
			"currency":       transaction.Currency,
			"reason":         reason,
		}).Info("Skipping queued transaction coalescing")
		return false, nil
	}

	batchSize := l.queuedCoalescingBatchSize(force)
	if batchSize < 2 {
		return false, nil
	}

	batch, scope, err := l.buildQueuedCoalescingBatch(ctx, transaction, batchSize)
	if err != nil {
		span.RecordError(err)
		logrus.WithError(err).WithFields(logrus.Fields{
			"transaction_id": transaction.TransactionID,
			"parent":         transaction.ParentTransaction,
			"source":         transaction.Source,
			"destination":    transaction.Destination,
			"currency":       transaction.Currency,
			"scope":          scope,
			"reason":         "queued_sibling_lookup_failed",
		}).Warn("Skipping transaction coalescing")
		return false, nil
	}

	if len(batch) < 2 {
		metrics.TransactionBatchTotal.Add(ctx, 1,
			otelmetric.WithAttributes(attribute.String("result", "skipped")),
		)
		return false, nil
	}

	if err := l.persistQueuedTransactionBatch(ctx, batch); err != nil {
		span.RecordError(err)
		logrus.WithError(err).WithFields(logrus.Fields{
			"transaction_id": transaction.TransactionID,
			"parent":         transaction.ParentTransaction,
			"source":         transaction.Source,
			"destination":    transaction.Destination,
			"currency":       transaction.Currency,
			"scope":          scope,
			"batch_size":     len(batch),
			"reason":         "batch_processing_failed_open",
		}).Warn("transaction coalescing failed open; switching leader to per-transaction processing")
		metrics.TransactionBatchTotal.Add(ctx, 1,
			otelmetric.WithAttributes(attribute.String("result", "failure")),
		)
		return false, nil
	}

	logrus.WithFields(logrus.Fields{
		"scope":      scope,
		"batch_size": len(batch),
	}).Info("Transactions processed successfully")

	span.AddEvent("Queued transaction batch processed", trace.WithAttributes(
		attribute.Int("batch.size", len(batch)),
		attribute.String("batch.scope", string(scope)),
		attribute.String("source.balance_id", transaction.Source),
		attribute.String("destination.balance_id", transaction.Destination),
	))

	// Record batch metrics on success.
	metrics.TransactionBatchSize.Record(ctx, int64(len(batch)))
	metrics.TransactionBatchTotal.Add(ctx, 1,
		otelmetric.WithAttributes(attribute.String("result", "success")),
	)

	return true, nil
}

// persistQueuedTransactionBatch acquires the balance-set lock, prepares the batch in memory,
// commits the final state atomically, and dispatches post-commit side effects.
func (l *Blnk) persistQueuedTransactionBatch(ctx context.Context, transactions []*model.Transaction) error {
	ctx, span := tracer.Start(ctx, "RecordQueuedTransactionBatch")
	defer span.End()

	if err := validateQueuedBatchCurrencies(transactions); err != nil {
		return err
	}

	if len(transactions) == 0 {
		return nil
	}

	currency := transactions[0].Currency

	balanceIDs := collectQueuedCoalescingBalanceIDs(transactions)
	locker, err := l.acquireBalanceSetLock(ctx, balanceIDs)
	if err != nil {
		hotpairs.RecordContention(ctx, l.hotPairs, transactions[0].Source, transactions[0].Destination, currency, err)
		return fmt.Errorf("failed to acquire batch lock: %w", err)
	}

	result, err := l.persistQueuedTransactionBatchLocked(ctx, span, locker, balanceIDs, transactions)
	if err != nil {
		return err
	}

	l.runQueuedBatchPostCommitWork(ctx, span, result)
	return nil
}

// validateQueuedBatchCurrencies ensures that every transaction in a coalesced batch uses the
// same currency before any lock or balance work begins.
func validateQueuedBatchCurrencies(transactions []*model.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	currency := transactions[0].Currency
	for _, txn := range transactions[1:] {
		if txn.Currency != currency {
			return fmt.Errorf("coalescing batch contains multiple currencies")
		}
	}

	return nil
}

// persistQueuedTransactionBatchLocked prepares every transaction against the shared in-memory
// balance set and persists the final balance and transaction state atomically.
func (l *Blnk) persistQueuedTransactionBatchLocked(ctx context.Context, span trace.Span, locker *redlock.MultiLocker, balanceIDs []string, transactions []*model.Transaction) (queuedBatchPersistResult, error) {
	defer l.releaseLock(ctx, locker)

	balancesByID, orderedBalances, err := l.loadBalancesForQueuedBatch(ctx, balanceIDs)
	if err != nil {
		return queuedBatchPersistResult{}, fmt.Errorf("failed to load balances for coalesced batch: %w", err)
	}
	preHooks, err := l.listHooksForExecution(ctx, blnkhooks.PreTransaction)
	if err != nil {
		return queuedBatchPersistResult{}, fmt.Errorf("failed to list pre-transaction hooks for coalesced batch: %w", err)
	}

	finalizedTransactions := make([]*model.Transaction, 0, len(transactions))
	outboxes := make([]*model.LineageOutbox, 0, len(transactions))
	postCommitWork := make([]queuedBatchPostCommitWork, 0, len(transactions))
	prefetchedReferences, existingReferences, err := l.queuedBatchReferenceSets(ctx, transactions)
	if err != nil {
		return queuedBatchPersistResult{}, err
	}
	batchReferences := make(map[string]struct{}, len(transactions))

	for _, txn := range transactions {
		work, err := l.prepareQueuedBatchTransaction(ctx, span, txn, balancesByID, preHooks, prefetchedReferences, existingReferences, batchReferences)
		if err != nil {
			return queuedBatchPersistResult{}, err
		}

		finalizedTransactions = append(finalizedTransactions, work.transaction)
		postCommitWork = append(postCommitWork, work)
		if work.outbox != nil {
			outboxes = append(outboxes, work.outbox)
		}
	}

	if _, err := l.datasource.RecordTransactionsWithBalanceSetAndOutboxes(ctx, finalizedTransactions, orderedBalances, outboxes); err != nil {
		return queuedBatchPersistResult{}, l.logAndRecordError(span, "failed to persist coalesced transaction batch", err)
	}

	return queuedBatchPersistResult{
		orderedBalances: orderedBalances,
		postCommitWork:  postCommitWork,
	}, nil
}

// queuedBatchReferenceSets prepares the prefetched and existing reference sets used by batch
// validation when batch reference checking is enabled.
func (l *Blnk) queuedBatchReferenceSets(ctx context.Context, transactions []*model.Transaction) (map[string]struct{}, map[string]struct{}, error) {
	prefetchedReferences := make(map[string]struct{})
	existingReferences := make(map[string]struct{})
	if !l.batchReferenceCheckEnabled() {
		return prefetchedReferences, existingReferences, nil
	}

	prefetchedReferences, existingReferences, err := l.getQueuedBatchExistingReferences(ctx, transactions)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to validate coalesced transaction references: %w", err)
	}

	return prefetchedReferences, existingReferences, nil
}

// prepareQueuedBatchTransaction performs the per-transaction work inside a coalesced batch:
// hooks, reference validation, in-memory balance application, and persistence shaping.
func (l *Blnk) prepareQueuedBatchTransaction(ctx context.Context, span trace.Span, txn *model.Transaction, balancesByID map[string]*model.Balance, preHooks []*blnkhooks.Hook, prefetchedReferences, existingReferences, batchReferences map[string]struct{}) (queuedBatchPostCommitWork, error) {
	sourceBalance, destinationBalance, err := coalescingBalancesForTransaction(balancesByID, txn)
	if err != nil {
		return queuedBatchPostCommitWork{}, err
	}

	if l.Hooks != nil {
		if err := l.Hooks.ExecuteHooks(ctx, preHooks, blnkhooks.PreTransaction, txn.TransactionID, txn); err != nil {
			span.RecordError(err)
			return queuedBatchPostCommitWork{}, fmt.Errorf("batch pre-transaction hook failed: %w", err)
		}
	}

	if err := l.validateQueuedBatchTransactionReference(ctx, txn, prefetchedReferences, existingReferences, batchReferences); err != nil {
		return queuedBatchPostCommitWork{}, fmt.Errorf("batch transaction validation failed: %w", err)
	}

	finalizedTxn := l.updateTransactionDetails(ctx, txn, sourceBalance, destinationBalance)
	if err := l.processBalances(ctx, finalizedTxn, sourceBalance, destinationBalance); err != nil {
		return queuedBatchPostCommitWork{}, err
	}

	work, skipPersist := l.buildTransactionExecutionWork(ctx, finalizedTxn, sourceBalance, destinationBalance)
	if skipPersist {
		return queuedBatchPostCommitWork{}, fmt.Errorf("batch coalescing does not support zero-amount transactions")
	}

	return work, nil
}

// runQueuedBatchPostCommitWork dispatches the post-commit work for a coalesced batch after
// the balance-set lock has been released.
func (l *Blnk) runQueuedBatchPostCommitWork(ctx context.Context, span trace.Span, result queuedBatchPersistResult) {
	postHooks, err := l.listHooksForExecution(ctx, blnkhooks.PostTransaction)
	if err != nil {
		logrus.WithError(err).Warn("failed to list post-transaction hooks for coalesced batch; falling back to per-transaction lookup")
		l.runTransactionPostCommitWork(ctx, span, result.orderedBalances, result.postCommitWork)
		return
	}

	l.runTransactionPostCommitWorkWithHooks(ctx, span, result.orderedBalances, result.postCommitWork, postHooks)
}

// runTransactionPostCommitWork executes monitor checks, post-hooks, and post-transaction
// actions for already-persisted work items outside the locked persistence path.
func (l *Blnk) queuedCoalescingBatchSize(force bool) int {
	if !force && !l.Config().Transaction.EnableCoalescing {
		return 0
	}

	size := l.Config().Transaction.BatchSize
	switch {
	case size <= 1:
		return size
	case size > maxQueuedCoalescingBatchSize:
		return maxQueuedCoalescingBatchSize
	default:
		return size
	}
}

func (l *Blnk) buildQueuedCoalescingBatch(ctx context.Context, transaction *model.Transaction, batchSize int) ([]*model.Transaction, queuedCoalescingScope, error) {
	for _, scope := range []queuedCoalescingScope{
		queuedCoalescingScopePair,
		queuedCoalescingScopeSource,
		queuedCoalescingScopeDestination,
	} {
		batch, err := l.buildQueuedCoalescingBatchForScope(ctx, transaction, scope, batchSize)
		if err != nil {
			return nil, scope, err
		}
		if len(batch) >= 2 {
			return batch, scope, nil
		}
	}

	return nil, "", nil
}

func (l *Blnk) buildQueuedCoalescingBatchForScope(ctx context.Context, leader *model.Transaction, scope queuedCoalescingScope, batchSize int) ([]*model.Transaction, error) {
	siblings, err := l.loadQueuedTransactionsForCoalescingScope(ctx, leader, scope, batchSize-1)
	if err != nil {
		return nil, err
	}

	batch := []*model.Transaction{leader}
	for _, sibling := range siblings {
		restoreTransactionFlagsFromMetadata(sibling)
		if eligible, reason := l.canCoalesceQueuedOriginal(sibling); !eligible {
			logrus.WithFields(logrus.Fields{
				"transaction_id": sibling.TransactionID,
				"parent":         sibling.ParentTransaction,
				"source":         sibling.Source,
				"destination":    sibling.Destination,
				"currency":       sibling.Currency,
				"scope":          scope,
				"reason":         reason,
			}).Info("Skipping sibling transaction during coalescing")
			continue
		}
		batch = append(batch, createQueueCopy(sibling, sibling.Reference))
	}

	return batch, nil
}

func (l *Blnk) loadQueuedTransactionsForCoalescingScope(ctx context.Context, leader *model.Transaction, scope queuedCoalescingScope, limit int) ([]*model.Transaction, error) {
	switch scope {
	case queuedCoalescingScopePair:
		return l.datasource.GetQueuedTransactionsForCoalescing(
			ctx,
			leader.Source,
			leader.Destination,
			leader.Currency,
			leader.ParentTransaction,
			leader.CreatedAt,
			limit,
		)
	case queuedCoalescingScopeSource:
		return l.datasource.GetQueuedTransactionsForSourceCoalescing(
			ctx,
			leader.Source,
			leader.Currency,
			leader.ParentTransaction,
			leader.CreatedAt,
			limit,
		)
	case queuedCoalescingScopeDestination:
		return l.datasource.GetQueuedTransactionsForDestinationCoalescing(
			ctx,
			leader.Destination,
			leader.Currency,
			leader.ParentTransaction,
			leader.CreatedAt,
			limit,
		)
	default:
		return nil, fmt.Errorf("unsupported queued coalescing scope %q", scope)
	}
}

func collectQueuedCoalescingBalanceIDs(transactions []*model.Transaction) []string {
	seen := make(map[string]struct{}, len(transactions)*2)
	ids := make([]string, 0, len(transactions)*2)
	for _, txn := range transactions {
		if txn == nil {
			continue
		}
		for _, id := range []string{txn.Source, txn.Destination} {
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (l *Blnk) acquireBalanceSetLock(ctx context.Context, balanceIDs []string) (*redlock.MultiLocker, error) {
	ctx, span := tracer.Start(ctx, "Acquiring Balance Set Lock")
	defer span.End()

	locker := redlock.NewMultiLocker(l.redis, balanceIDs, model.GenerateUUIDWithSuffix("loc"))
	if err := l.acquireTransactionLocker(ctx, locker); err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Locks acquired", trace.WithAttributes(
		attribute.StringSlice("locked.keys", locker.Keys()),
	))
	return locker, nil
}

func (l *Blnk) acquireTransactionLocker(ctx context.Context, locker *redlock.MultiLocker) error {
	lockCfg := l.Config().Transaction
	if lockCfg.LockWaitTimeout > 0 {
		return locker.WaitLock(ctx, lockCfg.LockDuration, lockCfg.LockWaitTimeout)
	}
	return locker.Lock(ctx, lockCfg.LockDuration)
}

func (l *Blnk) loadBalancesForQueuedBatch(ctx context.Context, balanceIDs []string) (map[string]*model.Balance, []*model.Balance, error) {
	balancesByID := make(map[string]*model.Balance, len(balanceIDs))

	if l.Config().Transaction.EnableQueuedChecks {
		for _, balanceID := range balanceIDs {
			balance, err := l.datasource.GetBalanceByID(balanceID, nil, true)
			if err != nil {
				return nil, nil, err
			}
			balancesByID[balanceID] = balance
		}
	} else {
		loaded, err := l.datasource.GetBalancesByIDsLite(ctx, balanceIDs)
		if err != nil {
			return nil, nil, err
		}
		for balanceID, balance := range loaded {
			balancesByID[balanceID] = balance
		}
	}

	orderedBalances := make([]*model.Balance, 0, len(balanceIDs))
	for _, balanceID := range balanceIDs {
		balance, ok := balancesByID[balanceID]
		if !ok || balance == nil {
			return nil, nil, fmt.Errorf("failed to load balance %s for coalesced batch", balanceID)
		}
		orderedBalances = append(orderedBalances, balance)
	}

	return balancesByID, orderedBalances, nil
}

func coalescingBalancesForTransaction(balancesByID map[string]*model.Balance, txn *model.Transaction) (*model.Balance, *model.Balance, error) {
	sourceBalance, ok := balancesByID[txn.Source]
	if !ok || sourceBalance == nil {
		return nil, nil, fmt.Errorf("missing source balance %s for coalesced transaction", txn.Source)
	}

	destinationBalance, ok := balancesByID[txn.Destination]
	if !ok || destinationBalance == nil {
		return nil, nil, fmt.Errorf("missing destination balance %s for coalesced transaction", txn.Destination)
	}

	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) getQueuedBatchExistingReferences(ctx context.Context, transactions []*model.Transaction) (map[string]struct{}, map[string]struct{}, error) {
	references := make([]string, 0, len(transactions))
	prefetched := make(map[string]struct{}, len(transactions))
	for _, txn := range transactions {
		if txn == nil || txn.Reference == "" {
			continue
		}
		if _, ok := prefetched[txn.Reference]; ok {
			continue
		}
		prefetched[txn.Reference] = struct{}{}
		references = append(references, txn.Reference)
	}

	existing, err := l.datasource.GetExistingTransactionReferences(ctx, references)
	if err != nil {
		return nil, nil, err
	}
	return prefetched, existing, nil
}

func (l *Blnk) validateQueuedBatchTransactionReference(ctx context.Context, transaction *model.Transaction, prefetchedReferences, existingReferences, batchReferences map[string]struct{}) error {
	if transaction == nil {
		return fmt.Errorf("nil transaction")
	}

	if transaction.Reference == "" {
		return fmt.Errorf("reference is required")
	}

	if _, ok := batchReferences[transaction.Reference]; ok {
		err := fmt.Errorf("reference %s has already been used", transaction.Reference)
		notification.NotifyError(err)
		return err
	}

	if _, ok := existingReferences[transaction.Reference]; ok {
		err := fmt.Errorf("reference %s has already been used", transaction.Reference)
		notification.NotifyError(err)
		return err
	}

	if len(prefetchedReferences) == 0 {
		if err := l.validateTxn(ctx, transaction); err != nil {
			return err
		}
		batchReferences[transaction.Reference] = struct{}{}
		return nil
	}

	// Fall back to the single-reference validation path only if the transaction's
	// current reference was not part of the prefetched batch reference set.
	if _, ok := prefetchedReferences[transaction.Reference]; !ok {
		if err := l.validateTxn(ctx, transaction); err != nil {
			return err
		}
	}

	batchReferences[transaction.Reference] = struct{}{}
	return nil
}

func (l *Blnk) batchReferenceCheckEnabled() bool {
	return !l.Config().Transaction.DisableBatchReferenceCheck
}

func (l *Blnk) canCoalesceQueuedTransaction(transaction *model.Transaction) (bool, string) {
	if transaction == nil {
		return false, "nil_transaction"
	}
	if transaction.ParentTransaction == "" {
		return false, "missing_parent_transaction"
	}
	if transaction.Status != StatusQueued {
		return false, "status_not_queued"
	}
	if transaction.Atomic || transaction.SkipQueue {
		switch {
		case transaction.Atomic:
			return false, "atomic_transaction"
		default:
			return false, "skip_queue_enabled"
		}
	}
	if len(transaction.Sources) > 0 || len(transaction.Destinations) > 0 {
		return false, "split_transaction"
	}
	if !transaction.ScheduledFor.IsZero() {
		return false, "scheduled_transaction"
	}
	if transaction.Source == "" || transaction.Destination == "" || transaction.Currency == "" {
		return false, "missing_pair_or_currency"
	}
	return true, ""
}

func (l *Blnk) canCoalesceQueuedOriginal(transaction *model.Transaction) (bool, string) {
	if transaction == nil {
		return false, "nil_transaction"
	}
	if transaction.Status != StatusQueued {
		return false, "status_not_queued"
	}
	if transaction.Atomic || transaction.SkipQueue {
		switch {
		case transaction.Atomic:
			return false, "atomic_transaction"
		default:
			return false, "skip_queue_enabled"
		}
	}
	if len(transaction.Sources) > 0 || len(transaction.Destinations) > 0 {
		return false, "split_transaction"
	}
	if !transaction.ScheduledFor.IsZero() {
		return false, "scheduled_transaction"
	}
	if transaction.Source == "" || transaction.Destination == "" || transaction.Currency == "" {
		return false, "missing_pair_or_currency"
	}
	return true, ""
}

func restoreTransactionFlagsFromMetadata(transaction *model.Transaction) {
	if transaction == nil || transaction.MetaData == nil {
		return
	}

	if v, ok := transaction.MetaData["inflight"].(bool); ok {
		transaction.Inflight = v
	}
	if v, ok := transaction.MetaData["atomic"].(bool); ok {
		transaction.Atomic = v
	}
	if v, ok := transaction.MetaData["allow_overdraft"].(bool); ok {
		transaction.AllowOverdraft = v
	}
}
