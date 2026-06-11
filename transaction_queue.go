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
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) prepareTransactionForQueue(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "PrepareTransactionForQueue")
	defer span.End()

	setTransactionStatus(transaction)
	setTransactionMetadata(transaction)

	// Get and update source/destination
	sourceBalance, destinationBalance, err := l.getSourceAndDestination(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get source/destination balances: %w", err)
	}

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID

	if err := hotpairs.AssignQueueLane(ctx, l.hotPairs, l.datasource, transaction); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to assign queue lane: %w", err)
	}

	return transaction, nil
}

// QueueTransaction processes and queues a transaction for execution.
// It handles both single transactions and split transactions, preparing them for processing
// by setting metadata, status, and managing their persistence and queueing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be queued.
//
// Returns:
// - *model.Transaction: A pointer to the queued Transaction model.
// - error: An error if the transaction could not be queued.
func (l *Blnk) QueueTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "QueueTransaction")
	defer span.End()

	// Initialize transaction metadata and status
	originalRef := transaction.Reference
	setTransactionMetadata(transaction)
	setTransactionStatus(transaction)
	originalTxnID := transaction.TransactionID

	// Handle split transactions if needed
	transactions, err := l.handleSplitTransactions(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// If SkipQueue is true, process synchronously
	if transaction.SkipQueue {
		_, err := l.processTxns(ctx, transaction, transactions, originalTxnID, originalRef)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Update transaction status based on inflight flag
		if transaction.Inflight {
			transaction.Status = StatusInflight
		} else {
			transaction.Status = StatusApplied
		}
	} else {
		if transaction.MetaData != nil && !transaction.SkipQueue {
			transaction.MetaData["QUEUED_PARENT_TRANSACTION"] = originalTxnID
		}

		if strings.Contains(transaction.ParentTransaction, "bulk") {
			transaction.MetaData["QUEUED_PARENT_TRANSACTION"] = transaction.ParentTransaction
		}
		// For normal queue mode, process asynchronously
		processTransactionAsync(context.Background(), l, transaction, originalRef, originalTxnID, transactions)
	}

	span.AddEvent("Transaction successfully queued", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
	))

	if !transaction.InflightExpiryDate.IsZero() {
		err = l.queue.QueueInflightExpiry(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, l.logAndRecordError(span, "failed to queue inflight expiry", err)
		}
	}

	// Queue an automatic commit task if inflight_commit_date is set.
	if !transaction.InflightCommitDate.IsZero() {
		err = l.queue.QueueInflightCommit(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, l.logAndRecordError(span, "failed to queue inflight commit", err)
		}
	}

	return transaction, nil
}

func processTransactionAsync(ctx context.Context, l *Blnk, transaction *model.Transaction, originalRef string, originalTxnID string, transactions []*model.Transaction) {
	// The background worker mutates the transaction (metadata, timestamps,
	// precise amount) while the caller still holds the pointer returned from
	// QueueTransaction. Snapshot it so the worker never races a caller that
	// inspects the returned transaction.
	asyncTxn := cloneTransactionForAsync(transaction)
	go func() {
		if err := asyncTxnSemaphore.Acquire(ctx, 1); err != nil {
			logrus.WithError(err).Error("failed to acquire async txn semaphore")
			return
		}
		defer asyncTxnSemaphore.Release(1)

		ctx, span := tracer.Start(ctx, "ProcessTransactionAsync")
		defer span.End()

		queueTransactions, err := l.processTxns(ctx, asyncTxn, transactions, originalTxnID, originalRef)
		if err != nil {
			span.RecordError(err)
		}

		if !asyncTxn.SkipQueue {
			if err := enqueueTransactions(ctx, l.queue, asyncTxn, queueTransactions); err != nil {
				span.RecordError(err)
			}
		}
	}()
}

// cloneTransactionForAsync returns a copy of the transaction safe to hand to the
// async worker: the MetaData map and EffectiveDate pointer are duplicated so the
// worker's mutations never touch the object the caller still holds.
func cloneTransactionForAsync(t *model.Transaction) *model.Transaction {
	clone := *t
	if t.MetaData != nil {
		md := make(map[string]interface{}, len(t.MetaData))
		for k, v := range t.MetaData {
			md[k] = v
		}
		clone.MetaData = md
	}
	if t.EffectiveDate != nil {
		ed := *t.EffectiveDate
		clone.EffectiveDate = &ed
	}
	return &clone
}

// cloneTransactionsForAsync clones each transaction so a background worker can
// mutate them without touching the slice the caller still holds.
func cloneTransactionsForAsync(txns []*model.Transaction) []*model.Transaction {
	clones := make([]*model.Transaction, len(txns))
	for i, t := range txns {
		clones[i] = cloneTransactionForAsync(t)
	}
	return clones
}

// handleSplitTransactions attempts to split a transaction into multiple transactions if needed.
// It starts a tracing span, attempts to split the transaction, and validates the result.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be potentially split.
//
// Returns:
// - []*model.Transaction: A slice of split transactions, or empty if no split is needed.
// - error: An error if the transaction splitting fails.
func (l *Blnk) handleSplitTransactions(ctx context.Context, transaction *model.Transaction) ([]*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "HandleSplitTransactions")
	defer span.End()

	transactions, err := transaction.SplitTransactionPrecise(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to split transaction: %w", err)
	}
	return transactions, nil
}

// processTxns handles the processing of transactions based on whether they are split or single.
// It delegates to the appropriate processing function based on the presence of split transactions.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - originalTxn *model.Transaction: The original transaction before processing.
// - splitTxns []*model.Transaction: Any split transactions derived from the original.
// - originalTxnID string: The ID of the original transaction.
// - originalRef string: The reference of the original transaction.
//
// Returns:
// - []*model.Transaction: A slice of processed transactions ready for queueing.
// - error: An error if the processing fails.
func (l *Blnk) processTxns(ctx context.Context, originalTxn *model.Transaction, splitTxns []*model.Transaction, originalTxnID, originalRef string) ([]*model.Transaction, error) {
	if originalTxn.SkipQueue {
		if len(splitTxns) == 0 {
			recorded, err := l.RecordTransaction(ctx, originalTxn)
			if err != nil {
				return nil, err
			}
			return []*model.Transaction{recorded}, nil
		} else {
			result := make([]*model.Transaction, len(splitTxns))
			for i, txn := range splitTxns {
				recorded, err := l.RecordTransaction(ctx, txn)
				if err != nil {
					if txn.Atomic {
						l.handleAsyncBulkTransactionFailure(ctx, err, originalTxnID, true, txn.Inflight)
					}
					return nil, fmt.Errorf("failed to record split transaction %d: %w", i, err)
				}
				result[i] = recorded
			}
			return result, nil
		}
	}
	if len(splitTxns) == 0 {
		return l.processSingleTransaction(ctx, originalTxn, originalRef)
	}
	return l.processSplitTransactions(ctx, splitTxns, originalTxnID, originalRef)
}

// processSingleTransaction handles the processing of a single (non-split) transaction.
// It prepares the transaction, persists it to the database, and creates a queue copy.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The single transaction to process.
// - originalRef string: The original reference of the transaction.
//
// Returns:
// - []*model.Transaction: A slice containing the processed transaction ready for queueing.
// - error: An error if the transaction processing fails.
func (l *Blnk) processSingleTransaction(ctx context.Context, transaction *model.Transaction, originalRef string) ([]*model.Transaction, error) {
	if len(transaction.Sources) == 0 && len(transaction.Destinations) == 0 {
		preparedTxn, err := l.prepareTransactionForQueue(ctx, transaction)
		if err != nil {
			return nil, err
		}
		transaction = preparedTxn
	}

	persistedTxn, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to persist original transaction: %w", err)
	}

	queueTxn := createQueueCopy(persistedTxn, originalRef)
	return []*model.Transaction{queueTxn}, nil
}

// processSplitTransactions handles the processing of multiple split transactions.
// It prepares each split transaction, persists them to the database, and creates queue copies.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transactions []*model.Transaction: The split transactions to process.
// - originalTxnID string: The ID of the original transaction.
// - originalRef string: The reference of the original transaction.
//
// Returns:
// - []*model.Transaction: A slice of processed transactions ready for queueing.
// - error: An error if any transaction processing fails.
func (l *Blnk) processSplitTransactions(ctx context.Context, transactions []*model.Transaction, originalTxnID, originalRef string) ([]*model.Transaction, error) {
	queueTransactions := make([]*model.Transaction, len(transactions))
	updateSplitTransactions(transactions, originalTxnID, originalRef)

	for i, splitTxn := range transactions {
		preparedSplitTxn, err := l.prepareTransactionForQueue(ctx, splitTxn)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare split transaction %d: %w", i, err)
		}

		// Persist the original transaction
		persistedTxn, err := l.datasource.RecordTransaction(ctx, preparedSplitTxn)
		if err != nil {
			return nil, fmt.Errorf("failed to persist original transaction: %w", err)
		}

		queueTxn := createQueueCopy(persistedTxn, splitTxn.Reference)
		queueTransactions[i] = queueTxn
	}

	return queueTransactions, nil
}

// setTransactionStatus determines and sets the appropriate status for a transaction.
// It evaluates the transaction's scheduled time and current status to set the correct status.
// If the transaction is scheduled for the future, it sets StatusScheduled.
// If the transaction has no status, it sets StatusQueued.
//
// Parameters:
// - transaction *model.Transaction: The transaction for which to set the status.
func setTransactionStatus(transaction *model.Transaction) {
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	} else if transaction.Status == "" {
		transaction.Status = StatusQueued
	}
}

// setTransactionMetadata initializes and sets the required metadata for a transaction.
// calculates the transaction hash, and sets the precise amount based on the transaction's precision.
//
// Parameters:
// - transaction *model.Transaction: The transaction for which to set metadata.
func setTransactionMetadata(transaction *model.Transaction) {
	transaction.CreatedAt = time.Now()
	if transaction.EffectiveDate == nil {
		transaction.EffectiveDate = &transaction.CreatedAt
	}
	transaction.Hash = transaction.HashTxn()
	transaction.PreciseAmount = model.ApplyPrecision(transaction)
	if transaction.TransactionID == "" {
		transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	}

	// Initialize metadata if it doesn't exist
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}

	// Set inflight flag in metadata if the transaction is inflight
	//maybe move to their respective columns in the db later
	if transaction.Inflight {
		transaction.MetaData["inflight"] = true
	}
	if transaction.Atomic {
		transaction.MetaData["atomic"] = true
	}
	if transaction.AllowOverdraft {
		transaction.MetaData["allow_overdraft"] = true
	}
}

// createQueueCopy creates a new copy of a transaction specifically for queueing.
// It generates new identifiers and maintains the relationship with the original transaction.
//
// Parameters:
// - persistedTxn *model.Transaction: The persisted transaction to copy.
// - originalRef string: The original reference to base the new reference on.
//
// Returns:
// - *model.Transaction: A pointer to the new queue copy of the transaction.
func createQueueCopy(persistedTxn *model.Transaction, originalRef string) *model.Transaction {
	queueTxn := *persistedTxn
	queueTxn.TransactionID = model.GenerateUUIDWithSuffix("txn")
	queueTxn.ParentTransaction = persistedTxn.TransactionID
	queueTxn.Reference = fmt.Sprintf("%s_q", originalRef)
	return &queueTxn
}

// updateSplitTransactions updates the metadata of split transactions to maintain their relationships.
// It sets the parent transaction ID and creates linked references for each split transaction.
//
// Parameters:
// - transactions []*model.Transaction: The split transactions to update.
// - parentID string: The ID of the parent transaction.
// - originalRef string: The original reference to base the new references on.
func updateSplitTransactions(transactions []*model.Transaction, parentID, originalRef string) {
	for i, txn := range transactions {
		txn.ParentTransaction = parentID
		txn.Reference = fmt.Sprintf("%s_%d", originalRef, i+1)
	}
}

// enqueueTransactions enqueues the original transaction or its split transactions into the provided queue.
// It starts by determining which transactions to enqueue, then iterates through them and enqueues each one.
// If an error occurs during enqueuing, it logs the error and sends a notification.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - queue *Queue: The queue to which the transactions will be enqueued.
// - originalTransaction *model.Transaction: The original transaction to be enqueued if no split transactions are provided.
// - splitTransactions []*model.Transaction: A slice of split transactions to be enqueued.
//
// Returns:
// - error: An error if any of the transactions could not be enqueued.
func enqueueTransactions(ctx context.Context, queue *Queue, originalTransaction *model.Transaction, splitTransactions []*model.Transaction) error {
	transactionsToEnqueue := splitTransactions
	if len(transactionsToEnqueue) == 0 {
		transactionsToEnqueue = []*model.Transaction{originalTransaction}
	}

	for _, txn := range transactionsToEnqueue {
		if err := queue.Enqueue(ctx, txn); err != nil {
			notification.NotifyError(err)
			logrus.WithError(err).Error("failed to queue transaction")
			return err
		}
	}

	return nil
}
