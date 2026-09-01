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
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) RefundWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int) {
	ctx, span := tracer.Start(ctx, "RefundWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		queuedRefundTxn, err := l.RefundTransaction(ctx, originalTxn.TransactionID, originalTxn.SkipQueue)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
			continue
		}
		results <- BatchJobResult{Txn: queuedRefundTxn}
		span.AddEvent("Refund processed", trace.WithAttributes(attribute.String("transaction.id", queuedRefundTxn.TransactionID)))
	}
}

// RefundWorkerWithOptions returns a transactionWorker that refunds each
// job with an explicit skipQueue decision, rather than inheriting it
// from the original transaction.
//
// This exists because skip_queue is a request-time routing flag and is
// deliberately NOT persisted (see Transaction Lifecycle docs). The
// original transaction fetched from storage for a refund therefore
// always deserializes SkipQueue=false, so the plain RefundWorker can
// never produce a synchronous refund. RefundWorkerWithOptions lets the
// caller — e.g. the /refund-transaction API handler honoring a
// "skip_queue" field in the request body — choose synchronous
// processing for time-sensitive refunds.
//
// RefundWorker is left unchanged; this is purely additive.
// RefundWorkerWithRefundOptions returns a transactionWorker that refunds each
// job using the supplied options, so a caller can set the reversal's
// description and metadata as well as its queueing.
//
// RefundWorkerWithOptions is the same worker with only SkipQueue set.
func (l *Blnk) RefundWorkerWithRefundOptions(opts RefundOptions) transactionWorker {
	return func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int) {
		ctx, span := tracer.Start(ctx, "RefundWorkerWithRefundOptions")
		defer span.End()

		defer wg.Done()
		for originalTxn := range jobs {
			queuedRefundTxn, err := l.RefundTransactionWithOptions(ctx, originalTxn.TransactionID, opts)
			if err != nil {
				results <- BatchJobResult{Error: err}
				span.RecordError(err)
				continue
			}
			results <- BatchJobResult{Txn: queuedRefundTxn}
			span.AddEvent("Refund processed", trace.WithAttributes(attribute.String("transaction.id", queuedRefundTxn.TransactionID)))
		}
	}
}

func (l *Blnk) RefundWorkerWithOptions(skipQueue bool) transactionWorker {
	return func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int) {
		ctx, span := tracer.Start(ctx, "RefundWorkerWithOptions")
		defer span.End()
		span.SetAttributes(attribute.Bool("refund.skip_queue", skipQueue))

		defer wg.Done()
		for originalTxn := range jobs {
			queuedRefundTxn, err := l.RefundTransaction(ctx, originalTxn.TransactionID, skipQueue)
			if err != nil {
				results <- BatchJobResult{Error: err}
				span.RecordError(err)
				continue
			}
			results <- BatchJobResult{Txn: queuedRefundTxn}
			span.AddEvent("Refund processed", trace.WithAttributes(attribute.String("transaction.id", queuedRefundTxn.TransactionID)))
		}
	}
}

// ProcessQueuedTransaction preserves the existing queued-worker behavior while routing the
// decision through the shared internal transaction executor.

func (l *Blnk) getOriginalTransactionForRefund(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "getOriginalTransactionForRefund")
	defer span.End()

	originalTxn, err := l.datasource.GetTransaction(ctx, transactionID)
	if err != nil {
		// Check if the error is due to no row found
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), fmt.Sprintf("Transaction with ID '%s' not found", transactionID)) {
			// Check the queue for the transaction
			queuedTxn, queueErr := l.queue.GetTransactionFromQueue(transactionID)
			if queueErr != nil {
				span.RecordError(queueErr)
				// Return the original DB error if queue retrieval also fails
				return nil, fmt.Errorf("transaction %s not found: %w", transactionID, err)
			}
			if queuedTxn == nil {
				err := fmt.Errorf("transaction %s not found", transactionID)
				span.RecordError(err)
				return nil, err
			}
			span.AddEvent("Transaction found in queue", trace.WithAttributes(attribute.String("transaction.id", transactionID)))
			return queuedTxn, nil // Return the transaction found in the queue
		}
		// Return other database errors directly
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get transaction %s from DB: %w", transactionID, err)
	}
	span.AddEvent("Transaction found in DB", trace.WithAttributes(attribute.String("transaction.id", transactionID)))
	return originalTxn, nil
}

// validateTransactionForRefund checks if the given transaction is eligible for a refund.
func (l *Blnk) validateTransactionForRefund(ctx context.Context, originalTxn *model.Transaction) error {
	ctx, span := tracer.Start(ctx, "validateTransactionForRefund")
	defer span.End()

	// Only settled (APPLIED) or voided originals are refundable. Refunding a
	// QUEUED/INFLIGHT/SCHEDULED original would reverse funds that were never
	// settled.
	switch originalTxn.Status {
	case StatusApplied, StatusVoid:
		// refundable
	default:
		err := fmt.Errorf("transaction %s cannot be refunded in status %s (only APPLIED or VOID)", originalTxn.TransactionID, originalTxn.Status)
		span.RecordError(err)
		return err
	}

	// Check if the transaction has already been refunded
	isRefunded, err := l.datasource.IsTransactionRefunded(ctx, originalTxn)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to check if transaction %s was already refunded: %w", originalTxn.TransactionID, err)
	}
	if isRefunded {
		err := fmt.Errorf("transaction %s has already been refunded", originalTxn.TransactionID)
		span.RecordError(err)
		return err
	}

	span.AddEvent("Transaction validated for refund", trace.WithAttributes(attribute.String("transaction.id", originalTxn.TransactionID)))
	return nil
}

// RefundOptions carries the caller-supplied settings for a refund.
//
// The zero value reproduces the historical behaviour: queued asynchronously,
// with the reversal inheriting the original transaction's description and
// metadata.
type RefundOptions struct {
	// SkipQueue processes the refund synchronously instead of placing it on
	// the transaction queue.
	SkipQueue bool

	// Description replaces the reversal's description, which otherwise copies
	// the original transaction's.
	Description string

	// MetaData is merged onto the metadata inherited from the original
	// transaction, with these keys winning on conflict. Merging rather than
	// replacing keeps the keys the ledger itself sets on the original — such
	// as inflight, atomic and allow_overdraft — which downstream processing
	// reads.
	MetaData map[string]interface{}
}

// prepareRefundTransaction creates and configures a new transaction object for the refund.
func prepareRefundTransaction(originalTxn *model.Transaction, opts RefundOptions) *model.Transaction {
	newTransaction := *originalTxn // Create a copy
	newTransaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	newTransaction.Reference = model.RefundReference(originalTxn.TransactionID)
	newTransaction.ParentTransaction = originalTxn.TransactionID
	newTransaction.Source = originalTxn.Destination // Swap source and destination
	newTransaction.Destination = originalTxn.Source
	newTransaction.AllowOverdraft = true
	newTransaction.SkipQueue = opts.SkipQueue

	if opts.Description != "" {
		newTransaction.Description = opts.Description
	}

	if len(opts.MetaData) > 0 {
		// Build a new map rather than writing into the inherited one: the copy
		// above shares the original transaction's metadata map, so mutating it
		// here would also rewrite the transaction being refunded.
		merged := make(map[string]interface{}, len(originalTxn.MetaData)+len(opts.MetaData))
		for k, v := range originalTxn.MetaData {
			merged[k] = v
		}
		for k, v := range opts.MetaData {
			merged[k] = v
		}
		newTransaction.MetaData = merged
	}

	// Adjust status based on original status for proper processing
	if originalTxn.Status == StatusVoid {
		// If original was voided, the refund should process like an inflight reversal
		newTransaction.Inflight = true
		newTransaction.Status = "" // Reset status to let QueueTransaction handle it
	} else {
		newTransaction.Status = "" // Reset status for standard queuing
		newTransaction.Inflight = false
	}

	return &newTransaction
}

// RefundTransaction processes a refund for a given transaction by its ID.
// It starts a tracing span, retrieves the original transaction, validates its status, creates a new refund transaction, and queues it.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transactionID string: The ID of the transaction to be refunded.
//
// Returns:
// - *model.Transaction: A pointer to the refunded Transaction model.
// - error: An error if the transaction could not be refunded.
func (l *Blnk) RefundTransaction(ctx context.Context, transactionID string, skipQueue bool) (*model.Transaction, error) {
	return l.RefundTransactionWithOptions(ctx, transactionID, RefundOptions{SkipQueue: skipQueue})
}

// RefundTransactionWithOptions refunds a transaction, letting the caller set
// the reversal's description and metadata rather than inheriting the
// original's.
//
// RefundTransaction is the same call with only SkipQueue set.
func (l *Blnk) RefundTransactionWithOptions(ctx context.Context, transactionID string, opts RefundOptions) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RefundTransaction")
	defer span.End()

	// Serialize refunds of the same transaction so the already-refunded check
	// can't be raced into a double refund.
	lockKey := fmt.Sprintf("refund:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))
	if err := locker.Lock(ctx, l.Config().Transaction.LockDuration); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for refund: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

	// 1. Retrieve the original transaction (from DB or Queue)
	originalTxn, err := l.getOriginalTransactionForRefund(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err // Error includes context from the helper function
	}

	// 2. Validate if the transaction can be refunded
	if err := l.validateTransactionForRefund(ctx, originalTxn); err != nil {
		span.RecordError(err)
		return nil, err // Error includes context from the helper function
	}

	// 3. Prepare the refund transaction object
	refundTxnObject := prepareRefundTransaction(originalTxn, opts)

	// 4. Queue the refund transaction for processing
	queuedRefundTxn, err := l.QueueTransaction(ctx, refundTxnObject)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to queue refund transaction %s: %w", refundTxnObject.TransactionID, err)
	}

	span.AddEvent("Refund transaction queued", trace.WithAttributes(attribute.String("refund.transaction.id", queuedRefundTxn.TransactionID)))
	return queuedRefundTxn, nil
}
