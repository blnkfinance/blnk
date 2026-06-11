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
	"time"

	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	StatusInflight = "INFLIGHT"
	StatusVoid     = "VOID"
	StatusCommit   = "COMMIT"
)

// GetInflightTransactionsByParentID retrieves inflight transactions by their parent transaction ID.
// It starts a tracing span, fetches the transactions from the datasource, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The ID of the parent transaction.
// - batchSize int: The number of transactions to retrieve in a batch.
// - offset int64: The offset for pagination.
//
// Returns:
// - []*model.Transaction: A slice of pointers to the retrieved Transaction models.
// - error: An error if the transactions could not be retrieved.
func (l *Blnk) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetInflightTransactionsByParentID")
	defer span.End()

	transactions, err := l.datasource.GetInflightTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("parent_transaction_id", parentTransactionID))
	span.AddEvent("Inflight transactions retrieved")
	return transactions, nil
}

// IsInflightTransaction checks whether a transaction is considered "inflight" based on its status and metadata.
// A transaction is considered inflight if:
// 1. It has a status of StatusInflight, OR
// 2. It has a status of StatusQueued AND has the inflight flag set to true in its metadata
//
// Parameters:
// - transaction *model.Transaction: The transaction to check
//
// Returns:
// - bool: true if the transaction is considered inflight, false otherwise
func IsInflightTransaction(transaction *model.Transaction) bool {
	return transaction.Status == StatusInflight ||
		(transaction.Status == StatusQueued &&
			transaction.MetaData != nil &&
			transaction.MetaData["inflight"] == true)
}

// CommitWorker processes commit transactions from the jobs channel and sends the results to the results channel.
// It starts a tracing span, processes each transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount *big.Int: The amount to be processed in the transaction.
func (l *Blnk) CommitWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int) {
	ctx, span := tracer.Start(ctx, "CommitWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		if !IsInflightTransaction(originalTxn) {
			err := fmt.Errorf("transaction is not in inflight status")
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
			continue
		}
		queuedCommitTxn, err := l.CommitInflightTransaction(ctx, originalTxn.TransactionID, amount)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
			continue
		}
		results <- BatchJobResult{Txn: queuedCommitTxn}
		span.AddEvent("Commit processed", trace.WithAttributes(attribute.String("transaction.id", queuedCommitTxn.TransactionID)))
	}
}

// VoidWorker processes void transactions from the jobs channel and sends the results to the results channel.
// It starts a tracing span, processes each transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount *big.Int: The amount to be processed in the transaction.
func (l *Blnk) VoidWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int) {
	ctx, span := tracer.Start(ctx, "VoidWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		if !IsInflightTransaction(originalTxn) {
			err := fmt.Errorf("transaction is not in inflight status")
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
			continue
		}
		queuedVoidTxn, err := l.VoidInflightTransaction(ctx, originalTxn.TransactionID)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
			continue
		}
		results <- BatchJobResult{Txn: queuedVoidTxn}
		span.AddEvent("Void processed", trace.WithAttributes(attribute.String("transaction.id", queuedVoidTxn.TransactionID)))
	}
}

// releaseSingleLock releases a single distributed lock.
// This is used for operations that lock on a single key (e.g., inflight transaction operations).
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - locker *redlock.Locker: The Locker object representing the acquired lock.
func (l *Blnk) releaseSingleLock(ctx context.Context, locker *redlock.Locker) {
	ctx, span := tracer.Start(ctx, "ReleaseSingleLock")
	defer span.End()

	if err := locker.Unlock(ctx); err != nil {
		span.RecordError(err)
		logrus.WithError(err).Error("failed to release lock")
	}
	span.AddEvent("Lock released")
}

// CommitInflightTransaction commits an inflight transaction by validating and updating its amount, and finalizing the commitment.
// It starts a tracing span, fetches and validates the inflight transaction, updates the amount, and finalizes the commitment.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transactionID string: The ID of the inflight transaction to be committed.
// - amount float64: The amount to be validated and updated in the transaction.
//
// Returns:
// - *model.Transaction: A pointer to the committed Transaction model.
// - error: An error if the transaction could not be committed.
func (l *Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount *big.Int) (*model.Transaction, error) {
	return l.CommitInflightTransactionWithRef(ctx, transactionID, amount, "")
}

// CommitInflightTransactionWithRef commits an inflight transaction using a
// caller-supplied reference for the committed child. A non-empty reference makes
// asynq retries idempotent: a retry that re-commits the same amount collides on
// the unique reference index. An empty reference preserves the legacy behavior
// of generating a random reference.
func (l *Blnk) CommitInflightTransactionWithRef(ctx context.Context, transactionID string, amount *big.Int, reference string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "CommitInflightTransaction")
	defer span.End()

	lockKey := fmt.Sprintf("inflight-commit:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for inflight commit: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

	transaction, err := l.fetchAndValidateInflightTransaction(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := l.validateAndUpdateAmount(ctx, transaction, amount); err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Inflight transaction committed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	metrics.InflightCommitTotal.Add(ctx, 1)

	committedTxn, err := l.finalizeCommitment(ctx, transaction, false, reference)
	if err != nil {
		return nil, err
	}

	if err := l.queueShadowWork(ctx, transactionID, model.LineageTypeShadowCommit); err != nil {
		logrus.WithError(err).WithField("transaction_id", transactionID).Error("failed to queue shadow commit")
	}

	return committedTxn, nil
}

func (l *Blnk) CommitInflightTransactionWithQueue(ctx context.Context, transactionID string, amount *big.Int) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "CommitInflightTransactionWithQueue")
	defer span.End()

	lockKey := fmt.Sprintf("inflight-commit:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for inflight commit: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

	transaction, err := l.fetchAndValidateInflightTransaction(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := l.validateAndUpdateAmount(ctx, transaction, amount); err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Inflight transaction committed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	committedTxn, err := l.finalizeCommitment(ctx, transaction, true, "")
	if err != nil {
		return nil, err
	}

	if err := l.queueShadowWork(ctx, transactionID, model.LineageTypeShadowCommit); err != nil {
		logrus.WithError(err).WithField("transaction_id", transactionID).Error("failed to queue shadow commit")
	}

	return committedTxn, nil
}

// validateAndUpdateAmount validates the amount to be committed for a transaction and updates the transaction's amount.
// It orchestrates the validation and update process by calling more specialized functions.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be validated and updated.
// - amount float64: The amount to be validated and updated in the transaction.
//
// Returns:
// - error: An error if the amount validation or update fails.
func (l *Blnk) validateAndUpdateAmount(ctx context.Context, transaction *model.Transaction, amount *big.Int) error {
	ctx, span := tracer.Start(ctx, "ValidateAndUpdateAmount")
	defer span.End()

	amountLeft, err := l.calculateRemainingAmount(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := l.checkTransactionCommitStatus(amountLeft); err != nil {
		span.RecordError(err)
		return err
	}

	if err := l.validateRequestedAmount(transaction, amount, amountLeft); err != nil {
		span.RecordError(err)
		return err
	}

	l.updateTransactionAmount(transaction, amount, amountLeft)

	amountLeftValue := l.convertPreciseToFloat(amountLeft, transaction.Precision)
	span.AddEvent("Amount validated and updated", trace.WithAttributes(attribute.Float64("amount.left", amountLeftValue)))
	return nil
}

// checkTransactionCommitStatus checks if a transaction can be committed based on its remaining amount.
//
// Parameters:
// - amountLeft *big.Int: The remaining amount that can be committed.
//
// Returns:
// - error: An error if the transaction is already fully committed.
func (l *Blnk) checkTransactionCommitStatus(amountLeft *big.Int) error {
	if amountLeft.Cmp(big.NewInt(0)) == 0 {
		return errors.New("cannot commit. Transaction already committed")
	}
	return nil
}

// validateRequestedAmount validates that the requested amount is within allowed limits.
//
// Parameters:
// - transaction *model.Transaction: The transaction being validated.
// - amount float64: The requested amount to commit.
// - amountLeft *big.Int: The remaining amount that can be committed.
//
// Returns:
// - error: An error if the requested amount exceeds allowed limits.
func (l *Blnk) validateRequestedAmount(transaction *model.Transaction, amount *big.Int, amountLeft *big.Int) error {
	if amount.Sign() < 0 {
		return fmt.Errorf("commit amount cannot be negative")
	}
	// Zero is the sentinel for "commit the full remaining amount".
	if amount.Sign() == 0 {
		return nil
	}

	requestedAmount := amount

	if requestedAmount.Cmp(transaction.PreciseAmount) > 0 {
		return fmt.Errorf("cannot commit more than the original transaction amount. Original: %s%.2f, Requested: %s%.2f",
			transaction.Currency, transaction.Amount, transaction.Currency, amount)
	}

	if requestedAmount.Cmp(amountLeft) > 0 {
		amountLeftValue := l.convertPreciseToFloat(amountLeft, transaction.Precision)
		return fmt.Errorf("cannot commit more than the remaining amount. Available: %s%.2f, Requested: %s%.2f",
			transaction.Currency, amountLeftValue, transaction.Currency, amount)
	}

	return nil
}

// updateTransactionAmount updates the transaction with the specified amount or the full remaining amount.
//
// Parameters:
// - transaction *model.Transaction: The transaction to update.
// - amount float64: The amount to commit (0 means commit the full remaining amount).
// - amountLeft *big.Int: The remaining amount that can be committed.
func (l *Blnk) updateTransactionAmount(transaction *model.Transaction, amount *big.Int, amountLeft *big.Int) {
	if amount.Cmp(big.NewInt(0)) != 0 {
		transaction.PreciseAmount = amount
	} else {
		transaction.Amount = l.convertPreciseToFloat(amountLeft, transaction.Precision)
		transaction.PreciseAmount = amountLeft
	}
}

// convertPreciseToFloat converts a precise amount (big.Int) to a floating-point representation.
//
// Parameters:
// - preciseAmount *big.Int: The precise amount to convert.
// - precision float64: The precision factor (e.g., 100 for 2 decimal places).
//
// Returns:
// - float64: The floating-point representation of the precise amount.
func (l *Blnk) convertPreciseToFloat(preciseAmount *big.Int, precision float64) float64 {
	precisionBigInt := new(big.Float).SetFloat64(precision)
	amountFloat := new(big.Float).SetInt(preciseAmount)
	result, _ := new(big.Float).Quo(amountFloat, precisionBigInt).Float64()
	return result
}

// finalizeCommitment finalizes the commitment of a transaction by updating its status and generating new identifiers.
// It starts a tracing span, updates the transaction details, queues the transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be finalized.
//
// Returns:
// - *model.Transaction: A pointer to the finalized Transaction model.
// - error: An error if the transaction could not be queued.
func (l *Blnk) finalizeCommitment(ctx context.Context, transaction *model.Transaction, withQueue bool, reference string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeCommitment")
	defer span.End()

	transaction.Status = StatusCommit
	transaction.ParentTransaction = transaction.TransactionID
	transaction.CreatedAt = time.Now()
	if transaction.EffectiveDate == nil {
		transaction.EffectiveDate = &transaction.CreatedAt
	}
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	if reference != "" {
		transaction.Reference = reference
	} else {
		transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	}
	transaction.Hash = transaction.HashTxn()

	if withQueue {
		err := enqueueTransactions(ctx, l.queue, transaction, nil)
		if err != nil {
			span.RecordError(err)
			return nil, l.logAndRecordError(span, "failed to enqueue transaction", err)
		}
	} else {
		transaction, err := l.RecordTransaction(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, l.logAndRecordError(span, "saving transaction to db error", err)
		}
		return transaction, nil
	}

	transaction.Status = StatusApplied

	span.AddEvent("Commitment finalized", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

// VoidInflightTransaction voids an inflight transaction by validating it, calculating the remaining amount, and finalizing the void.
// It starts a tracing span, fetches and validates the inflight transaction, calculates the remaining amount, and finalizes the void.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transactionID string: The ID of the inflight transaction to be voided.
//
// Returns:
// - *model.Transaction: A pointer to the voided Transaction model.
// - error: An error if the transaction could not be voided.
func (l *Blnk) VoidInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	return l.VoidInflightTransactionWithRef(ctx, transactionID, "")
}

// VoidInflightTransactionWithRef voids an inflight transaction using a
// caller-supplied reference for the voided child, making asynq retries
// idempotent. An empty reference preserves the legacy random-reference behavior.
func (l *Blnk) VoidInflightTransactionWithRef(ctx context.Context, transactionID string, reference string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "VoidInflightTransaction")
	defer span.End()

	lockKey := fmt.Sprintf("inflight-commit:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for inflight void: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

	transaction, err := l.fetchAndValidateInflightTransaction(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	amountLeft, err := l.calculateRemainingAmount(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if amountLeft.Cmp(big.NewInt(0)) == 0 {
		err := errors.New("cannot void. Transaction already committed")
		span.RecordError(err)
		return transaction, err
	}
	span.AddEvent("Inflight transaction voided", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	metrics.InflightVoidTotal.Add(ctx, 1)

	voidedTxn, err := l.finalizeVoidTransaction(ctx, transaction, amountLeft, reference)
	if err != nil {
		return nil, err
	}

	if err := l.queueShadowWork(ctx, transactionID, model.LineageTypeShadowVoid); err != nil {
		logrus.WithError(err).WithField("transaction_id", transactionID).Error("failed to queue shadow void")
	}

	return voidedTxn, nil
}

// fetchAndValidateInflightTransaction fetches and validates an inflight transaction by its ID.
// It starts a tracing span, attempts to retrieve the transaction from the database or queue, and validates its status.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transactionID string: The ID of the inflight transaction to be fetched and validated.
//
// Returns:
// - *model.Transaction: A pointer to the validated Transaction model.
// - error: An error if the transaction could not be fetched or validated.
func (l *Blnk) fetchAndValidateInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FetchAndValidateInflightTransaction")
	defer span.End()

	var transaction *model.Transaction

	dbTransaction, err := l.datasource.GetTransaction(ctx, transactionID)
	switch err {
	case nil:
		transaction = dbTransaction
	case sql.ErrNoRows:
		queuedTxn, err := l.queue.GetTransactionFromQueue(transactionID)
		if err != nil {
			span.RecordError(err)
			return &model.Transaction{}, err
		}
		if queuedTxn == nil {
			err := fmt.Errorf("transaction not found")
			span.RecordError(err)
			return nil, err
		}
		logrus.Info("found inflight transaction in queue using it for commit/void", transactionID, queuedTxn.TransactionID)
		transaction = queuedTxn
	default:
		span.RecordError(err)
		return &model.Transaction{}, err
	}

	if !IsInflightTransaction(transaction) {
		err := fmt.Errorf("transaction is not in inflight status")
		span.RecordError(err)
		return nil, err
	}

	parentVoided, err := l.datasource.IsParentTransactionVoid(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "error checking parent transaction status", err)
	}

	if parentVoided {
		err := fmt.Errorf("transaction has already been voided")
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Inflight transaction validated", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

// calculateRemainingAmount calculates the remaining amount for an inflight transaction by subtracting the committed amount from the precise amount.
// It starts a tracing span, fetches the total committed amount, calculates the remaining amount, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to calculate the remaining amount.
//
// Returns:
// - int64: The remaining amount for the transaction.
// - error: An error if the committed amount could not be fetched.
func (l *Blnk) calculateRemainingAmount(ctx context.Context, transaction *model.Transaction) (*big.Int, error) {
	ctx, span := tracer.Start(ctx, "CalculateRemainingAmount")
	defer span.End()

	committedAmount, err := l.datasource.GetTotalCommittedTransactions(ctx, transaction.TransactionID)
	if err != nil {
		span.RecordError(err)
		return big.NewInt(0), l.logAndRecordError(span, "error fetching committed amount", err)
	}

	remainingAmount := new(big.Int).Sub(transaction.PreciseAmount, committedAmount)

	span.AddEvent("Remaining amount calculated", trace.WithAttributes(attribute.Int64("amount.remaining", remainingAmount.Int64())))
	return remainingAmount, nil
}

// finalizeVoidTransaction finalizes the voiding of a transaction by updating its status and generating new identifiers.
// It starts a tracing span, updates the transaction details, queues the transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be voided.
// - amountLeft int64: The remaining amount to be set in the transaction.
//
// Returns:
// - *model.Transaction: A pointer to the voided Transaction model.
// - error: An error if the transaction could not be queued.
func (l *Blnk) finalizeVoidTransaction(ctx context.Context, transaction *model.Transaction, amountLeft *big.Int, reference string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeVoidTransaction")
	defer span.End()

	transaction.Status = StatusVoid
	transaction.PreciseAmount = amountLeft
	transaction.CreatedAt = time.Now()
	if transaction.EffectiveDate == nil {
		transaction.EffectiveDate = &transaction.CreatedAt
	}
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	if reference != "" {
		transaction.Reference = reference
	} else {
		transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	}
	transaction.Hash = transaction.HashTxn()
	model.ApplyPrecision(transaction)

	transaction, err := l.RecordTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	span.AddEvent("Void transaction finalized", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

// inflightActionReference derives the deterministic reference for a committed or
// voided leg from the action ID carried in the queued task. The same actionID
// on an asynq retry reproduces the same reference, so a re-applied leg collides
// on the unique reference index. An empty actionID yields "" (random reference).
func inflightActionReference(legTransactionID, action, actionID string) string {
	if actionID == "" {
		return ""
	}
	return fmt.Sprintf("%s_%s_%s", legTransactionID, action, actionID)
}

// isTerminalInflightError reports whether an inflight commit/void error is
// terminal — the action cannot succeed on retry and the queued task should be
// acked rather than retried.
func (l *Blnk) isTerminalInflightError(err error) bool {
	if IsDuplicateReferenceError(err) {
		return true
	}
	switch classifyInflightError(err) {
	case "ALREADY_COMMITTED", "ALREADY_VOIDED", "NOT_INFLIGHT", "INVALID_AMOUNT":
		return true
	default:
		return false
	}
}

// collectInflightLegs snapshots every inflight leg under a parent transaction
// (transaction_id == parent OR parent_transaction == parent). Committing a leg
// creates a child row but leaves the leg's own row INFLIGHT, so snapshotting up
// front avoids re-processing the same legs across pages.
func (l *Blnk) collectInflightLegs(ctx context.Context, parentTransactionID string) ([]*model.Transaction, error) {
	batchSize := l.Config().Transaction.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	var legs []*model.Transaction
	for offset := int64(0); ; offset += int64(batchSize) {
		batch, err := l.GetInflightTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
		if err != nil {
			return nil, err
		}
		legs = append(legs, batch...)
		if len(batch) < batchSize {
			return legs, nil
		}
	}
}

// applyInflightActionToLeg commits or voids a single inflight leg using the
// deterministic reference (derived from actionID) that makes asynq retries
// idempotent.
func (l *Blnk) applyInflightActionToLeg(ctx context.Context, leg *model.Transaction, action string, amount *big.Int, actionID string) error {
	ref := inflightActionReference(leg.TransactionID, action, actionID)
	if action == InflightActionVoid {
		_, err := l.VoidInflightTransactionWithRef(ctx, leg.TransactionID, ref)
		return err
	}
	_, err := l.CommitInflightTransactionWithRef(ctx, leg.TransactionID, amount, ref)
	return err
}

// RunInflightActionByParent commits or voids every inflight leg under a parent
// transaction — the same expansion the synchronous endpoint performs — and is
// the queued worker's entrypoint.
//
// retryable is true when at least one leg failed with a transient error (lock
// contention, fetch failure, not-yet-visible), so the asynq task should retry;
// already-finalized legs are terminal, skipped, and never block the ack.
func (l *Blnk) RunInflightActionByParent(ctx context.Context, parentTransactionID, action string, amount *big.Int, actionID string) (retryable bool, err error) {
	ctx, span := tracer.Start(ctx, "RunInflightActionByParent")
	defer span.End()

	legs, err := l.collectInflightLegs(ctx, parentTransactionID)
	if err != nil {
		span.RecordError(err)
		return true, err // a fetch failure is transient
	}
	if len(legs) == 0 {
		// Nothing inflight under this parent: already finalized or never existed.
		span.AddEvent("No inflight legs to process")
		return false, nil
	}

	var applied, skipped int
	var lastTransientErr error
	for _, leg := range legs {
		switch err := l.applyInflightActionToLeg(ctx, leg, action, amount, actionID); {
		case err == nil:
			applied++
		case l.isTerminalInflightError(err):
			// Expected on a retry once a leg is already finalized; benign.
			skipped++
			logrus.WithFields(logrus.Fields{
				"transaction_id": leg.TransactionID,
				"reason":         classifyInflightError(err),
			}).Debug("inflight leg already finalized; skipping (idempotent)")
		default:
			span.RecordError(err)
			lastTransientErr = err
		}
	}

	if lastTransientErr != nil {
		return true, lastTransientErr
	}

	logrus.WithFields(logrus.Fields{
		"parent_transaction_id": parentTransactionID,
		"action":                action,
		"applied":               applied,
		"skipped":               skipped,
	}).Info("inflight action processed")
	return false, nil
}

// preValidateInflightAction performs the read-only checks an inflight commit/void
// would perform, without mutating or persisting anything. It lets the API reject
// an obviously-invalid request (not inflight, already finalized, amount too high)
// synchronously before enqueuing, while the worker remains the authority.
func (l *Blnk) preValidateInflightAction(ctx context.Context, transactionID, action string, amount *big.Int) (*model.Transaction, error) {
	transaction, err := l.fetchAndValidateInflightTransaction(ctx, transactionID)
	if err != nil {
		return nil, err
	}

	amountLeft, err := l.calculateRemainingAmount(ctx, transaction)
	if err != nil {
		return nil, err
	}

	if err := l.checkTransactionCommitStatus(amountLeft); err != nil {
		return nil, err
	}

	if action == InflightActionCommit && amount != nil {
		if err := l.validateRequestedAmount(transaction, amount, amountLeft); err != nil {
			return nil, err
		}
	}

	return transaction, nil
}

// QueueInflightAction pre-validates a commit/void and enqueues it for the worker.
// It returns the still-inflight parent transaction (for the API response) and
// ErrInflightActionQueued if an action for the same transaction is already queued.
func (l *Blnk) QueueInflightAction(ctx context.Context, transactionID string, amount *big.Int, action string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "QueueInflightAction")
	defer span.End()

	transaction, err := l.preValidateInflightAction(ctx, transactionID, action, amount)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	preciseAmount := "0"
	if amount != nil {
		preciseAmount = amount.String()
	}

	payload := InflightActionPayload{
		TransactionID: transactionID,
		Action:        action,
		PreciseAmount: preciseAmount,
		ActionID:      model.GenerateUUIDWithSuffix("act"),
	}
	if err := l.queue.EnqueueInflightAction(ctx, payload); err != nil {
		span.RecordError(err)
		return nil, err
	}

	return transaction, nil
}

// BulkInflightAction discriminates between the two operations supported by
// BulkInflightUpdate.
type BulkInflightAction int

const (
	BulkInflightVoid BulkInflightAction = iota
	BulkInflightCommit
)

// BulkInflightItem is the internal per-item shape consumed by
// BulkInflightUpdate. For void calls only TransactionID is used.
type BulkInflightItem struct {
	TransactionID string
	Amount        *big.Int
}

// BulkInflightOutcome is the per-item outcome from BulkInflightUpdate.
// On success Err is nil; on failure Code carries a stable classification
// (see classifyInflightError) and Err preserves the original message.
type BulkInflightOutcome struct {
	TransactionID string
	Txn           *model.Transaction
	Err           error
	Code          string
}

// classifyInflightError maps the unstructured errors emitted by
// CommitInflightTransaction / VoidInflightTransaction to stable codes so
// bulk callers can branch on them programmatically instead of regex-matching
// human-readable messages.
//
// Codes are intentionally action-agnostic where possible. ALREADY_VOIDED and
// ALREADY_COMMITTED are reported as observed — a void on an already-committed
// txn surfaces ALREADY_COMMITTED, and vice versa.
// ClassifyInflightError maps an inflight commit/void error to a stable code
// (see classifyInflightError) so callers outside this package — e.g. the queued
// bulk API path — can report per-item codes without regex-matching messages.
func ClassifyInflightError(err error) string {
	return classifyInflightError(err)
}

func classifyInflightError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "transaction not found"),
		strings.Contains(msg, "no rows in result set"),
		strings.Contains(msg, "not found"):
		return "NOT_FOUND"
	case strings.Contains(msg, "has already been voided"):
		return "ALREADY_VOIDED"
	case strings.Contains(msg, "Transaction already committed"),
		strings.Contains(msg, "cannot void. Transaction already committed"):
		return "ALREADY_COMMITTED"
	case strings.Contains(msg, "not in inflight status"):
		return "NOT_INFLIGHT"
	case strings.Contains(msg, "cannot commit more than"):
		return "INVALID_AMOUNT"
	case strings.Contains(msg, "failed to acquire lock"):
		return "LOCKED"
	default:
		return "INTERNAL_ERROR"
	}
}

// BulkInflightUpdate voids or commits a list of independently-created
// inflight transactions in parallel.
//
// Each id is processed in its own goroutine via a worker pool of size
// `maxWorkers` (clamped to [1, 16]). Per-item failures are reported in the
// returned result slice and do not abort the rest of the batch; the returned
// error is non-nil only for input validation failures (empty list, too many
// items).
//
// Idempotency: bulk has no batch-wide idempotency key. Retries that include
// already-processed ids will see those ids surface as ALREADY_VOIDED /
// ALREADY_COMMITTED, which callers should treat as success-equivalent for
// retry semantics.
//
// Results are returned in the same order as the input.
func (l *Blnk) BulkInflightUpdate(ctx context.Context, action BulkInflightAction, items []BulkInflightItem, maxWorkers int) ([]BulkInflightOutcome, error) {
	if len(items) == 0 {
		return nil, errors.New("transaction_ids cannot be empty")
	}
	if maxWorkers < 1 {
		maxWorkers = 4
	}
	if maxWorkers > 16 {
		maxWorkers = 16
	}
	if maxWorkers > len(items) {
		maxWorkers = len(items)
	}

	results := make([]BulkInflightOutcome, len(items))
	type job struct {
		idx  int
		item BulkInflightItem
	}
	jobs := make(chan job, len(items))
	for i, it := range items {
		jobs <- job{idx: i, item: it}
	}
	close(jobs)

	var wg sync.WaitGroup
	for w := 0; w < maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				var txn *model.Transaction
				var err error
				switch action {
				case BulkInflightCommit:
					txn, err = l.CommitInflightTransaction(ctx, j.item.TransactionID, j.item.Amount)
				case BulkInflightVoid:
					txn, err = l.VoidInflightTransaction(ctx, j.item.TransactionID)
				default:
					err = fmt.Errorf("unknown bulk inflight action: %d", action)
				}
				results[j.idx] = BulkInflightOutcome{
					TransactionID: j.item.TransactionID,
					Txn:           txn,
					Err:           err,
					Code:          classifyInflightError(err),
				}
			}
		}()
	}
	wg.Wait()
	return results, nil
}
