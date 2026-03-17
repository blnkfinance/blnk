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
	"log"
	"math/big"
	"sync"
	"time"

	redlock "github.com/blnkfinance/blnk/internal/lock"
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

	committedTxn, err := l.finalizeCommitment(ctx, transaction, false)
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

	committedTxn, err := l.finalizeCommitment(ctx, transaction, true)
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
	if amount.Cmp(big.NewInt(0)) == 0 {
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
func (l *Blnk) finalizeCommitment(ctx context.Context, transaction *model.Transaction, withQueue bool) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeCommitment")
	defer span.End()

	transaction.Status = StatusCommit
	transaction.ParentTransaction = transaction.TransactionID
	transaction.CreatedAt = time.Now()
	if transaction.EffectiveDate == nil {
		transaction.EffectiveDate = &transaction.CreatedAt
	}
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
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

	voidedTxn, err := l.finalizeVoidTransaction(ctx, transaction, amountLeft)
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
		log.Println("found inflight transaction in queue using it for commit/void", transactionID, queuedTxn.TransactionID)
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
func (l *Blnk) finalizeVoidTransaction(ctx context.Context, transaction *model.Transaction, amountLeft *big.Int) (*model.Transaction, error) {
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
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
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
