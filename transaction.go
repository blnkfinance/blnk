// Apache License
// Version 2.0, January 2004
// http://www.apache.org/licenses/

// Copyright 2024 Blnk Finance
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blnk

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	redlock "github.com/jerry-enebeli/blnk/internal/lock"
	"github.com/jerry-enebeli/blnk/internal/notification"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	"github.com/jerry-enebeli/blnk/model"
)

var (
	tracer = otel.Tracer("blnk.transactions")
)

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
	StatusInflight  = "INFLIGHT"
	StatusVoid      = "VOID"
	StatusCommit    = "COMMIT"
	StatusRejected  = "REJECTED"
)

type getTxns func(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)
type transactionWorker func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64)
type BatchJobResult struct {
	Txn   *model.Transaction
	Error error
}

func (l *Blnk) getSourceAndDestination(ctx context.Context, transaction *model.Transaction) (source *model.Balance, destination *model.Balance, err error) {
	ctx, span := tracer.Start(ctx, "GetSourceAndDestination")
	defer span.End()

	var sourceBalance, destinationBalance *model.Balance

	// Check if Source starts with "@"
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err = l.getOrCreateBalanceByIndicator(ctx, transaction.Source, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			logrus.Errorf("source error %v", err)
			return nil, nil, err
		}
		// Update transaction source with the balance ID
		transaction.Source = sourceBalance.BalanceID
		span.SetAttributes(attribute.String("source.balance_id", sourceBalance.BalanceID))
	} else {
		sourceBalance, err = l.datasource.GetBalanceByIDLite(transaction.Source)
		if err != nil {
			span.RecordError(err)
			logrus.Errorf("source error %v", err)
			return nil, nil, err
		}
	}

	// Check if Destination starts with "@"
	if strings.HasPrefix(transaction.Destination, "@") {
		destinationBalance, err = l.getOrCreateBalanceByIndicator(ctx, transaction.Destination, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			logrus.Errorf("destination error %v", err)
			return nil, nil, err
		}
		// Update transaction destination with the balance ID
		transaction.Destination = destinationBalance.BalanceID
		span.SetAttributes(attribute.String("destination.balance_id", destinationBalance.BalanceID))
	} else {
		destinationBalance, err = l.datasource.GetBalanceByIDLite(transaction.Destination)
		if err != nil {
			span.RecordError(err)
			logrus.Errorf("destination error %v", err)
			return nil, nil, err
		}
	}
	span.AddEvent("Retrieved source and destination balances")
	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) acquireLock(ctx context.Context, transaction *model.Transaction) (*redlock.Locker, error) {
	ctx, span := tracer.Start(ctx, "Acquiring Lock")
	defer span.End()

	locker := redlock.NewLocker(l.redis, transaction.Source, model.GenerateUUIDWithSuffix("loc"))
	err := locker.Lock(ctx, time.Minute*30)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Lock acquired")
	return locker, nil
}

func (l *Blnk) updateTransactionDetails(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.Transaction {
	_, span := tracer.Start(ctx, "Updating Transaction Details")
	defer span.End()

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID
	applicableStatus := map[string]string{StatusQueued: StatusApplied, StatusScheduled: StatusApplied, StatusCommit: StatusApplied, StatusVoid: StatusVoid}
	transaction.Status = applicableStatus[transaction.Status]
	if transaction.Inflight {
		transaction.Status = StatusInflight
	}
	span.AddEvent("Transaction details updated")
	return transaction
}

func (l *Blnk) persistTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Persisting Transaction")
	defer span.End()

	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		logrus.Errorf("ERROR saving transaction to db. %s", err)
		return nil, err
	}
	span.SetAttributes(attribute.String("transaction.id", transaction.TransactionID))
	span.AddEvent("Transaction persisted")
	return transaction, nil
}

func (l *Blnk) postTransactionActions(ctx context.Context, transaction *model.Transaction) {
	_, span := tracer.Start(ctx, "Post Transaction Actions")
	defer span.End()

	go func() {
		err := l.queue.queueIndexData(transaction.TransactionID, "transactions", transaction)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
		err = SendWebhook(NewWebhook{
			Event:   getEventFromStatus(transaction.Status),
			Payload: transaction,
		})
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
		span.AddEvent("Post-transaction actions completed")
	}()
}

func (l *Blnk) updateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "Updating Balances")
	defer span.End()

	var wg sync.WaitGroup
	if err := l.datasource.UpdateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		span.RecordError(err)
		return err
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(ctx, sourceBalance)
		//	err := l.queue.queueIndexData(sourceBalance.BalanceID, "balances", sourceBalance)
		// if err != nil {
		// 	span.RecordError(err)
		// 	notification.NotifyError(err)
		// }
	}()
	go func() {
		defer wg.Done()
		// l.checkBalanceMonitors(ctx, destinationBalance)
		// err := l.queue.queueIndexData(destinationBalance.BalanceID, "balances", destinationBalance)
		// if err != nil {
		// 	span.RecordError(err)
		// 	notification.NotifyError(err)
		// }
	}()
	wg.Wait()
	span.AddEvent("Balances updated")
	return nil
}

func (l *Blnk) validateTxn(ctx context.Context, transaction *model.Transaction) error {
	ctx, span := tracer.Start(ctx, "Validating Transaction Reference")
	defer span.End()

	txn, err := l.datasource.TransactionExistsByRef(ctx, transaction.Reference)
	if err != nil {
		return err
	}

	if txn {
		err := fmt.Errorf("reference %s has already been used", transaction.Reference)
		span.RecordError(err)
		return err
	}

	span.AddEvent("Transaction validated")
	return nil
}

func (l *Blnk) applyTransactionToBalances(ctx context.Context, balances []*model.Balance, transaction *model.Transaction) error {
	_, span := tracer.Start(ctx, "Applying Transaction to Balances")
	defer span.End()

	span.AddEvent("Calculating new balances")

	if transaction.Status == StatusCommit {
		balances[0].CommitInflightDebit(transaction)
		balances[1].CommitInflightCredit(transaction)
		span.AddEvent("Committed inflight balances")
		return nil
	}

	transactionAmount := new(big.Int).SetInt64(transaction.PreciseAmount)
	if transaction.Status == StatusVoid {
		balances[0].RollbackInflightDebit(transactionAmount)
		balances[1].RollbackInflightCredit(transactionAmount)
		span.AddEvent("Rolled back inflight balances")
		return nil
	}

	err := model.UpdateBalances(transaction, balances[0], balances[1])
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Balances updated")
	return nil
}

func (l *Blnk) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetInflightTransactionsByParentID")
	defer span.End()

	transactions, err := l.datasource.GetInflightTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("parent_transaction_id", parentTransactionID))
	return transactions, nil
}

func (l *Blnk) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetRefundableTransactionsByParentID")
	defer span.End()

	transactions, err := l.datasource.GetRefundableTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("parent_transaction_id", parentTransactionID))
	return transactions, nil
}

func (l *Blnk) ProcessTransactionInBatches(ctx context.Context, parentTransactionID string, amount float64, maxWorkers int, streamMode bool, gt getTxns, tw transactionWorker) ([]*model.Transaction, error) {
	_, span := tracer.Start(ctx, "ProcessTransactionInBatches")
	const (
		batchSize    = 100000
		maxQueueSize = 1000
	)
	var allTxns []*model.Transaction
	var allErrors []error
	var mu sync.Mutex

	jobs := make(chan *model.Transaction, maxQueueSize)
	results := make(chan BatchJobResult, maxQueueSize)

	// Start worker pool
	var wg sync.WaitGroup
	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go tw(ctx, jobs, results, &wg, amount)
	}

	// Start a goroutine to close the results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	if !streamMode {
		// Start a goroutine to process results
		done := make(chan struct{})
		go processResults(results, &mu, &allTxns, &allErrors, done)

		// Fetch and process transactions in batches concurrently
		errChan := make(chan error, 1)
		go fetchTransactions(ctx, parentTransactionID, batchSize, gt, jobs, errChan)

		// Wait for all processing to complete
		select {
		case err := <-errChan:
			span.RecordError(err)
			return allTxns, err
		case <-done:
		}

		if len(allErrors) > 0 {
			// Log errors and return a combined error
			for _, err := range allErrors {
				log.Printf("Error during processing: %v", err)
				span.RecordError(err)
			}
			return allTxns, fmt.Errorf("multiple errors occurred during processing: %v", allErrors)
		}

		span.AddEvent("Processed all transactions in batches")
		return allTxns, nil
	} else {
		var wg sync.WaitGroup
		// Stream mode: just fetch transactions and send to jobs channel
		errChan := make(chan error, 1)

		wg.Add(1)
		go func() {
			defer wg.Done()
			fetchTransactions(ctx, parentTransactionID, batchSize, gt, jobs, errChan)
		}()

		wg.Wait()
		span.AddEvent("Processed all transactions in streaming mode")
		return nil, nil
	}
}

func processResults(results chan BatchJobResult, mu *sync.Mutex, allTxns *[]*model.Transaction, allErrors *[]error, done chan struct{}) {
	for result := range results {
		mu.Lock()
		if result.Error != nil {
			*allErrors = append(*allErrors, result.Error)
		} else if result.Txn != nil {
			*allTxns = append(*allTxns, result.Txn)
		}
		mu.Unlock()
	}
	close(done)
}

func fetchTransactions(ctx context.Context, parentTransactionID string, batchSize int, gt getTxns, jobs chan *model.Transaction, errChan chan error) {
	newCtx, span := tracer.Start(ctx, "FetchTransactions")
	defer span.End()

	var offset int64 = 0
	for {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			span.RecordError(ctx.Err())
			return
		default:
			txns, err := gt(newCtx, parentTransactionID, batchSize, offset)
			if err != nil {
				errChan <- err
				span.RecordError(err)
				return
			}
			if len(txns) == 0 {
				close(jobs)
				span.AddEvent("No more transactions to fetch")
				return
			}

			for _, txn := range txns {
				select {
				case jobs <- txn:
				case <-ctx.Done():
					errChan <- ctx.Err()
					span.RecordError(ctx.Err())
					return
				}
			}

			offset += int64(len(txns))
		}
	}
}

func (l *Blnk) RefundWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	ctx, span := tracer.Start(ctx, "RefundWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		queuedRefundTxn, err := l.RefundTransaction(ctx, originalTxn.TransactionID)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
		}
		results <- BatchJobResult{Txn: queuedRefundTxn}
		span.AddEvent("Refund processed", trace.WithAttributes(attribute.String("transaction.id", queuedRefundTxn.TransactionID)))
	}
}

func (l *Blnk) CommitWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	ctx, span := tracer.Start(ctx, "CommitWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		if originalTxn.Status != StatusInflight {
			err := fmt.Errorf("transaction is not in inflight status")
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
		}
		queuedCommitTxn, err := l.CommitInflightTransaction(ctx, originalTxn.TransactionID, amount)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
		}
		results <- BatchJobResult{Txn: queuedCommitTxn}
		span.AddEvent("Commit processed", trace.WithAttributes(attribute.String("transaction.id", queuedCommitTxn.TransactionID)))
	}
}

func (l *Blnk) VoidWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	ctx, span := tracer.Start(ctx, "VoidWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		if originalTxn.Status != StatusInflight {
			err := fmt.Errorf("transaction is not in inflight status")
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
		}
		queuedVoidTxn, err := l.VoidInflightTransaction(ctx, originalTxn.TransactionID)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
		}
		results <- BatchJobResult{Txn: queuedVoidTxn}
		span.AddEvent("Void processed", trace.WithAttributes(attribute.String("transaction.id", queuedVoidTxn.TransactionID)))
	}
}

func (l *Blnk) RecordTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RecordTransaction")
	defer span.End()

	return l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		sourceBalance, destinationBalance, err := l.validateAndPrepareTransaction(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		if err := l.processBalances(ctx, transaction, sourceBalance, destinationBalance); err != nil {
			span.RecordError(err)
			return nil, err
		}

		transaction, err = l.finalizeTransaction(ctx, transaction, sourceBalance, destinationBalance)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		l.postTransactionActions(ctx, transaction)

		span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
		return transaction, nil
	})
}

func (l *Blnk) executeWithLock(ctx context.Context, transaction *model.Transaction, fn func(context.Context) (*model.Transaction, error)) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "ExecuteWithLock")
	defer span.End()

	locker, err := l.acquireLock(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	return fn(ctx)
}

func (l *Blnk) validateAndPrepareTransaction(ctx context.Context, transaction *model.Transaction) (*model.Balance, *model.Balance, error) {
	ctx, span := tracer.Start(ctx, "ValidateAndPrepareTransaction")
	defer span.End()

	if err := l.validateTxn(ctx, transaction); err != nil {
		span.RecordError(err)
		return nil, nil, l.logAndRecordError(span, "transaction validation failed", err)
	}

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, nil, l.logAndRecordError(span, "failed to get source and destination balances", err)
	}

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID

	span.AddEvent("Transaction validated and prepared", trace.WithAttributes(attribute.String("source.balance_id", sourceBalance.BalanceID), attribute.String("destination.balance_id", destinationBalance.BalanceID)))

	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) processBalances(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessBalances")
	defer span.End()

	if err := l.applyTransactionToBalances(ctx, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "failed to apply transaction to balances", err)
	}

	if err := l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "failed to update balances", err)
	}

	span.AddEvent("Balances processed")
	return nil
}

func (l *Blnk) finalizeTransaction(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeTransaction")
	defer span.End()

	transaction = l.updateTransactionDetails(ctx, transaction, sourceBalance, destinationBalance)

	transaction, err := l.persistTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "failed to persist transaction", err)
	}

	span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	return transaction, nil
}

func (l *Blnk) releaseLock(ctx context.Context, locker *redlock.Locker) {
	ctx, span := tracer.Start(ctx, "ReleaseLock")
	defer span.End()

	if err := locker.Unlock(ctx); err != nil {
		span.RecordError(err)
		logrus.Error("failed to release lock", err)
	}
	span.AddEvent("Lock released")
}

func (l *Blnk) logAndRecordError(span trace.Span, msg string, err error) error {
	span.RecordError(err)
	logrus.Error(msg, err)
	return fmt.Errorf("%s: %w", msg, err)
}

func (l *Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RejectTransaction")
	defer span.End()

	transaction.Status = StatusRejected
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}
	transaction.MetaData["blnk_rejection_reason"] = reason

	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		logrus.Errorf("ERROR saving transaction to db. %s", err)
	}

	span.AddEvent("Transaction rejected", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	return transaction, nil
}

func (l *Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount float64) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "CommitInflightTransaction")
	defer span.End()

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
	return l.finalizeCommitment(ctx, transaction)
}

func (l *Blnk) validateAndUpdateAmount(ctx context.Context, transaction *model.Transaction, amount float64) error {
	ctx, span := tracer.Start(ctx, "ValidateAndUpdateAmount")
	defer span.End()

	committedAmount, err := l.datasource.GetTotalCommittedTransactions(ctx, transaction.TransactionID)
	if err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "error fetching committed amount", err)
	}

	originalAmount := transaction.PreciseAmount
	amountLeft := originalAmount - committedAmount

	if amount != 0 {
		transaction.Amount = amount
		transaction.PreciseAmount = 0
	} else {
		transaction.Amount = float64(amountLeft) / transaction.Precision
	}

	if amountLeft < model.ApplyPrecision(transaction) {
		err := fmt.Errorf("can not commit %s %.2f. You can only commit an amount between 1.00 - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(amountLeft)/transaction.Precision)
		span.RecordError(err)
		return err
	} else if amountLeft == 0 {
		err := fmt.Errorf("can not commit %s %.2f. Transaction already committed with amount of - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(committedAmount)/transaction.Precision)
		span.RecordError(err)
		return err
	}

	span.AddEvent("Amount validated and updated", trace.WithAttributes(attribute.Float64("amount.left", float64(amountLeft)/transaction.Precision)))
	return nil
}

func (l *Blnk) finalizeCommitment(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeCommitment")
	defer span.End()

	transaction.Status = StatusCommit
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()
	transaction, err := l.QueueTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	span.AddEvent("Commitment finalized", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

func (l *Blnk) VoidInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "VoidInflightTransaction")
	defer span.End()

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

	span.AddEvent("Inflight transaction voided", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return l.finalizeVoidTransaction(ctx, transaction, amountLeft)
}

func (l *Blnk) fetchAndValidateInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FetchAndValidateInflightTransaction")
	defer span.End()

	var transaction *model.Transaction
	dbTransaction, err := l.datasource.GetTransaction(ctx, transactionID)
	if err == sql.ErrNoRows {
		queuedTxn, err := l.queue.GetTransactionFromQueue(transactionID)
		log.Println("found inflight transaction in queue using it for commit/void", transactionID, queuedTxn.TransactionID)
		if err != nil {
			span.RecordError(err)
			return &model.Transaction{}, err
		}
		if queuedTxn == nil {
			err := fmt.Errorf("transaction not found")
			span.RecordError(err)
			return nil, err
		}
		transaction = queuedTxn
	} else if err == nil {
		transaction = dbTransaction
	} else {
		span.RecordError(err)
		return &model.Transaction{}, err
	}

	if transaction.Status != StatusInflight {
		err := fmt.Errorf("transaction is not in inflight status")
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "invalid transaction status", err)
	}

	parentVoided, err := l.datasource.IsParentTransactionVoid(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "error checking parent transaction status", err)
	}

	if parentVoided {
		err := fmt.Errorf("transaction has already been voided")
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "Error voiding transaction", err)
	}

	span.AddEvent("Inflight transaction validated", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

func (l *Blnk) calculateRemainingAmount(ctx context.Context, transaction *model.Transaction) (int64, error) {
	ctx, span := tracer.Start(ctx, "CalculateRemainingAmount")
	defer span.End()

	committedAmount, err := l.datasource.GetTotalCommittedTransactions(ctx, transaction.TransactionID)
	if err != nil {
		span.RecordError(err)
		return 0, l.logAndRecordError(span, "error fetching committed amount", err)
	}

	span.AddEvent("Remaining amount calculated", trace.WithAttributes(attribute.Int64("amount.remaining", transaction.PreciseAmount-committedAmount)))
	return transaction.PreciseAmount - committedAmount, nil
}

func (l *Blnk) finalizeVoidTransaction(ctx context.Context, transaction *model.Transaction, amountLeft int64) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeVoidTransaction")
	defer span.End()

	transaction.Status = StatusVoid
	transaction.Amount = float64(amountLeft) / transaction.Precision
	transaction.PreciseAmount = amountLeft
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()
	transaction, err := l.QueueTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	span.AddEvent("Void transaction finalized", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

func (l *Blnk) QueueTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "QueueTransaction")
	defer span.End()

	// Set transaction status and metadata
	span.AddEvent("Setting transaction status and metadata")
	setTransactionStatus(transaction)
	setTransactionMetadata(transaction)

	// Attempt to split the transaction if needed
	transactions, err := transaction.SplitTransaction(ctx)
	if err != nil {
		span.RecordError(err)
		span.AddEvent("Transaction split failed", trace.WithAttributes(
			attribute.String("transaction.id", transaction.TransactionID),
		))
		return nil, err
	}

	// Enqueue the transaction(s)
	if len(transactions) == 0 {
		transactions = []*model.Transaction{transaction}
	}

	span.AddEvent("Enqueuing transactions", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
		attribute.String("transaction.id", transaction.TransactionID),
	))

	if err := enqueueTransactions(ctx, l.queue, transaction, transactions); err != nil {
		span.RecordError(err)
		span.AddEvent("Failed to enqueue transactions", trace.WithAttributes(
			attribute.String("transaction.id", transaction.TransactionID),
		))
		return nil, err
	}

	span.SetAttributes(attribute.String("transaction.id", transaction.TransactionID))
	span.AddEvent("Transaction successfully queued", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
	))

	return transaction, nil
}

func setTransactionStatus(transaction *model.Transaction) {
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	} else if transaction.Inflight {
		transaction.Status = StatusInflight
	} else if transaction.Status == "" {
		transaction.Status = StatusQueued
	}
}

func setTransactionMetadata(transaction *model.Transaction) {
	transaction.SkipBalanceUpdate = true
	transaction.CreatedAt = time.Now()
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Hash = transaction.HashTxn()
	transaction.PreciseAmount = int64(transaction.Amount * transaction.Precision)
}

func enqueueTransactions(ctx context.Context, queue *Queue, originalTransaction *model.Transaction, splitTransactions []*model.Transaction) error {
	transactionsToEnqueue := splitTransactions
	if len(transactionsToEnqueue) == 0 {
		transactionsToEnqueue = []*model.Transaction{originalTransaction}
	}

	for _, txn := range transactionsToEnqueue {
		if err := queue.Enqueue(ctx, txn); err != nil {
			notification.NotifyError(err)
			logrus.Errorf("Error queuing transaction: %v", err)
			return err
		}
	}

	return nil
}

func (l *Blnk) GetTransaction(ctx context.Context, TransactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetTransaction")
	defer span.End()

	transaction, err := l.datasource.GetTransaction(ctx, TransactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Transaction retrieved", trace.WithAttributes(attribute.String("transaction.id", TransactionID)))
	return transaction, nil
}

func (l *Blnk) GetAllTransactions() ([]model.Transaction, error) {
	ctx, span := tracer.Start(context.Background(), "GetAllTransactions")
	defer span.End()

	transactions, err := l.datasource.GetAllTransactions(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("All transactions retrieved")
	return transactions, nil
}

func (l *Blnk) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetTransactionByRef")
	defer span.End()

	transaction, err := l.datasource.GetTransactionByRef(ctx, reference)
	if err != nil {
		span.RecordError(err)
		return model.Transaction{}, err
	}

	span.AddEvent("Transaction retrieved by reference", trace.WithAttributes(attribute.String("transaction.reference", reference)))
	return transaction, nil
}

func (l *Blnk) UpdateTransactionStatus(ctx context.Context, id string, status string) error {
	ctx, span := tracer.Start(ctx, "UpdateTransactionStatus")
	defer span.End()

	err := l.datasource.UpdateTransactionStatus(ctx, id, status)
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Transaction status updated", trace.WithAttributes(attribute.String("transaction.id", id), attribute.String("transaction.status", status)))
	return nil
}

func (l *Blnk) RefundTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RefundTransaction")
	defer span.End()

	originalTxn, err := l.GetTransaction(ctx, transactionID)
	if err != nil {
		// Check if the error is due to no row found
		if err == sql.ErrNoRows {
			// Check the queue for the transaction
			queuedTxn, err := l.queue.GetTransactionFromQueue(transactionID)
			log.Println("found transaction in queue using it for refund", transactionID, queuedTxn.TransactionID)
			if err != nil {
				span.RecordError(err)
				return &model.Transaction{}, err
			}
			if queuedTxn == nil {
				err := fmt.Errorf("transaction not found")
				span.RecordError(err)
				return nil, err
			}
			originalTxn = queuedTxn
		} else {
			span.RecordError(err)
			return &model.Transaction{}, err
		}
	}

	if originalTxn.Status == StatusRejected {
		err := fmt.Errorf("transaction is not in a state that can be refunded")
		span.RecordError(err)
		return nil, err
	}

	if originalTxn.Status == StatusVoid {
		originalTxn.Inflight = true
	} else {
		originalTxn.Status = ""
	}

	newTransaction := *originalTxn
	newTransaction.Reference = model.GenerateUUIDWithSuffix("ref")
	newTransaction.ParentTransaction = originalTxn.TransactionID
	newTransaction.Source = originalTxn.Destination
	newTransaction.Destination = originalTxn.Source
	newTransaction.AllowOverdraft = true
	refundTxn, err := l.QueueTransaction(ctx, &newTransaction)
	if err != nil {
		span.RecordError(err)
		return &model.Transaction{}, err
	}

	span.AddEvent("Transaction refunded", trace.WithAttributes(attribute.String("transaction.id", refundTxn.TransactionID)))
	return refundTxn, nil
}
