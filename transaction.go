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

// getTxns is a function type that retrieves a batch of transactions based on the parent transaction ID, batch size, and offset.
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
type getTxns func(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)

// transactionWorker is a function type that processes transactions from a job channel and sends the results to a results channel.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount float64: The amount to be processed in the transaction.
type transactionWorker func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64)

// BatchJobResult represents the result of processing a transaction in a batch job.
//
// Fields:
// - Txn *model.Transaction: A pointer to the processed Transaction model.
// - Error error: An error if the transaction could not be processed.
type BatchJobResult struct {
	Txn   *model.Transaction
	Error error
}

// getSourceAndDestination retrieves the source and destination balances for a transaction.
// It checks if the source or destination starts with "@", indicating the need to create or retrieve a balance by indicator.
// If not, it retrieves the balances by their IDs.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to retrieve the balances.
//
// Returns:
// - source *model.Balance: A pointer to the source Balance model.
// - destination *model.Balance: A pointer to the destination Balance model.
// - err error: An error if the balances could not be retrieved.
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

// acquireLock acquires a distributed lock for a transaction to ensure exclusive access to the source balance.
// It starts a tracing span, attempts to acquire the lock, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to acquire the lock.
//
// Returns:
// - *redlock.Locker: A pointer to the acquired Locker if successful.
// - error: An error if the lock could not be acquired.
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

// updateTransactionDetails updates the details of a transaction, including source and destination balances and status.
// It starts a tracing span, creates a new transaction object with updated details, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The original transaction to be updated.
// - sourceBalance *model.Balance: The source balance for the transaction.
// - destinationBalance *model.Balance: The destination balance for the transaction.
//
// Returns:
// - *model.Transaction: A pointer to the new transaction object with updated details.
func (l *Blnk) updateTransactionDetails(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.Transaction {
	_, span := tracer.Start(ctx, "Updating Transaction Details")
	defer span.End()

	// Create a new transaction object with updated details (immutable pattern)
	newTransaction := *transaction // Copy the original transaction
	newTransaction.Source = sourceBalance.BalanceID
	newTransaction.Destination = destinationBalance.BalanceID

	// Update the status based on the current status and inflight flag
	applicableStatus := map[string]string{
		StatusQueued:    StatusApplied,
		StatusScheduled: StatusApplied,
		StatusCommit:    StatusApplied,
		StatusVoid:      StatusVoid,
	}
	newTransaction.Status = applicableStatus[transaction.Status]
	if transaction.Inflight {
		newTransaction.Status = StatusInflight
	}

	span.AddEvent("Transaction details updated")
	return &newTransaction
}

// persistTransaction persists a transaction to the database.
// It starts a tracing span, records the transaction, and handles any errors that occur during the process.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be persisted.
//
// Returns:
// - *model.Transaction: A pointer to the persisted Transaction model.
// - error: An error if the transaction could not be persisted.
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

// postTransactionActions performs post-processing actions for a transaction.
// It starts a tracing span, queues the transaction data for indexing, and sends a webhook notification.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to perform post-processing actions.
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

// updateBalances updates the source and destination balances in the database.
// It starts a tracing span, updates the balances, and performs post-update actions such as checking balance monitors
// and queuing the balances for indexing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - sourceBalance *model.Balance: The source balance to be updated.
// - destinationBalance *model.Balance: The destination balance to be updated.
//
// Returns:
// - error: An error if the balances could not be updated.
func (l *Blnk) updateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "Updating Balances")
	defer span.End()

	var wg sync.WaitGroup

	// Update the balances in the datasource
	if err := l.datasource.UpdateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		span.RecordError(err)
		return err
	}

	// Add two tasks to the wait group
	wg.Add(2)

	// Goroutine to check monitors and queue index data for the source balance
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(ctx, sourceBalance)
		err := l.queue.queueIndexData(sourceBalance.BalanceID, "balances", sourceBalance)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
	}()

	// Goroutine to check monitors and queue index data for the destination balance
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(ctx, destinationBalance)
		err := l.queue.queueIndexData(destinationBalance.BalanceID, "balances", destinationBalance)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
	}()

	// Wait for both goroutines to complete
	wg.Wait()

	span.AddEvent("Balances updated")
	return nil
}

// validateTxn validates a transaction by checking if its reference has already been used.
// It starts a tracing span, checks the existence of the transaction reference, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be validated.
//
// Returns:
// - error: An error if the transaction reference has already been used or if there was an issue checking the reference.
func (l *Blnk) validateTxn(ctx context.Context, transaction *model.Transaction) error {
	ctx, span := tracer.Start(ctx, "Validating Transaction Reference")
	defer span.End()

	// Check if a transaction with the same reference already exists
	txn, err := l.datasource.TransactionExistsByRef(ctx, transaction.Reference)
	if err != nil {
		return err
	}

	// If the transaction reference already exists, return an error
	if txn {
		err := fmt.Errorf("reference %s has already been used", transaction.Reference)
		span.RecordError(err)
		return err
	}

	span.AddEvent("Transaction validated")
	return nil
}

// applyTransactionToBalances applies a transaction to the provided balances.
// It starts a tracing span, calculates new balances, and updates the balances based on the transaction status.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balances []*model.Balance: A slice of Balance models to be updated. The first balance is the source, and the second is the destination.
// - transaction *model.Transaction: The transaction to be applied to the balances.
//
// Returns:
// - error: An error if the balances could not be updated.
func (l *Blnk) applyTransactionToBalances(ctx context.Context, balances []*model.Balance, transaction *model.Transaction) error {
	_, span := tracer.Start(ctx, "Applying Transaction to Balances")
	defer span.End()

	span.AddEvent("Calculating new balances")

	// Handle committed inflight transactions
	if transaction.Status == StatusCommit {
		balances[0].CommitInflightDebit(transaction)
		balances[1].CommitInflightCredit(transaction)
		span.AddEvent("Committed inflight balances")
		return nil
	}

	transactionAmount := new(big.Int).SetInt64(transaction.PreciseAmount)

	// Handle voided transactions
	if transaction.Status == StatusVoid {
		balances[0].RollbackInflightDebit(transactionAmount)
		balances[1].RollbackInflightCredit(transactionAmount)
		span.AddEvent("Rolled back inflight balances")
		return nil
	}

	// Update balances for other transaction statuses
	err := model.UpdateBalances(transaction, balances[0], balances[1])
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Balances updated")
	return nil
}

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

// GetRefundableTransactionsByParentID retrieves refundable transactions by their parent transaction ID.
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
func (l *Blnk) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetRefundableTransactionsByParentID")
	defer span.End()

	transactions, err := l.datasource.GetRefundableTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("parent_transaction_id", parentTransactionID))
	span.AddEvent("Refundable transactions retrieved")
	return transactions, nil
}

// ProcessTransactionInBatches processes transactions in batches or streams them based on the provided mode.
// It starts a tracing span, initializes worker pools, fetches transactions, and processes them concurrently.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The ID of the parent transaction.
// - amount float64: The amount to be processed in the transaction.
// - maxWorkers int: The maximum number of workers to process transactions concurrently.
// - streamMode bool: A flag indicating whether to process transactions in streaming mode.
// - gt getTxns: A function to retrieve transactions in batches.
// - tw transactionWorker: A function to process transactions.
//
// Returns:
// - []*model.Transaction: A slice of pointers to the processed Transaction models.
// - error: An error if the transactions could not be processed.
func (l *Blnk) ProcessTransactionInBatches(ctx context.Context, parentTransactionID string, amount float64, maxWorkers int, streamMode bool, gt getTxns, tw transactionWorker) ([]*model.Transaction, error) {
	// Start a tracing span
	ctx, span := tracer.Start(ctx, "ProcessTransactionInBatches")
	defer span.End()

	// Constants for batch and queue sizes
	const (
		batchSize    = 100000
		maxQueueSize = 1000
	)

	// Slice to collect all processed transactions and errors
	var allTxns []*model.Transaction
	var allErrors []error
	var mu sync.Mutex // Mutex to protect shared resources

	// Create channels for jobs and results
	jobs := make(chan *model.Transaction, maxQueueSize)
	results := make(chan BatchJobResult, maxQueueSize)

	// Initialize worker pool
	var wg sync.WaitGroup
	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go tw(ctx, jobs, results, &wg, amount)
	}

	// Ensure the results channel is closed once all workers are done
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

// processResults processes the results from the results channel, collecting transactions and errors.
// It locks access to shared data to ensure thread safety and signals completion when done.
//
// Parameters:
// - results chan BatchJobResult: The channel from which to receive results.
// - mu *sync.Mutex: A mutex to synchronize access to shared data.
// - allTxns *[]*model.Transaction: A slice to collect all processed transactions.
// - allErrors *[]error: A slice to collect all errors encountered during processing.
// - done chan struct{}: A channel to signal when processing is complete.
func processResults(results chan BatchJobResult, mu *sync.Mutex, allTxns *[]*model.Transaction, allErrors *[]error, done chan struct{}) {
	for result := range results {
		mu.Lock()
		if result.Error != nil {
			// Log any error encountered during transaction processing
			log.Printf("Error processing transaction: %v", result.Error)
			*allErrors = append(*allErrors, result.Error)
		} else if result.Txn != nil {
			*allTxns = append(*allTxns, result.Txn)
		} else {
			// Handle the case where the result contains no transaction and no error
			log.Printf("Received a result with no transaction and no error")
		}
		mu.Unlock()
	}
	close(done) // Signal completion of processing.
}

// fetchTransactions fetches transactions in batches and sends them to the jobs channel.
// It starts a tracing span, iterates through the transactions, and handles context cancellation and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The ID of the parent transaction.
// - batchSize int: The number of transactions to retrieve in a batch.
// - gt getTxns: A function to retrieve transactions in batches.
// - jobs chan *model.Transaction: The channel to send fetched transactions to.
// - errChan chan error: The channel to send errors to.
func fetchTransactions(ctx context.Context, parentTransactionID string, batchSize int, gt getTxns, jobs chan *model.Transaction, errChan chan error) {
	newCtx, span := tracer.Start(ctx, "FetchTransactions")
	defer span.End()
	defer close(jobs) // Ensure the jobs channel is closed in all cases to avoid deadlocks

	var offset int64 = 0
	for {
		select {
		case <-ctx.Done():
			// Handle context cancellation gracefully by sending the error and returning
			err := ctx.Err()
			if err != nil {
				errChan <- err
				span.RecordError(err)
			}
			return
		default:
			// Fetch the transactions in batches
			txns, err := gt(newCtx, parentTransactionID, batchSize, offset)
			if err != nil {
				// Log and send error if fetching transactions fails
				log.Printf("Error fetching transactions: %v", err)
				errChan <- err
				span.RecordError(err)
				return
			}
			if len(txns) == 0 {
				// Stop if no more transactions are found
				span.AddEvent("No more transactions to fetch")
				return
			}

			// Send fetched transactions to the jobs channel
			for _, txn := range txns {
				select {
				case jobs <- txn: // Send the transaction to be processed
				case <-ctx.Done():
					// If context is canceled, handle gracefully
					err := ctx.Err()
					if err != nil {
						errChan <- err
						span.RecordError(err)
					}
					return
				}
			}

			// Increment offset to fetch the next batch
			offset += int64(len(txns))
		}
	}
}

// RefundWorker processes refund transactions from the jobs channel and sends the results to the results channel.
// It starts a tracing span, processes each transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount float64: The amount to be processed in the transaction.
func (l *Blnk) RefundWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	ctx, span := tracer.Start(ctx, "RefundWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		queuedRefundTxn, err := l.RefundTransaction(ctx, originalTxn.TransactionID)
		if err != nil {
			results <- BatchJobResult{Error: err}
			span.RecordError(err)
			continue
		}
		results <- BatchJobResult{Txn: queuedRefundTxn}
		span.AddEvent("Refund processed", trace.WithAttributes(attribute.String("transaction.id", queuedRefundTxn.TransactionID)))
	}
}

// CommitWorker processes commit transactions from the jobs channel and sends the results to the results channel.
// It starts a tracing span, processes each transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount float64: The amount to be processed in the transaction.
func (l *Blnk) CommitWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	ctx, span := tracer.Start(ctx, "CommitWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		if originalTxn.Status != StatusInflight {
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
// - amount float64: The amount to be processed in the transaction (not used in this function).
func (l *Blnk) VoidWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	ctx, span := tracer.Start(ctx, "VoidWorker")
	defer span.End()

	defer wg.Done()
	for originalTxn := range jobs {
		if originalTxn.Status != StatusInflight {
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

// RecordTransaction records a transaction by validating, processing balances, and finalizing the transaction.
// It starts a tracing span, acquires a lock, and performs the necessary steps to record the transaction.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be recorded.
//
// Returns:
// - *model.Transaction: A pointer to the recorded Transaction model.
// - error: An error if the transaction could not be recorded.
func (l *Blnk) RecordTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RecordTransaction")
	defer span.End()

	return l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		// Validate and prepare the transaction, including retrieving source and destination balances
		transaction, sourceBalance, destinationBalance, err := l.validateAndPrepareTransaction(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Process the balances by applying the transaction
		if err := l.processBalances(ctx, transaction, sourceBalance, destinationBalance); err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Finalize the transaction by persisting it and updating the balances
		transaction, err = l.finalizeTransaction(ctx, transaction, sourceBalance, destinationBalance)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Perform post-transaction actions such as indexing and sending webhooks
		l.postTransactionActions(ctx, transaction)

		span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
		return transaction, nil
	})
}

// executeWithLock executes a function with a distributed lock to ensure exclusive access to the transaction.
// It starts a tracing span, acquires the lock, executes the provided function, and releases the lock.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to acquire the lock.
// - fn func(context.Context) (*model.Transaction, error): The function to execute with the lock.
//
// Returns:
// - *model.Transaction: A pointer to the Transaction model returned by the function.
// - error: An error if the lock could not be acquired or if the function execution fails.
func (l *Blnk) executeWithLock(ctx context.Context, transaction *model.Transaction, fn func(context.Context) (*model.Transaction, error)) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "ExecuteWithLock")
	defer span.End()

	// Acquire a distributed lock for the transaction
	locker, err := l.acquireLock(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	// Execute the provided function with the lock
	return fn(ctx)
}

// validateAndPrepareTransaction validates the transaction and prepares it by retrieving the source and destination balances.
// It starts a tracing span, validates the transaction, retrieves the balances, and updates the transaction with the balance IDs.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be validated and prepared.
//
// Returns:
// - *model.Transaction: A pointer to the new transaction object with updated details.
// - *model.Balance: A pointer to the source Balance model.
// - *model.Balance: A pointer to the destination Balance model.
// - error: An error if the transaction validation or balance retrieval fails.
func (l *Blnk) validateAndPrepareTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, *model.Balance, *model.Balance, error) {
	ctx, span := tracer.Start(ctx, "ValidateAndPrepareTransaction")
	defer span.End()

	// Validate the transaction
	if err := l.validateTxn(ctx, transaction); err != nil {
		span.RecordError(err)
		return nil, nil, nil, l.logAndRecordError(span, "transaction validation failed", err)
	}

	// Retrieve the source and destination balances
	sourceBalance, destinationBalance, err := l.getSourceAndDestination(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, nil, nil, l.logAndRecordError(span, "failed to get source and destination balances", err)
	}

	// Create a copy of the transaction and update it (immutable)
	newTransaction := *transaction // Copy the original transaction
	newTransaction.Source = sourceBalance.BalanceID
	newTransaction.Destination = destinationBalance.BalanceID

	span.AddEvent("Transaction validated and prepared", trace.WithAttributes(
		attribute.String("source.balance_id", sourceBalance.BalanceID),
		attribute.String("destination.balance_id", destinationBalance.BalanceID)))

	// Return the new transaction, source, and destination balances
	return &newTransaction, sourceBalance, destinationBalance, nil
}

// processBalances processes the source and destination balances by applying the transaction and updating the balances.
// It starts a tracing span, applies the transaction to the balances, updates the balances, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be applied to the balances.
// - sourceBalance *model.Balance: The source balance to be updated.
// - destinationBalance *model.Balance: The destination balance to be updated.
//
// Returns:
// - error: An error if the transaction could not be applied to the balances or if the balances could not be updated.
func (l *Blnk) processBalances(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessBalances")
	defer span.End()

	// Apply the transaction to the source and destination balances
	if err := l.applyTransactionToBalances(ctx, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "failed to apply transaction to balances", err)
	}

	// Update the source and destination balances in the datasource
	if err := l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "failed to update balances", err)
	}

	span.AddEvent("Balances processed")
	return nil
}

// finalizeTransaction finalizes the transaction by updating its details and persisting it to the database.
// It starts a tracing span, updates the transaction details, persists the transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be finalized.
// - sourceBalance *model.Balance: The source balance associated with the transaction.
// - destinationBalance *model.Balance: The destination balance associated with the transaction.
//
// Returns:
// - *model.Transaction: A pointer to the finalized Transaction model.
// - error: An error if the transaction could not be persisted.
func (l *Blnk) finalizeTransaction(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeTransaction")
	defer span.End()

	// Update the transaction details with the source and destination balances
	transaction = l.updateTransactionDetails(ctx, transaction, sourceBalance, destinationBalance)

	// Persist the transaction to the database
	transaction, err := l.persistTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "failed to persist transaction", err)
	}

	span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	return transaction, nil
}

// releaseLock releases the distributed lock acquired for a transaction.
// It starts a tracing span, attempts to release the lock, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - locker *redlock.Locker: The locker object representing the acquired lock.
func (l *Blnk) releaseLock(ctx context.Context, locker *redlock.Locker) {
	ctx, span := tracer.Start(ctx, "ReleaseLock")
	defer span.End()

	// Attempt to release the lock
	if err := locker.Unlock(ctx); err != nil {
		span.RecordError(err)
		logrus.Error("failed to release lock", err)
	}
	span.AddEvent("Lock released")
}

// logAndRecordError logs an error message and records the error in the tracing span.
// It returns a formatted error message combining the provided message and the original error.
//
// Parameters:
// - span trace.Span: The tracing span to record the error.
// - msg string: The error message to log and include in the formatted error.
// - err error: The original error to be logged and recorded.
//
// Returns:
// - error: A formatted error message combining the provided message and the original error.
func (l *Blnk) logAndRecordError(span trace.Span, msg string, err error) error {
	span.RecordError(err)
	logrus.Error(msg, err)
	return fmt.Errorf("%s: %w", msg, err)
}

// RejectTransaction rejects a transaction by updating its status and recording the rejection reason.
// It starts a tracing span, updates the transaction status and metadata, persists the transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be rejected.
// - reason string: The reason for rejecting the transaction.
//
// Returns:
// - *model.Transaction: A pointer to the rejected Transaction model.
// - error: An error if the transaction could not be recorded.
func (l *Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RejectTransaction")
	defer span.End()

	// Update the transaction status to rejected
	transaction.Status = StatusRejected

	// Initialize MetaData if it's nil and add the rejection reason
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}
	transaction.MetaData["blnk_rejection_reason"] = reason

	// Persist the transaction with the updated status and metadata
	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		logrus.Errorf("ERROR saving transaction to db. %s", err)
		return nil, err
	}

	span.AddEvent("Transaction rejected", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	return transaction, nil
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
func (l *Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount float64) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "CommitInflightTransaction")
	defer span.End()

	// Fetch and validate the inflight transaction
	transaction, err := l.fetchAndValidateInflightTransaction(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Validate and update the transaction amount
	if err := l.validateAndUpdateAmount(ctx, transaction, amount); err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Inflight transaction committed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	// Finalize the commitment of the transaction
	return l.finalizeCommitment(ctx, transaction)
}

// validateAndUpdateAmount validates the amount to be committed for a transaction and updates the transaction's amount.
// It starts a tracing span, fetches the total committed amount, calculates the remaining amount, and updates the transaction.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be validated and updated.
// - amount float64: The amount to be validated and updated in the transaction.
//
// Returns:
// - error: An error if the amount validation or update fails.
func (l *Blnk) validateAndUpdateAmount(ctx context.Context, transaction *model.Transaction, amount float64) error {
	ctx, span := tracer.Start(ctx, "ValidateAndUpdateAmount")
	defer span.End()

	// Fetch the total committed amount for the transaction
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(ctx, transaction.TransactionID)
	if err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "error fetching committed amount", err)
	}

	originalAmount := transaction.PreciseAmount
	amountLeft := originalAmount - committedAmount

	// Update the transaction amount based on the provided amount
	if amount != 0 {
		transaction.Amount = amount
		transaction.PreciseAmount = 0
	} else {
		transaction.Amount = float64(amountLeft) / transaction.Precision
	}

	// Validate the remaining amount
	if amountLeft < model.ApplyPrecision(transaction) {
		err := fmt.Errorf("cannot commit %s %.2f. You can only commit an amount between 1.00 - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(amountLeft)/transaction.Precision)
		span.RecordError(err)
		return err
	} else if amountLeft == 0 {
		err := fmt.Errorf("cannot commit %s %.2f. Transaction already committed with amount of - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(committedAmount)/transaction.Precision)
		span.RecordError(err)
		return err
	}

	span.AddEvent("Amount validated and updated", trace.WithAttributes(attribute.Float64("amount.left", float64(amountLeft)/transaction.Precision)))
	return nil
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
func (l *Blnk) finalizeCommitment(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeCommitment")
	defer span.End()

	// Update the transaction status to committed and generate new identifiers
	transaction.Status = StatusCommit
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()

	// Queue the transaction for further processing
	transaction, err := l.QueueTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

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

	// Fetch and validate the inflight transaction
	transaction, err := l.fetchAndValidateInflightTransaction(ctx, transactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Calculate the remaining amount for the transaction
	amountLeft, err := l.calculateRemainingAmount(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Inflight transaction voided", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	// Finalize the void transaction
	return l.finalizeVoidTransaction(ctx, transaction, amountLeft)
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

	// Attempt to retrieve the transaction from the database
	dbTransaction, err := l.datasource.GetTransaction(ctx, transactionID)
	if err == sql.ErrNoRows {
		// If not found in the database, attempt to retrieve from the queue
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

	// Validate the transaction status
	if transaction.Status != StatusInflight {
		err := fmt.Errorf("transaction is not in inflight status")
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "invalid transaction status", err)
	}

	// Check if the parent transaction has been voided
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
func (l *Blnk) calculateRemainingAmount(ctx context.Context, transaction *model.Transaction) (int64, error) {
	ctx, span := tracer.Start(ctx, "CalculateRemainingAmount")
	defer span.End()

	// Fetch the total committed amount for the transaction
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(ctx, transaction.TransactionID)
	if err != nil {
		span.RecordError(err)
		return 0, l.logAndRecordError(span, "error fetching committed amount", err)
	}

	// Calculate the remaining amount
	remainingAmount := transaction.PreciseAmount - committedAmount

	span.AddEvent("Remaining amount calculated", trace.WithAttributes(attribute.Int64("amount.remaining", remainingAmount)))
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
func (l *Blnk) finalizeVoidTransaction(ctx context.Context, transaction *model.Transaction, amountLeft int64) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "FinalizeVoidTransaction")
	defer span.End()

	// Update the transaction status to void and set the remaining amount
	transaction.Status = StatusVoid
	transaction.Amount = float64(amountLeft) / transaction.Precision
	transaction.PreciseAmount = amountLeft
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()

	// Queue the transaction for further processing
	transaction, err := l.QueueTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	span.AddEvent("Void transaction finalized", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

// QueueTransaction queues a transaction by setting its status and metadata, attempting to split it if needed, and enqueuing it.
// It starts a tracing span, sets the transaction status and metadata, splits the transaction if necessary, and enqueues it.
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

// setTransactionStatus sets the status of a transaction based on its scheduled time, inflight status, and current status.
// If the transaction is scheduled for a future time, it sets the status to scheduled.
// If the transaction is inflight, it sets the status to inflight.
// If the transaction status is empty, it sets the status to queued.
//
// Parameters:
// - transaction *model.Transaction: The transaction for which to set the status.
func setTransactionStatus(transaction *model.Transaction) {
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	} else if transaction.Inflight {
		transaction.Status = StatusInflight
	} else if transaction.Status == "" {
		transaction.Status = StatusQueued
	}
}

// setTransactionMetadata sets the metadata for a transaction, including skipping balance updates, setting creation time,
// generating a new transaction ID, hashing the transaction, and calculating the precise amount.
//
// Parameters:
// - transaction *model.Transaction: The transaction for which to set the metadata.
func setTransactionMetadata(transaction *model.Transaction) {
	transaction.SkipBalanceUpdate = true
	transaction.CreatedAt = time.Now()
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Hash = transaction.HashTxn()
	transaction.PreciseAmount = int64(transaction.Amount * transaction.Precision)
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
			logrus.Errorf("Error queuing transaction: %v", err)
			return err
		}
	}

	return nil
}

// GetTransaction retrieves a transaction by its ID from the datasource.
// It starts a tracing span, fetches the transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - TransactionID string: The ID of the transaction to be retrieved.
//
// Returns:
// - *model.Transaction: A pointer to the retrieved Transaction model.
// - error: An error if the transaction could not be retrieved.
func (l *Blnk) GetTransaction(ctx context.Context, TransactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetTransaction")
	defer span.End()

	// Fetch the transaction from the datasource
	transaction, err := l.datasource.GetTransaction(ctx, TransactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Transaction retrieved", trace.WithAttributes(attribute.String("transaction.id", TransactionID)))
	return transaction, nil
}

// GetAllTransactions retrieves all transactions from the datasource.
// It starts a tracing span, fetches all transactions, and records relevant events and errors.
//
// Returns:
// - []model.Transaction: A slice of all retrieved Transaction models.
// - error: An error if the transactions could not be retrieved.
func (l *Blnk) GetAllTransactions(limit, offset int) ([]model.Transaction, error) {
	ctx, span := tracer.Start(context.Background(), "GetAllTransactions")
	defer span.End()

	// Fetch all transactions from the datasource
	transactions, err := l.datasource.GetAllTransactions(ctx, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("All transactions retrieved")
	return transactions, nil
}

// GetTransactionByRef retrieves a transaction by its reference from the datasource.
// It starts a tracing span, fetches the transaction by reference, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - reference string: The reference of the transaction to be retrieved.
//
// Returns:
// - model.Transaction: The retrieved Transaction model.
// - error: An error if the transaction could not be retrieved.
func (l *Blnk) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetTransactionByRef")
	defer span.End()

	// Fetch the transaction by reference from the datasource
	transaction, err := l.datasource.GetTransactionByRef(ctx, reference)
	if err != nil {
		span.RecordError(err)
		return model.Transaction{}, err
	}

	span.AddEvent("Transaction retrieved by reference", trace.WithAttributes(attribute.String("transaction.reference", reference)))
	return transaction, nil
}

// UpdateTransactionStatus updates the status of a transaction by its ID in the datasource.
// It starts a tracing span, updates the transaction status, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the transaction to be updated.
// - status string: The new status to be set for the transaction.
//
// Returns:
// - error: An error if the transaction status could not be updated.
func (l *Blnk) UpdateTransactionStatus(ctx context.Context, id string, status string) error {
	ctx, span := tracer.Start(ctx, "UpdateTransactionStatus")
	defer span.End()

	// Update the transaction status in the datasource
	err := l.datasource.UpdateTransactionStatus(ctx, id, status)
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Transaction status updated", trace.WithAttributes(attribute.String("transaction.id", id), attribute.String("transaction.status", status)))
	return nil
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
func (l *Blnk) RefundTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RefundTransaction")
	defer span.End()

	// Retrieve the original transaction
	originalTxn, err := l.GetTransaction(ctx, transactionID)
	if err != nil {
		// Check if the error is due to no row found
		if strings.Contains(err.Error(), fmt.Sprintf("Transaction with ID '%s' not found", transactionID)) {
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

	// Validate the transaction status
	if originalTxn.Status == StatusRejected {
		err := fmt.Errorf("transaction is not in a state that can be refunded")
		span.RecordError(err)
		return nil, err
	}

	// Update the transaction status for refund processing
	if originalTxn.Status == StatusVoid {
		originalTxn.Inflight = true
	} else {
		originalTxn.Status = ""
	}

	// Create a new refund transaction
	newTransaction := *originalTxn
	newTransaction.Reference = model.GenerateUUIDWithSuffix("ref")
	newTransaction.ParentTransaction = originalTxn.TransactionID
	newTransaction.Source = originalTxn.Destination
	newTransaction.Destination = originalTxn.Source
	newTransaction.AllowOverdraft = true

	// Queue the refund transaction
	refundTxn, err := l.QueueTransaction(ctx, &newTransaction)
	if err != nil {
		span.RecordError(err)
		return &model.Transaction{}, err
	}

	span.AddEvent("Transaction refunded", trace.WithAttributes(attribute.String("transaction.id", refundTxn.TransactionID)))
	return refundTxn, nil
}
