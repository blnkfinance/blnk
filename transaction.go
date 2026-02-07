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
	"strings"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/filter"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/internal/search"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	"github.com/blnkfinance/blnk/model"
)

var tracer = otel.Tracer("blnk.transactions")

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
	StatusInflight  = "INFLIGHT"
	StatusVoid      = "VOID"
	StatusCommit    = "COMMIT"
	StatusRejected  = "REJECTED"
)

var asyncBulkSemaphore = semaphore.NewWeighted(100) // max 100 concurrent

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
// - amount *big.Int: The amount to be processed in the transaction.
type transactionWorker func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int)

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
// If not, it retrieves the balances by their IDs. When EnableQueuedChecks is enabled in the transaction config,
// it will use GetBalanceByID with queued balances included instead of GetBalanceByIDLite.
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

	cfg := l.config

	// Check if Source starts with "@"
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err = l.getOrCreateBalanceByIndicator(ctx, transaction.Source, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("source balance lookup failed")
			return nil, nil, err
		}
		// Update transaction source with the balance ID
		transaction.Source = sourceBalance.BalanceID
		span.SetAttributes(attribute.String("source.balance_id", sourceBalance.BalanceID))
	} else {
		// Use GetBalanceByID with queued checks if enabled, otherwise use lite version
		if cfg.Transaction.EnableQueuedChecks {
			sourceBalance, err = l.datasource.GetBalanceByID(transaction.Source, []string{}, true)
		} else {
			sourceBalance, err = l.datasource.GetBalanceByIDLite(transaction.Source)
		}
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("source balance lookup failed")
			return nil, nil, err
		}
	}

	// Check if Destination starts with "@"
	if strings.HasPrefix(transaction.Destination, "@") {
		destinationBalance, err = l.getOrCreateBalanceByIndicator(ctx, transaction.Destination, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("destination balance lookup failed")
			return nil, nil, err
		}
		// Update transaction destination with the balance ID
		transaction.Destination = destinationBalance.BalanceID
		span.SetAttributes(attribute.String("destination.balance_id", destinationBalance.BalanceID))
	} else {
		// Use GetBalanceByID with queued checks if enabled, otherwise use lite version
		if cfg.Transaction.EnableQueuedChecks {
			destinationBalance, err = l.datasource.GetBalanceByID(transaction.Destination, []string{}, true)
		} else {
			destinationBalance, err = l.datasource.GetBalanceByIDLite(transaction.Destination)
		}
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("destination balance lookup failed")
			return nil, nil, err
		}
	}
	span.AddEvent("Retrieved source and destination balances")
	return sourceBalance, destinationBalance, nil
}

// resolveBalanceIDs resolves source and destination to actual balance IDs.
// If the source or destination starts with "@", it indicates a balance indicator (like @world)
// that needs to be resolved to an actual balance ID. This function creates the balance if needed.
// This should be called BEFORE acquiring locks to ensure we lock on the correct balance IDs.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction containing source and destination.
//
// Returns:
// - sourceBalanceID string: The resolved source balance ID.
// - destinationBalanceID string: The resolved destination balance ID.
// - error: An error if the balance IDs could not be resolved.
func (l *Blnk) resolveBalanceIDs(ctx context.Context, transaction *model.Transaction) (string, string, error) {
	ctx, span := tracer.Start(ctx, "ResolveBalanceIDs")
	defer span.End()

	sourceID := transaction.Source
	destID := transaction.Destination

	// Resolve source if it's an indicator (starts with "@")
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err := l.getOrCreateBalanceByIndicator(ctx, transaction.Source, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			return "", "", fmt.Errorf("failed to resolve source balance indicator: %w", err)
		}
		sourceID = sourceBalance.BalanceID
		span.SetAttributes(attribute.String("source.resolved_id", sourceID))
	}

	// Resolve destination if it's an indicator (starts with "@")
	if strings.HasPrefix(transaction.Destination, "@") {
		destBalance, err := l.getOrCreateBalanceByIndicator(ctx, transaction.Destination, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			return "", "", fmt.Errorf("failed to resolve destination balance indicator: %w", err)
		}
		destID = destBalance.BalanceID
		span.SetAttributes(attribute.String("destination.resolved_id", destID))
	}

	span.AddEvent("Balance IDs resolved")
	return sourceID, destID, nil
}

// acquireLock acquires distributed locks for a transaction to ensure exclusive access to both
// source and destination balances. It uses a MultiLocker with deterministic ordering to prevent
// deadlocks when multiple transactions target the same balances.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - sourceBalanceID string: The ID of the source balance to lock.
// - destinationBalanceID string: The ID of the destination balance to lock.
//
// Returns:
// - *redlock.MultiLocker: A pointer to the acquired MultiLocker if successful.
// - error: An error if the locks could not be acquired.
func (l *Blnk) acquireLock(ctx context.Context, sourceBalanceID, destinationBalanceID string) (*redlock.MultiLocker, error) {
	ctx, span := tracer.Start(ctx, "Acquiring Lock")
	defer span.End()

	// Create a MultiLocker with both balance IDs
	// MultiLocker handles deduplication (if source == destination) and sorts keys lexicographically
	locker := redlock.NewMultiLocker(l.redis, []string{sourceBalanceID, destinationBalanceID}, model.GenerateUUIDWithSuffix("loc"))
	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Locks acquired", trace.WithAttributes(
		attribute.StringSlice("locked.keys", locker.Keys()),
	))
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
		StatusApplied:   StatusApplied,
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

// postTransactionActions performs post-processing actions for a transaction.
// It starts a tracing span, queues the transaction and balance data for indexing in dependency order,
// sends a webhook notification, and processes fund lineage if applicable.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to perform post-processing actions.
// - sourceBalance *model.Balance: The source balance (can be nil for rejected transactions).
// - destinationBalance *model.Balance: The destination balance (can be nil for rejected transactions).
func (l *Blnk) postTransactionActions(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) {
	_, span := tracer.Start(ctx, "Post Transaction Actions")
	defer span.End()
	go func() {
		// Create an index batch to ensure balances are indexed before the transaction
		batch := search.NewIndexBatch(transaction.TransactionID)

		// Add balances as dependencies (indexed first) if they exist
		if sourceBalance != nil {
			batch.AddDependency("balances", sourceBalance.BalanceID, sourceBalance)
		}
		if destinationBalance != nil {
			batch.AddDependency("balances", destinationBalance.BalanceID, destinationBalance)
		}

		// Set transaction as the primary item (indexed after dependencies)
		batch.SetPrimary(l.Config().Transaction.IndexQueuePrefix, transaction.TransactionID, transaction)

		// Queue the batch for indexing
		err := l.queue.queueIndexBatch(batch)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}

		// Send webhook notification
		err = l.SendWebhook(NewWebhook{
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

	transactionAmount := transaction.PreciseAmount

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
// - amount *big.Int: The amount to be processed in the transaction.
// - maxWorkers int: The maximum number of workers to process transactions concurrently.
// - streamMode bool: A flag indicating whether to process transactions in streaming mode.
// - gt getTxns: A function to retrieve transactions in batches.
// - tw transactionWorker: A function to process transactions.
//
// Returns:
// - []*model.Transaction: A slice of pointers to the processed Transaction models.
// - error: An error if the transactions could not be processed.
func (l *Blnk) ProcessTransactionInBatches(ctx context.Context, parentTransactionID string, amount *big.Int, maxWorkers int, streamMode bool, gt getTxns, tw transactionWorker) ([]*model.Transaction, error) {
	// Start a tracing span
	ctx, span := tracer.Start(ctx, "ProcessTransactionInBatches")
	defer span.End()

	batchSize := l.Config().Transaction.BatchSize
	maxQueueSize := l.Config().Transaction.MaxQueueSize

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
			return allTxns, fmt.Errorf("error occurred during processing: %v", allErrors)
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
		// Check inflight status using the helper function
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
		// Check inflight status using the helper function
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
		// Execute pre-transaction hooks
		if err := l.Hooks.ExecutePreHooks(ctx, transaction.TransactionID, transaction); err != nil {
			span.RecordError(err)
			return nil, err
		}

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

		// Execute post-transaction hooks
		if err := l.Hooks.ExecutePostHooks(ctx, transaction.TransactionID, transaction); err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("post-transaction hooks failed")
		}

		// Perform post-transaction actions such as indexing and sending webhooks
		// Pass balances to ensure they are indexed before the transaction
		l.postTransactionActions(ctx, transaction, sourceBalance, destinationBalance)

		span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
		return transaction, nil
	})
}

// executeWithLock executes a function with distributed locks to ensure exclusive access to both
// source and destination balances. It resolves balance IDs first (handling @world indicators),
// then acquires locks in deterministic order to prevent deadlocks.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to acquire the locks.
// - fn func(context.Context) (*model.Transaction, error): The function to execute with the locks.
//
// Returns:
// - *model.Transaction: A pointer to the Transaction model returned by the function.
// - error: An error if the locks could not be acquired or if the function execution fails.
func (l *Blnk) executeWithLock(ctx context.Context, transaction *model.Transaction, fn func(context.Context) (*model.Transaction, error)) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "ExecuteWithLock")
	defer span.End()

	// First resolve balance IDs (handle @ indicators) BEFORE acquiring locks
	// This ensures we lock on the correct balance IDs
	sourceID, destID, err := l.resolveBalanceIDs(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to resolve balance IDs: %w", err)
	}

	// Update transaction with resolved IDs
	transaction.Source = sourceID
	transaction.Destination = destID

	// Acquire distributed locks for both source and destination balances
	// MultiLocker handles deduplication (if source == destination) and sorts keys lexicographically
	locker, err := l.acquireLock(ctx, sourceID, destID)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	// Execute the provided function with the locks held
	// The function will re-fetch balances to get fresh state after lock acquisition
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

// processBalances processes the source and destination balances by applying the transaction in-memory.
// It starts a tracing span, applies the transaction to the balances, and records relevant events and errors.
// Note: The actual database update of balances is done atomically with the transaction persistence
// in finalizeTransaction to ensure consistency.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be applied to the balances.
// - sourceBalance *model.Balance: The source balance to be updated.
// - destinationBalance *model.Balance: The destination balance to be updated.
//
// Returns:
// - error: An error if the transaction could not be applied to the balances.
func (l *Blnk) processBalances(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessBalances")
	defer span.End()

	// Apply the transaction to the source and destination balances (in-memory only)
	if err := l.applyTransactionToBalances(ctx, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "failed to apply transaction to balances", err)
	}

	span.AddEvent("Balances calculated")
	return nil
}

// finalizeTransaction finalizes the transaction by updating its details and atomically persisting
// both the transaction and balance updates to the database in a single database transaction.
// This ensures that either both operations succeed together, or neither is committed,
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

	// Discard transaction if amount is 0
	if transaction.PreciseAmount != nil && transaction.PreciseAmount.Cmp(big.NewInt(0)) == 0 {
		span.AddEvent("Transaction with zero amount discarded, not persisted", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
		return transaction, nil
	}

	// Prepare lineage outbox entry for atomic insertion with transaction
	// This ensures lineage processing intent is captured even if later async operations fail
	var outbox *model.LineageOutbox
	if transaction.Status == StatusApplied || transaction.Status == StatusInflight {
		// Skip lineage for commits of inflight transactions - lineage was already created for the original
		isInflightCommit := transaction.ParentTransaction != "" && transaction.MetaData != nil && transaction.MetaData["inflight"] == true
		if !isInflightCommit {
			outbox = l.PrepareLineageOutbox(ctx, transaction, sourceBalance, destinationBalance)
		}
	}

	// Atomically persist the transaction, update balances, and insert lineage outbox in a single database transaction
	transaction, err := l.datasource.RecordTransactionWithBalancesAndOutbox(ctx, transaction, sourceBalance, destinationBalance, outbox)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "failed to persist transaction with balances", err)
	}

	// Check balance monitors asynchronously (fire and forget - not on critical path)
	go l.checkBalanceMonitors(ctx, sourceBalance)
	go l.checkBalanceMonitors(ctx, destinationBalance)

	span.AddEvent("Transaction and balances persisted atomically", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
		attribute.Bool("lineage.outbox_created", outbox != nil),
	))

	return transaction, nil
}

// releaseLock releases the distributed locks acquired for a transaction.
// It starts a tracing span, attempts to release all locks, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - locker *redlock.MultiLocker: The MultiLocker object representing the acquired locks.
func (l *Blnk) releaseLock(ctx context.Context, locker *redlock.MultiLocker) {
	ctx, span := tracer.Start(ctx, "ReleaseLock")
	defer span.End()

	// Attempt to release all locks
	if err := locker.Unlock(ctx); err != nil {
		span.RecordError(err)
		logrus.WithError(err).Error("failed to release lock")
	}
	span.AddEvent("Locks released")
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

	// Attempt to release the lock
	if err := locker.Unlock(ctx); err != nil {
		span.RecordError(err)
		logrus.WithError(err).Error("failed to release lock")
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
	logrus.WithError(err).Error(msg)
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
		logrus.WithError(err).Error("failed to save transaction to db")
		return nil, err
	}

	span.AddEvent("Transaction rejected", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	if transaction.Atomic {
		logrus.Info(transaction.ParentTransaction, "parent transaction", transaction.Atomic, "atomic", transaction.Inflight, "inflight")
		parentTransactionID, ok := transaction.MetaData["QUEUED_PARENT_TRANSACTION"].(string)
		if !ok {
			return nil, fmt.Errorf("parent transaction ID not found in meta data")
		}
		l.handleAsyncBulkTransactionFailure(ctx, errors.New("transaction rejected"), parentTransactionID, transaction.Atomic, transaction.Inflight)
	}
	// For rejected transactions, no balances were updated, so pass nil
	l.postTransactionActions(ctx, transaction, nil, nil)
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
func (l *Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount *big.Int) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "CommitInflightTransaction")
	defer span.End()

	// Create a lock key using the transaction ID (which is the parent ID for the commit)
	lockKey := fmt.Sprintf("inflight-commit:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	// Acquire the lock using cached config
	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for inflight commit: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

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
	committedTxn, err := l.finalizeCommitment(ctx, transaction, false)
	if err != nil {
		return nil, err
	}

	// Queue shadow commit work via outbox for reliable processing with retries
	if err := l.queueShadowWork(ctx, transactionID, model.LineageTypeShadowCommit); err != nil {
		logrus.WithError(err).WithField("transaction_id", transactionID).Error("failed to queue shadow commit")
	}

	return committedTxn, nil
}

func (l *Blnk) CommitInflightTransactionWithQueue(ctx context.Context, transactionID string, amount *big.Int) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "CommitInflightTransactionWithQueue")
	defer span.End()

	// Create a lock key using the transaction ID (which is the parent ID for the commit)
	lockKey := fmt.Sprintf("inflight-commit:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	// Acquire the lock using cached config
	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for inflight commit: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

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
	committedTxn, err := l.finalizeCommitment(ctx, transaction, true)
	if err != nil {
		return nil, err
	}

	// Queue shadow commit work via outbox for reliable processing with retries
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

	// Get the remaining amount that can be committed
	amountLeft, err := l.calculateRemainingAmount(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Check if transaction is already fully committed
	if err := l.checkTransactionCommitStatus(amountLeft); err != nil {
		span.RecordError(err)
		return err
	}

	// Validate the requested amount against limits
	if err := l.validateRequestedAmount(transaction, amount, amountLeft); err != nil {
		span.RecordError(err)
		return err
	}

	// Update the transaction with the appropriate amount
	l.updateTransactionAmount(transaction, amount, amountLeft)

	// Log the successful validation and update
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
	// Skip validation for full commit (amount = 0)
	if amount.Cmp(big.NewInt(0)) == 0 {
		return nil
	}

	requestedAmount := amount

	// Check if requested amount exceeds original transaction amount
	if requestedAmount.Cmp(transaction.PreciseAmount) > 0 {
		return fmt.Errorf("cannot commit more than the original transaction amount. Original: %s%.2f, Requested: %s%.2f",
			transaction.Currency, transaction.Amount, transaction.Currency, amount)
	}

	// Check if requested amount exceeds remaining amount
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
		// Update with specific amount
		transaction.PreciseAmount = amount
	} else {
		// Update with full remaining amount
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

	// Update the transaction status to committed and generate new identifiers
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
		// Queue the transaction for further processing
		transaction, err := l.RecordTransaction(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, l.logAndRecordError(span, "saving transaction to db error", err)
		}
		return transaction, nil
	}

	// Update the transaction status to applied for client response. commit is an internal status.
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

	// Create a lock key using the transaction ID (which is the parent ID for the void)
	lockKey := fmt.Sprintf("inflight-commit:%s", transactionID)
	locker := redlock.NewLocker(l.redis, lockKey, model.GenerateUUIDWithSuffix("loc"))

	// Acquire the lock using cached config
	err := locker.Lock(ctx, l.Config().Transaction.LockDuration)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock for inflight void: %w", err)
	}
	defer l.releaseSingleLock(ctx, locker)

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

	if amountLeft.Cmp(big.NewInt(0)) == 0 {
		err := errors.New("cannot void. Transaction already committed")
		span.RecordError(err)
		return transaction, err
	}
	span.AddEvent("Inflight transaction voided", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	// Finalize the void transaction
	voidedTxn, err := l.finalizeVoidTransaction(ctx, transaction, amountLeft)
	if err != nil {
		return nil, err
	}

	// Queue shadow void work via outbox for reliable processing with retries
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

	// Validate the transaction status using the helper function
	if !IsInflightTransaction(transaction) {
		err := fmt.Errorf("transaction is not in inflight status")
		span.RecordError(err)
		return nil, err
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

	// Fetch the total committed amount for the transaction
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(ctx, transaction.TransactionID)
	if err != nil {
		span.RecordError(err)
		return big.NewInt(0), l.logAndRecordError(span, "error fetching committed amount", err)
	}

	// Calculate the remaining amount
	committedBigInt := committedAmount
	remainingAmount := new(big.Int).Sub(transaction.PreciseAmount, committedBigInt)

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

	// Update the transaction status to void and set the remaining amount
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

	// Queue the transaction for further processing
	transaction, err := l.RecordTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	span.AddEvent("Void transaction finalized", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))
	return transaction, nil
}

// prepareTransactionForQueue prepares a transaction for queueing by setting status, metadata,
// and resolving source/destination balances.
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

	err = l.queue.QueueInflightExpiry(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "failed to queue inflight expiry", err)
	}

	return transaction, nil
}

func processTransactionAsync(ctx context.Context, l *Blnk, transaction *model.Transaction, originalRef string, originalTxnID string, transactions []*model.Transaction) {
	go func() {
		ctx, span := tracer.Start(ctx, "ProcessTransactionAsync")
		defer span.End()

		queueTransactions, err := l.processTxns(ctx, transaction, transactions, originalTxnID, originalRef)
		if err != nil {
			span.RecordError(err)
			// return nil, err
		}

		if !transaction.SkipQueue {
			if err := enqueueTransactions(ctx, l.queue, transaction, queueTransactions); err != nil {
				span.RecordError(err)
				// return nil, err
			}
		}
	}()
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
	if transaction.Rate == 0 {
		transaction.Rate = 1
	}

	// Initialize metadata if it doesn't exist
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}

	// Set inflight flag in metadata if the transaction is inflight
	if transaction.Inflight {
		transaction.MetaData["inflight"] = true
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

// GetAllTransactionsWithFilter retrieves transactions using advanced filters.
// It starts a tracing span, fetches transactions matching the filter criteria, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - filters *filter.QueryFilterSet: Filter conditions to apply.
// - limit int: Maximum number of transactions to return.
// - offset int: Offset for pagination.
//
// Returns:
// - []model.Transaction: A slice of Transaction models matching the filter criteria.
// - error: An error if the transactions could not be retrieved.
func (l *Blnk) GetAllTransactionsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetAllTransactionsWithFilter")
	defer span.End()

	transactions, err := l.datasource.GetAllTransactionsWithFilter(ctx, filters, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Transactions with filter retrieved")
	return transactions, nil
}

// GetAllTransactionsWithFilterAndOptions retrieves transactions with advanced filters, sorting, and optional count.
func (l *Blnk) GetAllTransactionsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Transaction, *int64, error) {
	ctx, span := tracer.Start(ctx, "GetAllTransactionsWithFilterAndOptions")
	defer span.End()

	transactions, count, err := l.datasource.GetAllTransactionsWithFilterAndOptions(ctx, filters, opts, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}

	span.AddEvent("Transactions with filter and options retrieved")
	return transactions, count, nil
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

// getOriginalTransactionForRefund retrieves the original transaction to be refunded,
// checking both the database and the queue if necessary.
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
				return nil, fmt.Errorf("transaction %s not found in DB or queue: %w", transactionID, err)
			}
			if queuedTxn == nil {
				err := fmt.Errorf("transaction %s not found in DB or queue", transactionID)
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

	// Validate the transaction status
	if originalTxn.Status == StatusRejected {
		err := fmt.Errorf("transaction %s is not in a state that can be refunded (status: %s)", originalTxn.TransactionID, originalTxn.Status)
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

// prepareRefundTransaction creates and configures a new transaction object for the refund.
func prepareRefundTransaction(originalTxn *model.Transaction, skipQueue bool) *model.Transaction {
	newTransaction := *originalTxn // Create a copy
	newTransaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	newTransaction.Reference = model.GenerateUUIDWithSuffix("ref")
	newTransaction.ParentTransaction = originalTxn.TransactionID
	newTransaction.Source = originalTxn.Destination // Swap source and destination
	newTransaction.Destination = originalTxn.Source
	newTransaction.AllowOverdraft = true
	newTransaction.SkipQueue = skipQueue

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
	ctx, span := tracer.Start(ctx, "RefundTransaction")
	defer span.End()

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
	refundTxnObject := prepareRefundTransaction(originalTxn, skipQueue)

	// 4. Queue the refund transaction for processing
	queuedRefundTxn, err := l.QueueTransaction(ctx, refundTxnObject)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to queue refund transaction %s: %w", refundTxnObject.TransactionID, err)
	}

	span.AddEvent("Refund transaction queued", trace.WithAttributes(attribute.String("refund.transaction.id", queuedRefundTxn.TransactionID)))
	return queuedRefundTxn, nil
}

// processBulkTransactions prepares and queues all transactions in a batch with the given batch ID
func (l *Blnk) processBulkTransactions(ctx context.Context, transactions []*model.Transaction, batchID string, inflight bool, skipQueue bool) error {
	for i, txn := range transactions {
		// Set transaction properties
		txn.Inflight = inflight
		txn.SkipQueue = skipQueue // Process synchronously within the batch context first
		txn.ParentTransaction = batchID

		// Add sequence number to metadata
		if txn.MetaData == nil {
			txn.MetaData = make(map[string]interface{})
		}
		txn.MetaData["sequence"] = i + 1

		// Queue the transaction (which will record it if SkipQueue is true)
		if _, err := l.QueueTransaction(ctx, txn); err != nil {
			// Create a more descriptive error that includes transaction reference details
			return fmt.Errorf("failed to queue transaction %d (Reference: %s, Source: %s, Destination: %s, Amount: %.2f): %w",
				i+1, txn.Reference, txn.Source, txn.Destination, txn.Amount, err)
		}
	}
	return nil
}

// rollbackBatchTransactions performs a rollback of transactions in a batch
// Returns the action performed (voided/refunded) and any error that occurred
func (l *Blnk) rollbackBatchTransactions(ctx context.Context, batchID string, isInflight bool) (string, error) {
	var action string
	var rollbackErr error

	if isInflight {
		action, rollbackErr = l.voidInflightBatchTransactions(ctx, batchID)
	} else {
		action, rollbackErr = l.refundNonInflightBatchTransactions(ctx, batchID)
	}

	l.logRollbackResult(batchID, action, rollbackErr)
	return action, rollbackErr
}

// voidInflightBatchTransactions voids all inflight transactions in a batch
func (l *Blnk) voidInflightBatchTransactions(ctx context.Context, batchID string) (string, error) {
	_, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetInflightTransactionsByParentID,
		l.VoidWorker,
	)
	return "voided", err
}

// refundNonInflightBatchTransactions refunds all non-inflight transactions in a batch
func (l *Blnk) refundNonInflightBatchTransactions(ctx context.Context, batchID string) (string, error) {
	_, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetRefundableTransactionsByParentID,
		l.RefundWorker,
	)
	return "refunded", err
}

// logRollbackResult logs the outcome of a rollback operation
func (l *Blnk) logRollbackResult(batchID string, action string, err error) {
	if err != nil {
		logrus.WithError(err).WithField("batch_id", batchID).Error("failed to rollback batch transactions")
	} else {
		logrus.WithFields(logrus.Fields{
			"batch_id": batchID,
			"action":   action,
		}).Info("successfully rolled back atomic batch")
	}
}

// sendBulkTransactionWebhook sends a webhook notification for a bulk transaction result
func (l *Blnk) sendBulkTransactionWebhook(batchID, status, errorMsg string, transactionCount int) {
	// Create payload with or without error info depending on status
	payload := map[string]interface{}{
		"batch_id":  batchID,
		"status":    status,
		"timestamp": time.Now(),
	}

	// Only include transaction count for success cases
	if status != "failed" {
		payload["transaction_count"] = transactionCount
	}

	// Include error details for failure cases
	if status == "failed" && errorMsg != "" {
		payload["error"] = errorMsg
	}

	err := l.SendWebhook(NewWebhook{
		Event:   "bulk_transaction." + status,
		Payload: payload,
	})
	if err != nil {
		logrus.WithError(err).WithField("batch_id", batchID).Error("failed to send webhook notification")
	}
}

// handleAsyncBulkTransactionFailure handles failures in asynchronous processing
// and builds a detailed error message including rollback status
func (l *Blnk) handleAsyncBulkTransactionFailure(ctx context.Context, err error, batchID string, isAtomic bool, isInflight bool) {
	logrus.WithError(err).WithField("batch_id", batchID).Error("async bulk transaction error")

	var errorMessage string

	if isAtomic {
		action, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, isInflight)

		if rollbackErr != nil {
			errorMessage = fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
			logrus.WithError(rollbackErr).WithField("batch_id", batchID).Error("failed to roll back batch")
		} else {
			errorMessage = fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action)
			logrus.WithFields(logrus.Fields{
				"batch_id": batchID,
				"action":   action,
			}).Info("successfully rolled back async batch")
		}
	} else {
		// If not atomic, just include the original error and note about no rollback
		errorMessage = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
	}

	// Send webhook with the complete error message including rollback status
	l.sendBulkTransactionWebhook(batchID, "failed", errorMessage, 0) // 0 count for failed batch
}

// CreateBulkTransactions handles the creation of multiple transactions in a batch.
// If atomic is true: Any failure will cause all transactions to be rolled back (or voided if inflight).
// If atomic is false: Failures will stop processing but previous transactions remain unaffected.
// If run_async is true: Processing happens in background with webhook notifications.
func (l *Blnk) CreateBulkTransactions(ctx context.Context, req *model.BulkTransactionRequest) (*model.BulkTransactionResult, error) {
	ctx, span := tracer.Start(ctx, "Blnk.CreateBulkTransactions")
	defer span.End()

	// Generate batch ID (parent transaction ID)
	batchID := model.GenerateUUIDWithSuffix("bulk")
	span.SetAttributes(attribute.String("batch.id", batchID))

	// Check if this should be run asynchronously
	if req.RunAsync {
		if !asyncBulkSemaphore.TryAcquire(1) {
			return nil, apierror.NewAPIError(
				apierror.ErrRateLimited,
				"too many async bulk operations in progress, try again later",
				nil,
			)
		}

		// Start processing in background
		go func() {
			defer asyncBulkSemaphore.Release(1)

			// Create a background context with timeout
			bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()

			logrus.Infof("Starting async bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
				batchID, len(req.Transactions), req.Atomic, req.Inflight)

			// Process transactions in batch
			err := l.processBulkTransactions(bgCtx, req.Transactions, batchID, req.Inflight, req.SkipQueue)

			if err != nil {
				// Handle failure (rollback if atomic, send webhook)
				l.handleAsyncBulkTransactionFailure(bgCtx, err, batchID, req.Atomic, req.Inflight)
			} else {
				// Send webhook notification for success
				status := "inflight"
				if !req.Inflight {
					status = "applied"
				}
				l.sendBulkTransactionWebhook(batchID, status, "", len(req.Transactions))
				logrus.Infof("Completed async bulk transaction batch %s successfully", batchID)
			}
		}()

		// Return immediate response indicating async processing started
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "processing", // Indicate that it's running in the background
		}, nil
	}

	// Synchronous processing
	logrus.Infof("Starting sync bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
		batchID, len(req.Transactions), req.Atomic, req.Inflight)

	// Process transactions in batch
	if err := l.processBulkTransactions(ctx, req.Transactions, batchID, req.Inflight, req.SkipQueue); err != nil {
		span.RecordError(err)
		logrus.WithError(err).WithField("batch_id", batchID).Error("sync bulk transaction error")

		var responseError string
		if req.Atomic {
			action, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, req.Inflight)
			if rollbackErr != nil {
				responseError = fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
			} else {
				responseError = fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action)
			}
		} else {
			responseError = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
		}

		// Return error result for synchronous failure
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "failed",
			Error:   responseError,
		}, errors.New(responseError) // Return the error itself as well
	}

	// Synchronous success
	status := "inflight"
	if !req.Inflight {
		status = "applied"
	}

	logrus.Infof("Completed sync bulk transaction batch %s successfully", batchID)
	return &model.BulkTransactionResult{
		BatchID:          batchID,
		Status:           status,
		TransactionCount: len(req.Transactions),
	}, nil
}
