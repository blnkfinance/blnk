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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/filter"
	blnkhooks "github.com/blnkfinance/blnk/internal/hooks"
	"github.com/blnkfinance/blnk/internal/hotpairs"
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
	StatusRejected  = "REJECTED"
)

var asyncBulkSemaphore = semaphore.NewWeighted(100) // max 100 concurrent
var asyncTxnSemaphore = semaphore.NewWeighted(20)   // max 20 concurrent async transaction processors

const (
	maxQueuedCoalescingBatchSize = 10000
)

type queuedCoalescingScope string

const (
	queuedCoalescingScopePair        queuedCoalescingScope = "pair"
	queuedCoalescingScopeSource      queuedCoalescingScope = "source"
	queuedCoalescingScopeDestination queuedCoalescingScope = "destination"
)

type queuedBatchPostCommitWork struct {
	transaction        *model.Transaction
	sourceBalance      *model.Balance
	destinationBalance *model.Balance
	outbox             *model.LineageOutbox
}

type queuedBatchPersistResult struct {
	orderedBalances []*model.Balance
	postCommitWork  []queuedBatchPostCommitWork
}

type transactionExecutionMode string

const (
	transactionExecutionModeSingle         transactionExecutionMode = "single"
	transactionExecutionModeQueuedBatch    transactionExecutionMode = "queued_batch"
	transactionExecutionModeHotQueuedBatch transactionExecutionMode = "hot_queued_batch"
)

type transactionExecutionPlan struct {
	mode        transactionExecutionMode
	transaction *model.Transaction
}

type transactionExecutionResult struct {
	mode        transactionExecutionMode
	transaction *model.Transaction
}

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
				logrus.Errorf("Error during processing: %v", err)
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
			logrus.Errorf("Error processing transaction: %v", result.Error)
			*allErrors = append(*allErrors, result.Error)
		} else if result.Txn != nil {
			*allTxns = append(*allTxns, result.Txn)
		} else {
			// Handle the case where the result contains no transaction and no error
			logrus.Warn("Received a result with no transaction and no error")
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
				logrus.Errorf("Error fetching transactions: %v", err)
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

// usedCoalescing reports whether the execution result came from a queued batch path
// instead of the single-transaction executor.
func (r transactionExecutionResult) usedCoalescing() bool {
	return r.mode == transactionExecutionModeQueuedBatch || r.mode == transactionExecutionModeHotQueuedBatch
}

// planTransactionExecution selects the internal execution mode for a transaction based on
// whether queued batching is allowed and whether hot-lane execution should be used.
func (l *Blnk) planTransactionExecution(transaction *model.Transaction, allowQueuedBatch, hotLane bool) transactionExecutionPlan {
	if allowQueuedBatch {
		if hotLane {
			return transactionExecutionPlan{mode: transactionExecutionModeHotQueuedBatch, transaction: transaction}
		}
		return transactionExecutionPlan{mode: transactionExecutionModeQueuedBatch, transaction: transaction}
	}

	return transactionExecutionPlan{mode: transactionExecutionModeSingle, transaction: transaction}
}

// executeTransactionPlan runs the selected internal execution mode and fails open from queued
// batching back to the single-transaction path when batching does not handle the work.
func (l *Blnk) executeTransactionPlan(ctx context.Context, plan transactionExecutionPlan) (transactionExecutionResult, error) {
	switch plan.mode {
	case transactionExecutionModeQueuedBatch:
		handled, err := l.TryRecordQueuedTransactionBatch(ctx, plan.transaction)
		if err != nil {
			logrus.WithError(err).Warnf("coalesced processing attempt failed for transaction %s", plan.transaction.TransactionID)
		}
		if handled {
			return transactionExecutionResult{
				mode:        transactionExecutionModeQueuedBatch,
				transaction: plan.transaction,
			}, nil
		}
		return l.executeTransactionPlan(ctx, l.planTransactionExecution(plan.transaction, false, false))
	case transactionExecutionModeHotQueuedBatch:
		handled, err := l.TryRecordQueuedTransactionBatchForHotLane(ctx, plan.transaction)
		if err != nil {
			logrus.WithError(err).Warnf("coalesced hot-lane processing attempt failed for transaction %s", plan.transaction.TransactionID)
		}
		if handled {
			return transactionExecutionResult{
				mode:        transactionExecutionModeHotQueuedBatch,
				transaction: plan.transaction,
			}, nil
		}
		return l.executeTransactionPlan(ctx, l.planTransactionExecution(plan.transaction, false, false))
	case transactionExecutionModeSingle:
		transaction, err := l.recordTransactionSingle(ctx, plan.transaction)
		if err != nil {
			return transactionExecutionResult{}, err
		}
		return transactionExecutionResult{
			mode:        transactionExecutionModeSingle,
			transaction: transaction,
		}, nil
	default:
		transaction, err := l.recordTransactionSingle(ctx, plan.transaction)
		if err != nil {
			return transactionExecutionResult{}, err
		}
		return transactionExecutionResult{
			mode:        transactionExecutionModeSingle,
			transaction: transaction,
		}, nil
	}
}

// processQueuedTransaction routes queued work through the shared executor so the planner can
// choose between normal queued batching, hot-lane batching, and single-transaction fallback.
func (l *Blnk) processQueuedTransaction(ctx context.Context, transaction *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
	return l.executeTransactionPlan(ctx, l.planTransactionExecution(transaction, true, hotLane))
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

// ProcessQueuedTransaction preserves the existing queued-worker behavior while routing the
// decision through the shared internal transaction executor.
func (l *Blnk) ProcessQueuedTransaction(ctx context.Context, transaction *model.Transaction, hotLane bool) (*model.Transaction, error) {
	result, err := l.processQueuedTransaction(ctx, transaction, hotLane)
	if err != nil {
		return nil, err
	}
	return result.transaction, nil
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
	result, err := l.executeTransactionPlan(ctx, l.planTransactionExecution(transaction, false, false))
	if err != nil {
		return nil, err
	}
	return result.transaction, nil
}

// recordTransactionSingle preserves the existing direct transaction-processing semantics by
// running the single-transaction flow under the balance lock.
func (l *Blnk) recordTransactionSingle(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
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

		work, skipPersist := l.buildTransactionExecutionWork(ctx, transaction, sourceBalance, destinationBalance)
		if skipPersist {
			span.AddEvent("Transaction with zero amount discarded, not persisted", trace.WithAttributes(attribute.String("transaction.id", work.transaction.TransactionID)))
			return work.transaction, nil
		}

		work, err = l.persistSingleTransactionExecutionWork(ctx, work)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		l.runTransactionPostCommitWork(ctx, span, []*model.Balance{sourceBalance, destinationBalance}, []queuedBatchPostCommitWork{work})

		span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", work.transaction.TransactionID)))
		logrus.Infof("Transaction %s processed successfully", work.transaction.TransactionID)
		return work.transaction, nil
	})
}

// TryRecordQueuedTransactionBatch opportunistically coalesces multiple QUEUED transactions that
// share the same contention shape into a single balance commit. It prefers the narrowest safe
// scope first (pair, then source, then destination) and fails open by returning handled=false if
// batching would be unsafe or provides no benefit.
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
func (l *Blnk) runTransactionPostCommitWork(ctx context.Context, span trace.Span, orderedBalances []*model.Balance, postCommitWork []queuedBatchPostCommitWork) {
	l.runTransactionPostCommitWorkWithHooks(ctx, span, orderedBalances, postCommitWork, nil)
}

// runTransactionPostCommitWorkWithHooks executes monitor checks, post-hooks, and
// post-transaction actions for persisted work items, optionally reusing a preloaded hook set.
func (l *Blnk) runTransactionPostCommitWorkWithHooks(ctx context.Context, span trace.Span, orderedBalances []*model.Balance, postCommitWork []queuedBatchPostCommitWork, postHooks []*blnkhooks.Hook) {
	for _, balance := range orderedBalances {
		go l.checkBalanceMonitors(ctx, balance)
	}

	for _, work := range postCommitWork {
		if l.Hooks != nil {
			var err error
			if postHooks != nil {
				err = l.Hooks.ExecuteHooks(ctx, postHooks, blnkhooks.PostTransaction, work.transaction.TransactionID, work.transaction)
			} else {
				err = l.Hooks.ExecutePostHooks(ctx, work.transaction.TransactionID, work.transaction)
			}
			if err != nil {
				span.RecordError(err)
				logrus.WithError(err).Error("post-transaction hooks failed")
			}
		}
		l.postTransactionActions(ctx, work.transaction, work.sourceBalance, work.destinationBalance)
	}
}

// listHooksForExecution returns the current hook set for the requested type, or nil when
// hook execution is not configured for the current Blnk instance.
func (l *Blnk) listHooksForExecution(ctx context.Context, hookType blnkhooks.HookType) ([]*blnkhooks.Hook, error) {
	if l.Hooks == nil {
		return nil, nil
	}

	return l.Hooks.ListHooks(ctx, hookType)
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
		hotpairs.RecordContention(ctx, l.hotPairs, sourceID, destID, transaction.Currency, err)
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
// step to ensure consistency.
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

// buildTransactionExecutionWork converts an in-memory-applied transaction into the shared
// persistence and post-commit work shape used by both single and batched execution paths.
func (l *Blnk) buildTransactionExecutionWork(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (queuedBatchPostCommitWork, bool) {
	transaction = l.updateTransactionDetails(ctx, transaction, sourceBalance, destinationBalance)
	if transaction.PreciseAmount != nil && transaction.PreciseAmount.Cmp(big.NewInt(0)) == 0 {
		return queuedBatchPostCommitWork{
			transaction:        transaction,
			sourceBalance:      sourceBalance,
			destinationBalance: destinationBalance,
		}, true
	}

	return queuedBatchPostCommitWork{
		transaction:        transaction,
		sourceBalance:      sourceBalance,
		destinationBalance: destinationBalance,
		outbox:             l.prepareTransactionOutbox(ctx, transaction, sourceBalance, destinationBalance),
	}, false
}

// persistSingleTransactionExecutionWork atomically persists one prepared transaction, its
// updated balances, and any lineage outbox using the shared execution work shape.
func (l *Blnk) persistSingleTransactionExecutionWork(ctx context.Context, work queuedBatchPostCommitWork) (queuedBatchPostCommitWork, error) {
	ctx, span := tracer.Start(ctx, "PersistSingleTransactionExecutionWork")
	defer span.End()

	transaction, err := l.datasource.RecordTransactionWithBalancesAndOutbox(ctx, work.transaction, work.sourceBalance, work.destinationBalance, work.outbox)
	if err != nil {
		span.RecordError(err)
		return queuedBatchPostCommitWork{}, l.logAndRecordError(span, "failed to persist transaction with balances", err)
	}

	work.transaction = transaction
	span.AddEvent("Transaction and balances persisted atomically", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
		attribute.Bool("lineage.outbox_created", work.outbox != nil),
	))

	return work, nil
}

func (l *Blnk) prepareTransactionOutbox(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.LineageOutbox {
	if transaction.Status != StatusApplied && transaction.Status != StatusInflight {
		return nil
	}

	// Skip lineage for commits of inflight transactions - lineage was already created for the original.
	isInflightCommit := transaction.ParentTransaction != "" && transaction.MetaData != nil && transaction.MetaData["inflight"] == true
	if isInflightCommit {
		return nil
	}

	return l.PrepareLineageOutbox(ctx, transaction, sourceBalance, destinationBalance)
}

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
	if err := locker.Lock(ctx, l.Config().Transaction.LockDuration); err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Locks acquired", trace.WithAttributes(
		attribute.StringSlice("locked.keys", locker.Keys()),
	))
	return locker, nil
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
		return fmt.Errorf("reference %s has already been used", transaction.Reference)
	}

	if _, ok := existingReferences[transaction.Reference]; ok {
		return fmt.Errorf("reference %s has already been used", transaction.Reference)
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

	err = l.queue.QueueInflightExpiry(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, l.logAndRecordError(span, "failed to queue inflight expiry", err)
	}

	return transaction, nil
}

func processTransactionAsync(ctx context.Context, l *Blnk, transaction *model.Transaction, originalRef string, originalTxnID string, transactions []*model.Transaction) {
	go func() {
		if err := asyncTxnSemaphore.Acquire(ctx, 1); err != nil {
			logrus.WithError(err).Error("failed to acquire async txn semaphore")
			return
		}
		defer asyncTxnSemaphore.Release(1)

		ctx, span := tracer.Start(ctx, "ProcessTransactionAsync")
		defer span.End()

		queueTransactions, err := l.processTxns(ctx, transaction, transactions, originalTxnID, originalRef)
		if err != nil {
			span.RecordError(err)
		}

		if !transaction.SkipQueue {
			if err := enqueueTransactions(ctx, l.queue, transaction, queueTransactions); err != nil {
				span.RecordError(err)
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
