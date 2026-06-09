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
	"sync"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

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
