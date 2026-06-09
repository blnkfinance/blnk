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
	"math/big"
	"sync"

	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/semaphore"
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

// balanceMonitorSem bounds the number of concurrent balance-monitor checks
// spawned by post-commit work across all transactions.
var balanceMonitorSem = make(chan struct{}, 32)

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
