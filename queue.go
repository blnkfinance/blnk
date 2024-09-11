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
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"time"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/hibiken/asynq"
	"github.com/jerry-enebeli/blnk/model"
)

const (
	TRANSACTION_QUEUE     = "new:transaction"
	WEBHOOK_QUEUE         = "new:webhoook"
	INDEX_QUEUE           = "new:index"
	EXPIREDINFLIGHT_QUEUE = "new:inflight-expiry"
	NumberOfQueues        = 20
)

// Queue represents a queue for handling various tasks.
type Queue struct {
	Client    *asynq.Client
	Inspector *asynq.Inspector
}

// TransactionTypePayload represents the payload for a transaction type.
type TransactionTypePayload struct {
	Data model.Transaction
}

// NewQueue initializes a new Queue instance with the provided configuration.
//
// Parameters:
// - conf *config.Configuration: The configuration for the queue.
//
// Returns:
// - *Queue: A pointer to the newly created Queue instance.
func NewQueue(conf *config.Configuration) *Queue {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	return &Queue{
		Client:    client,
		Inspector: inspector,
	}
}

// queueInflightExpiry enqueues a task to handle inflight expiry for a transaction.
//
// Parameters:
// - transactionID string: The ID of the transaction.
// - expiresAt time.Time: The expiration time for the inflight status.
//
// Returns:
// - error: An error if the task could not be enqueued.
func (q *Queue) queueInflightExpiry(transactionID string, expiresAt time.Time) error {
	IPayload, err := json.Marshal(transactionID)
	if err != nil {
		log.Fatal(err)
	}
	taskOptions := []asynq.Option{asynq.TaskID(transactionID), asynq.Queue(EXPIREDINFLIGHT_QUEUE), asynq.ProcessIn(time.Until(expiresAt))}
	task := asynq.NewTask(EXPIREDINFLIGHT_QUEUE, IPayload, taskOptions...)
	info, err := q.Client.Enqueue(task)
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued inflight expiry: %+v", transactionID)
	return nil
}

// queueIndexData enqueues a task to index data in a specified collection.
//
// Parameters:
// - id string: The ID of the data to index.
// - collection string: The name of the collection to index the data in.
// - data interface{}: The data to be indexed.
//
// Returns:
// - error: An error if the task could not be enqueued.
func (q *Queue) queueIndexData(id string, collection string, data interface{}) error {
	payload := map[string]interface{}{
		"collection": collection,
		"payload":    data,
	}

	IPayload, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
	}

	taskOptions := []asynq.Option{asynq.Queue(INDEX_QUEUE)}
	task := asynq.NewTask(INDEX_QUEUE, IPayload, taskOptions...)
	info, err := q.Client.Enqueue(task)
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued index data: %+v", id)
	return nil
}

// Enqueue enqueues a transaction to the Redis queue.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be enqueued.
//
// Returns:
// - error: An error if the transaction could not be enqueued.
func (q *Queue) Enqueue(ctx context.Context, transaction *model.Transaction) error {
	ctx, span := tracer.Start(ctx, "Adding Transaction To Redis Queue")
	defer span.End()

	payload, err := json.Marshal(transaction)
	if err != nil {
		log.Fatal(err)
	}
	info, err := q.Client.EnqueueContext(ctx, q.geTask(transaction, payload), asynq.MaxRetry(5))
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued transaction: %+v", transaction.Reference)

	if !transaction.InflightExpiryDate.IsZero() {
		fmt.Println(transaction.InflightExpiryDate)
		return q.queueInflightExpiry(transaction.TransactionID, transaction.InflightExpiryDate)
	}

	return nil
}

// geTask generates a task for a transaction and assigns it to a specific queue based on the balance ID.
// It ensures that transactions are evenly distributed across multiple queues by hashing the balance ID.
// This approach helps to avoid race conditions on a balance by ensuring that all transactions related to the same balance
// are processed serially within the same queue, thereby maintaining accuracy and consistency.
//
// Parameters:
// - transaction *model.Transaction: The transaction for which to generate the task.
// - payload []byte: The payload for the task, typically the serialized transaction data.
//
// Returns:
// - *asynq.Task: The generated task ready to be enqueued.
func (q *Queue) geTask(transaction *model.Transaction, payload []byte) *asynq.Task {
	// Hash the balance ID and use modulo to select a queue
	queueIndex := hashBalanceID(transaction.Source) % NumberOfQueues
	// Queue names are 1-based, so we add 1 to the index
	queueName := fmt.Sprintf("%s_%d", TRANSACTION_QUEUE, queueIndex+1)

	// Initialize task options with the task ID and the selected queue name
	taskOptions := []asynq.Option{asynq.TaskID(transaction.TransactionID), asynq.Queue(queueName)}

	// If the transaction is scheduled for a future time, add a processing delay
	if !transaction.ScheduledFor.IsZero() {
		taskOptions = append(taskOptions, asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
	}

	// Create and return the new task with the specified options
	return asynq.NewTask(queueName, payload, taskOptions...)
}

// hashBalanceID returns a consistent hash value for a string balance ID.
//
// Parameters:
// - balanceID string: The balance ID to hash.
//
// Returns:
// - int: The hash value of the balance ID.
func hashBalanceID(balanceID string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(balanceID))
	return int(hasher.Sum32())
}

// GetTransactionFromQueue retrieves a transaction from the queue by its ID.
//
// Parameters:
// - transactionID string: The ID of the transaction to retrieve.
//
// Returns:
// - *model.Transaction: A pointer to the Transaction model if found.
// - error: An error if the transaction could not be retrieved.
func (q *Queue) GetTransactionFromQueue(transactionID string) (*model.Transaction, error) {
	// Iterate over all specific transaction queues
	for i := 1; i <= NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", TRANSACTION_QUEUE, i)
		task, err := q.Inspector.GetTaskInfo(queueName, transactionID)
		if err == nil && task != nil {
			var txn model.Transaction
			if err := json.Unmarshal(task.Payload, &txn); err != nil {
				return nil, err
			}
			return &txn, nil
		}
	}
	return nil, nil // Return nil if transaction is not found in any queue
}
