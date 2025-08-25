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

	"github.com/blnkfinance/blnk/config"
	redis_db "github.com/blnkfinance/blnk/internal/redis-db"

	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
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
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		log.Fatalf("Error parsing Redis URL: %v", err)
	}

	queueOptions := asynq.RedisClientOpt{Addr: redisOption.Addr, Password: redisOption.Password, DB: redisOption.DB, TLSConfig: redisOption.TLSConfig}
	client := asynq.NewClient(queueOptions)
	inspector := asynq.NewInspector(queueOptions)
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
	cfg, err := config.Fetch()
	if err != nil {
		return err
	}

	IPayload, err := json.Marshal(transactionID)
	if err != nil {
		return err
	}
	taskOptions := []asynq.Option{
		asynq.TaskID(transactionID),
		asynq.Queue(cfg.Queue.InflightExpiryQueue),
		asynq.ProcessIn(time.Until(expiresAt)),
	}
	task := asynq.NewTask(cfg.Queue.InflightExpiryQueue, IPayload, taskOptions...)
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
	cfg, err := config.Fetch()
	if err != nil {
		return err
	}

	if cfg.TypeSense.Dns == "" {
		return nil
	}

	payload := map[string]interface{}{
		"collection": collection,
		"payload":    data,
	}

	IPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	taskOptions := []asynq.Option{asynq.Queue(cfg.Queue.IndexQueue)}
	task := asynq.NewTask(cfg.Queue.IndexQueue, IPayload, taskOptions...)
	info, err := q.Client.Enqueue(task)
	if err != nil {
		log.Println("here", err, info)
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
		return err
	}
	info, err := q.Client.EnqueueContext(ctx, q.geTask(transaction, payload), asynq.MaxRetry(5))
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued transaction: %+v", transaction.Reference)

	return nil
}

// QueueInflightExpiry handles queuing a transaction for inflight expiration.
// This method is separate from the main Enqueue to ensure expiration is handled
// regardless of whether the transaction is queued or processed immediately.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to queue for expiration.
//
// Returns:
// - error: An error if the expiration could not be queued.
func (q *Queue) QueueInflightExpiry(ctx context.Context, transaction *model.Transaction) error {
	if !transaction.InflightExpiryDate.IsZero() {
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
	cnf, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config: %v", err)
		// Use default values if config fetch fails
		return q.geTaskWithDefaults(transaction, payload)
	}
	queueIndex := hashBalanceID(transaction.Source) % cnf.Queue.NumberOfQueues
	queueName := fmt.Sprintf("%s_%d", cnf.Queue.TransactionQueue, queueIndex+1)

	taskOptions := []asynq.Option{asynq.TaskID(transaction.TransactionID), asynq.Queue(queueName)}
	if !transaction.ScheduledFor.IsZero() {
		taskOptions = append(taskOptions, asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
	}

	return asynq.NewTask(queueName, payload, taskOptions...)
}

// Fallback function for when config fetch fails
func (q *Queue) geTaskWithDefaults(transaction *model.Transaction, payload []byte) *asynq.Task {
	conf, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config: %v", err)
		return nil
	}
	queueIndex := hashBalanceID(transaction.Source) % conf.Queue.NumberOfQueues
	queueName := fmt.Sprintf("new:transaction_%d", queueIndex+1) // Default prefix

	taskOptions := []asynq.Option{asynq.TaskID(transaction.TransactionID), asynq.Queue(queueName)}
	if !transaction.ScheduledFor.IsZero() {
		taskOptions = append(taskOptions, asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
	}

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
	cfg, err := config.Fetch()
	if err != nil {
		return nil, err
	}

	// Iterate over all specific transaction queues
	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
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
