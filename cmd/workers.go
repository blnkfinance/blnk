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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
	trace "github.com/jerry-enebeli/blnk/internal/traces"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/hibiken/asynq"
)

// indexData represents the data structure used for indexing data in the system.
// It includes the collection name and the payload which is the data to be indexed.
type indexData struct {
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload"`
}

// processTransaction processes a transaction received from the Redis queue.
// If a transaction fails due to "insufficient funds", it is rejected, and a webhook is sent.
// Otherwise, it retries the transaction in case of other failures.
func (b *blnkInstance) processTransaction(ctx context.Context, t *asynq.Task) error {
	// Start an OpenTelemetry span for tracing the transaction processing.
	ctx, span := otel.Tracer("blnk.transactions.worker").Start(ctx, "Process Transaction From Redis Queue")
	defer span.End()

	var txn model.Transaction
	// Unmarshal the transaction data from the Redis task payload.
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		logrus.Error(err)
		return err
	}

	// Attempt to record the transaction.
	_, err := b.blnk.RecordTransaction(ctx, &txn)
	if err != nil {
		// Check for "insufficient funds" error and handle rejection.
		if strings.Contains(strings.ToLower(err.Error()), "insufficient funds") {
			_, rejectErr := b.blnk.RejectTransaction(ctx, &txn, err.Error())
			if rejectErr != nil {
				return rejectErr
			}
			// Send a webhook for the rejected transaction.
			webhookErr := blnk.SendWebhook(blnk.NewWebhook{
				Event:   "transaction.rejected",
				Payload: txn,
			})
			return webhookErr
		}
		// Log the retry attempt for other errors.
		logrus.Infof("Transaction %s pushed back for retry due to error: %v", txn.TransactionID, err)
		return err
	}

	log.Println(" [*] Transaction Processed", txn.TransactionID)
	return nil
}

// indexData indexes data into TypeSense for searchability.
// It fetches the collection name and payload from the task, ensures the collections exist,
// and sends the payload to the appropriate TypeSense collection for indexing.
func (b *blnkInstance) indexData(_ context.Context, t *asynq.Task) error {
	var data indexData

	// Unmarshal the indexing data from the task payload.
	if err := json.Unmarshal(t.Payload(), &data); err != nil {
		logrus.Error(err)
		return err
	}

	collection := data.Collection
	payload := data.Payload

	// Initialize a new TypeSense client and ensure collections exist.
	newSearch := blnk.NewTypesenseClient("blnk-api-key", []string{b.cnf.TypeSense.Dns})
	err := newSearch.EnsureCollectionsExist(context.Background())
	if err != nil {
		log.Fatalf("Failed to ensure collections exist: %v", err)
	}

	// Handle the notification and send the payload to the collection for indexing.
	err = newSearch.HandleNotification(collection, payload)
	if err != nil {
		log.Println("Error indexing data", err)
		return err
	}

	log.Println(" [*] Data indexed", collection)
	return nil
}

// processInflightExpiry handles the expiry of inflight transactions.
// It voids the transaction by its ID and logs the action.
func (b *blnkInstance) processInflightExpiry(cxt context.Context, t *asynq.Task) error {
	var txnID string
	// Unmarshal the transaction ID from the task payload.
	if err := json.Unmarshal(t.Payload(), &txnID); err != nil {
		logrus.Error(err)
		return err
	}

	// Void the inflight transaction by its ID.
	_, err := b.blnk.VoidInflightTransaction(cxt, txnID)
	if err != nil {
		return err
	}

	logrus.Printf(" [*] Inflight Transaction Expired %s", txnID)
	return nil
}

// workerCommands defines the "workers" command to start worker processes.
// The workers listen to various queues such as transaction processing, indexing, and inflight expiry.
func workerCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "start blnk workers", // Short description of the command
		Run: func(cmd *cobra.Command, args []string) {
			// Fetch the configuration for the worker environment.
			conf, err := config.Fetch()
			if err != nil {
				log.Println("Error fetching config:", err)
				return
			}

			// Set up OpenTelemetry for tracing worker actions.
			shutdown, err := trace.SetupOTelSDK(context.Background(), "BLNK WORKERS")
			if err != nil {
				log.Fatalf("Error setting up OTel SDK: %v", err)
			}
			defer func() {
				if err := shutdown(context.Background()); err != nil {
					log.Printf("Error shutting down OTel SDK: %v", err)
				}
			}()

			// Define the queue names and their concurrency.
			queues := make(map[string]int)
			queues[blnk.WEBHOOK_QUEUE] = 3
			queues[blnk.INDEX_QUEUE] = 1
			queues[blnk.EXPIREDINFLIGHT_QUEUE] = 3

			// Set up individual transaction queues with concurrency.
			for i := 1; i <= blnk.NumberOfQueues; i++ {
				queueName := fmt.Sprintf("%s_%d", blnk.TRANSACTION_QUEUE, i)
				queues[queueName] = 1
			}

			// Initialize the Asynq server with Redis as the backend and the queue configuration.
			srv := asynq.NewServer(
				asynq.RedisClientOpt{Addr: conf.Redis.Dns},
				asynq.Config{
					Concurrency: 1, // Set the concurrency level for processing tasks
					Queues:      queues,
				},
			)

			// Set up the serve multiplexer to handle task processing for each queue.
			mux := asynq.NewServeMux()
			for i := 1; i <= blnk.NumberOfQueues; i++ {
				queueName := fmt.Sprintf("%s_%d", blnk.TRANSACTION_QUEUE, i)
				mux.HandleFunc(queueName, b.processTransaction)
			}

			// Register handlers for other task types (indexing, webhook, inflight expiry).
			mux.HandleFunc(blnk.INDEX_QUEUE, b.indexData)
			mux.HandleFunc(blnk.WEBHOOK_QUEUE, blnk.ProcessWebhook)
			mux.HandleFunc(blnk.EXPIREDINFLIGHT_QUEUE, b.processInflightExpiry)

			// Run the Asynq server and start processing tasks from the queues.
			if err := srv.Run(mux); err != nil {
				log.Fatal("Error running server:", err)
			}
		},
	}

	return cmd
}
