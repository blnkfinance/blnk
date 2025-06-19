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
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.elastic.co/apm/module/apmlogrus/v2"
	"go.opentelemetry.io/otel"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
	redis_db "github.com/jerry-enebeli/blnk/internal/redis-db"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
)

// indexData represents the data structure used for indexing data in the system.
// It includes the collection name and the payload which is the data to be indexed.
type indexData struct {
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload"`
}

func init() {
	logrus.AddHook(&apmlogrus.Hook{})
}

// processTransaction processes a transaction received from the Redis queue.
// If a transaction fails due to "insufficient funds", it is rejected, and a webhook is sent.
// Otherwise, it retries the transaction in case of other failures.
func (b *blnkInstance) processTransaction(ctx context.Context, t *asynq.Task) error {
	ctx, span := otel.Tracer("blnk.transactions.worker").Start(ctx, "Process Transaction From Redis Queue")
	defer span.End()

	var txn model.Transaction
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		logrus.Error(err)
		return err
	}

	_, err := b.blnk.RecordTransaction(ctx, &txn)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "insufficient funds") {
			cfg, _ := config.Fetch()
			if !cfg.Queue.InsufficientFundRetries {
				return handleTransactionRejection(ctx, b, &txn, err)
			}

			retryCount, _ := asynq.GetRetryCount(ctx)
			if retryCount >= cfg.Queue.MaxRetryAttempts {
				return handleTransactionRejection(ctx, b, &txn, fmt.Errorf("max retry attempts reached after insufficient funds"))
			}

			logrus.Infof("Insufficient funds for transaction %s, retry attempt %d/%d",
				txn.TransactionID, retryCount, cfg.Queue.MaxRetryAttempts)
			return err // This will trigger a retry
		}

		if strings.Contains(strings.ToLower(err.Error()), "transaction exceeds overdraft limit") {
			return handleTransactionRejection(ctx, b, &txn, err)
		}

		logrus.Infof("Transaction %s pushed back for retry due to error: %v", txn.TransactionID, err)
		return err
	}

	log.Println(" [*] Transaction Processed", txn.TransactionID)
	return nil
}

func handleTransactionRejection(ctx context.Context, b *blnkInstance, txn *model.Transaction, err error) error {
	_, rejectErr := b.blnk.RejectTransaction(ctx, txn, err.Error())
	if rejectErr != nil {
		return rejectErr
	}

	webhookErr := blnk.SendWebhook(blnk.NewWebhook{
		Event:   "transaction.rejected",
		Payload: *txn,
	})
	return webhookErr
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
	newSearch := blnk.NewTypesenseClient(b.cnf.TypeSenseKey, []string{b.cnf.TypeSense.Dns})
	err := newSearch.EnsureCollectionsExist(context.Background())
	if err != nil {
		log.Printf("Failed to ensure collections exist: %v", err)
		return err
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

func initializeQueues() map[string]int {
	cfg, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config, using defaults: %v", err)
		return nil
	}

	queues := make(map[string]int)
	queues[cfg.Queue.WebhookQueue] = 3
	queues[cfg.Queue.IndexQueue] = 1
	queues[cfg.Queue.InflightExpiryQueue] = 3

	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
		queues[queueName] = 1
	}
	return queues
}

func initializeWorkerServer(conf *config.Configuration, queues map[string]int) (*asynq.Server, error) {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %v", err)
	}

	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
		},
		asynq.Config{
			Concurrency: 1,
			Queues:      queues,
		},
	), nil
}

func initializeTaskHandlers(b *blnkInstance, mux *asynq.ServeMux) {
	cfg, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config, using defaults: %v", err)
		return
	}

	// Register handlers for transaction queues
	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
		mux.HandleFunc(queueName, b.processTransaction)
	}

	// Register handlers for other task types
	mux.HandleFunc(cfg.Queue.IndexQueue, b.indexData)
	mux.HandleFunc(cfg.Queue.WebhookQueue, blnk.ProcessWebhook)
	mux.HandleFunc(cfg.Queue.InflightExpiryQueue, b.processInflightExpiry)
}

// workerCommands defines the "workers" command to start worker processes.
// The workers listen to various queues such as transaction processing, indexing, and inflight expiry.
func workerCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "start blnk workers", // Short description of the command
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()

			// Load configuration
			conf, err := config.Fetch()
			if err != nil {
				log.Fatal("Error fetching config:", err)
			}

			// Initialize observability (tracing and PostHog)
			phClient, shutdown, err := initializeObservability(ctx, conf)
			if err != nil {
				log.Fatal(err)
			}
			if shutdown != nil {
				defer func() {
					if err := shutdown(ctx); err != nil {
						log.Printf("Error during shutdown: %v", err)
					}
				}()
			}
			if phClient != nil {
				defer phClient.Close()
			}

			// Initialize queues
			queues := initializeQueues()

			// Initialize worker server
			srv, err := initializeWorkerServer(conf, queues)
			if err != nil {
				log.Fatal(err)
			}

			// Initialize task handlers
			mux := asynq.NewServeMux()
			initializeTaskHandlers(b, mux)

			// Start asynqmon server for health checks and monitoring
			redisOption, _ := redis_db.ParseRedisURL(conf.Redis.Dns)
			h := asynqmon.New(asynqmon.Options{
				RootPath: "/monitoring", //  Optional: if you want to serve asynqmon under a sub-path.
				RedisConnOpt: asynq.RedisClientOpt{
					Addr:      redisOption.Addr,
					Password:  redisOption.Password,
					DB:        redisOption.DB,
					TLSConfig: redisOption.TLSConfig,
				},
			})

			// Start asynqmon HTTP server in a new goroutine
			go func() {
				monitoringAddr := fmt.Sprintf(":%s", conf.Queue.MonitoringPort)
				log.Printf("Asynqmon server listening on %s/monitoring", monitoringAddr)
				if err := http.ListenAndServe(monitoringAddr, h); err != nil {
					log.Fatalf("could not start asynqmon server: %v", err)
				}
			}()

			// Start worker server
			if err := srv.Run(mux); err != nil {
				log.Fatalf("could not run server: %v", err)
			}
		},
	}

	return cmd
}
