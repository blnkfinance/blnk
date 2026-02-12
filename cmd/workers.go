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
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/notification"
	redis_db "github.com/blnkfinance/blnk/internal/redis-db"
	"github.com/blnkfinance/blnk/internal/search"
	"github.com/blnkfinance/blnk/model"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
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
	ctx, span := otel.Tracer("blnk.transactions.worker").Start(ctx, "Process Transaction From Redis Queue")
	defer span.End()

	var txn model.Transaction
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		logrus.Error(err)
		return err
	}

	_, err := b.blnk.RecordTransaction(ctx, &txn)
	if err != nil {
		// Handle reference already used error
		if strings.Contains(strings.ToLower(err.Error()), "reference") && strings.Contains(strings.ToLower(err.Error()), "already been used") {
			notification.NotifyError(err)
			return nil
		}

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

	logrus.Infof("Transaction %s processed successfully", txn.TransactionID)
	return nil
}

func handleTransactionRejection(ctx context.Context, b *blnkInstance, txn *model.Transaction, err error) error {
	_, rejectErr := b.blnk.RejectTransaction(ctx, txn, err.Error())
	if rejectErr != nil {
		return rejectErr
	}

	webhookErr := b.blnk.SendWebhook(blnk.NewWebhook{
		Event:   "transaction.rejected",
		Payload: *txn,
	})
	return webhookErr
}

// indexData indexes data into TypeSense for searchability.
// It fetches the collection name and payload from the task, ensures the collections exist,
// and sends the payload to the appropriate TypeSense collection for indexing.
func (b *blnkInstance) indexData(ctx context.Context, t *asynq.Task) error {
	if b.cnf.TypeSense.Dns == "" {
		return nil
	}

	var data indexData

	// Unmarshal the indexing data from the task payload.
	if err := json.Unmarshal(t.Payload(), &data); err != nil {
		logrus.Error(err)
		return err
	}

	collection := data.Collection
	payload := data.Payload

	// Initialize a new TypeSense client and ensure collections exist.
	newSearch := search.NewTypesenseClient(b.cnf.TypeSenseKey, []string{b.cnf.TypeSense.Dns})
	err := newSearch.EnsureCollectionsExist(ctx)
	if err != nil {
		log.Printf("Failed to ensure collections exist: %v", err)
		return err
	}

	// Handle the notification and send the payload to the collection for indexing.
	err = newSearch.HandleNotification(ctx, collection, payload)
	if err != nil {
		log.Println("Error indexing data", err)
		return err
	}

	log.Println(" [*] Data indexed", collection)
	return nil
}

// indexBatchData indexes a batch of items into TypeSense in dependency order.
// It first indexes all dependencies (e.g., balances), then indexes the primary item (e.g., transaction).
// This ensures referential integrity in the search index.
func (b *blnkInstance) indexBatchData(ctx context.Context, t *asynq.Task) error {
	if b.cnf.TypeSense.Dns == "" {
		return nil
	}

	var batch search.IndexBatch

	// Unmarshal the batch data from the task payload.
	if err := json.Unmarshal(t.Payload(), &batch); err != nil {
		logrus.Error(err)
		return err
	}

	// Initialize a new TypeSense client and ensure collections exist.
	newSearch := search.NewTypesenseClient(b.cnf.TypeSenseKey, []string{b.cnf.TypeSense.Dns})
	err := newSearch.EnsureCollectionsExist(ctx)
	if err != nil {
		log.Printf("Failed to ensure collections exist: %v", err)
		return err
	}

	// Handle the batch notification - indexes dependencies first, then primary.
	err = newSearch.HandleBatchNotification(ctx, &batch)
	if err != nil {
		log.Printf("Error indexing batch %s: %v", batch.ID, err)
		return err
	}

	log.Printf(" [*] Batch indexed: %s (deps: %d)", batch.ID, len(batch.Dependencies))
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
	queues[cfg.Queue.InflightExpiryQueue] = 1

	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
		queues[queueName] = 1
	}
	return queues
}

func initializeWebhookQueues() map[string]int {
	cfg, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config, using defaults: %v", err)
		return nil
	}

	queues := make(map[string]int)
	queues[cfg.Queue.WebhookQueue] = 3
	queues[cfg.Queue.IndexQueue] = 1

	return queues
}

func initializeWebhookWorkerServer(conf *config.Configuration, queues map[string]int) (*asynq.Server, error) {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %v", err)
	}

	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
		asynq.Config{
			Concurrency:     conf.Queue.WebhookConcurrency,
			Queues:          queues,
			ShutdownTimeout: 30 * time.Second,
		},
	), nil
}

func initializeWorkerServer(conf *config.Configuration, queues map[string]int) (*asynq.Server, error) {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %v", err)
	}

	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
		asynq.Config{
			Concurrency:     1,
			Queues:          queues,
			ShutdownTimeout: 30 * time.Second,
		},
	), nil
}

func initializeTaskHandlers(b *blnkInstance, mux *asynq.ServeMux) {
	cfg, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config, using defaults: %v", err)
		return
	}

	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
		mux.HandleFunc(queueName, b.processTransaction)
	}
	mux.HandleFunc(cfg.Queue.InflightExpiryQueue, b.processInflightExpiry)
}

func initializeWebhookTaskHandlers(b *blnkInstance, mux *asynq.ServeMux) {
	cfg, err := config.Fetch()
	if err != nil {
		log.Printf("Error fetching config, using defaults: %v", err)
		return
	}

	mux.HandleFunc(cfg.Queue.WebhookQueue, b.blnk.ProcessWebhook)
	mux.HandleFunc("new:hook_execution", b.blnk.Hooks.ProcessHookTask)
	mux.HandleFunc(cfg.Queue.IndexQueue, b.indexData)
	mux.HandleFunc("new:index:batch", b.indexBatchData)
}

// workerCommands defines the "workers" command to start worker processes.
// The workers listen to various queues such as transaction processing, indexing, and inflight expiry.
func workerCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "start blnk workers",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			conf, err := config.Fetch()
			if err != nil {
				log.Fatal("Error fetching config:", err)
			}

			phClient, shutdown, err := initializeTelemetryAndObservability(context.Background(), conf)
			if err != nil {
				log.Fatal(err)
			}
			if shutdown != nil {
				defer func() {
					tctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := shutdown(tctx); err != nil {
						log.Printf("Error during shutdown: %v", err)
					}
				}()
			}
			if phClient != nil {
				defer phClient.Close()
			}

			srv, webhookSrv, mux, webhookMux, err := setupWorkerServers(b, conf)
			if err != nil {
				log.Fatal(err)
			}

			monitoringSrv := startMonitoringServer(conf)

			if err := srv.Start(mux); err != nil {
				log.Fatalf("could not start transaction worker server: %v", err)
			}
			if err := webhookSrv.Start(webhookMux); err != nil {
				log.Fatalf("could not start webhook worker server: %v", err)
			}

			recoveryProcessor := blnk.NewQueuedTransactionRecoveryProcessor(b.blnk)
			recoveryProcessor.Start(ctx)

			logrus.Info("Workers started.")

			// Wait for SIGINT/SIGTERM.
			<-ctx.Done()

			log.Printf("Shutdown signal received. Shutting down...")

			recoveryProcessor.Stop()

			if monitoringSrv != nil {
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := monitoringSrv.Shutdown(sctx); err != nil {
					log.Printf("monitoring shutdown error: %v", err)
				}
			}

			webhookSrv.Shutdown()
			srv.Shutdown()

			log.Printf("Shutdown complete.")
		},
	}

	return cmd
}

func setupWorkerServers(b *blnkInstance, conf *config.Configuration) (*asynq.Server, *asynq.Server, *asynq.ServeMux, *asynq.ServeMux, error) {
	queues := initializeQueues()
	webhookQueues := initializeWebhookQueues()

	srv, err := initializeWorkerServer(conf, queues)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	webhookSrv, err := initializeWebhookWorkerServer(conf, webhookQueues)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)

	webhookMux := asynq.NewServeMux()
	initializeWebhookTaskHandlers(b, webhookMux)

	return srv, webhookSrv, mux, webhookMux, nil
}

func startMonitoringServer(conf *config.Configuration) *http.Server {
	redisOption, _ := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	asynqmonHandler := asynqmon.New(asynqmon.Options{
		RootPath: "/monitoring",
		RedisConnOpt: asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
	})

	monitoringMux := http.NewServeMux()

	monitoringMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status": "UP", "service": "worker"}`)
	})

	monitoringMux.Handle("/monitoring/", asynqmonHandler)

	monitoringAddr := fmt.Sprintf(":%s", conf.Queue.MonitoringPort)
	srv := &http.Server{
		Addr:    monitoringAddr,
		Handler: monitoringMux,
	}

	go func() {
		log.Printf("Worker monitoring server listening on %s (health: /health, dashboard: /monitoring)", monitoringAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("could not start monitoring server: %v", err)
		}
	}()

	return srv
}
