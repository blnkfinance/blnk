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

package search

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/database"
	"github.com/sirupsen/logrus"
)

// ReindexProgress tracks the progress of a reindex operation.
type ReindexProgress struct {
	Status           string     `json:"status"` // "in_progress", "completed", "failed"
	Phase            string     `json:"phase"`  // "drop_collections", "indexing_ledgers", etc.
	TotalRecords     int64      `json:"total_records"`
	ProcessedRecords int64      `json:"processed_records"`
	Errors           []string   `json:"errors,omitempty"`
	StartedAt        time.Time  `json:"started_at"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
}

// ReindexConfig holds configuration for reindexing.
type ReindexConfig struct {
	BatchSize int
}

// ReindexService handles reindexing operations.
type ReindexService struct {
	client     *TypesenseClient
	datasource database.IDataSource
	config     ReindexConfig
	progress   *ReindexProgress
	mu         sync.RWMutex
}

// NewReindexService creates a new ReindexService instance.
func NewReindexService(client *TypesenseClient, datasource database.IDataSource, config ReindexConfig) *ReindexService {
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	return &ReindexService{
		client:     client,
		datasource: datasource,
		config:     config,
		progress: &ReindexProgress{
			Status: "pending",
		},
	}
}

// GetProgress returns the current progress of the reindex operation.
func (r *ReindexService) GetProgress() ReindexProgress {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.progress
}

func (r *ReindexService) updateProgress(phase string, processed int64, total int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress.Phase = phase
	r.progress.ProcessedRecords = processed
	r.progress.TotalRecords = total
}

func (r *ReindexService) addError(err string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress.Errors = append(r.progress.Errors, err)
}

// StartReindex performs a complete reindex of all data.
// It drops all collections, recreates them, and indexes data in order:
// ledgers -> identities -> balances -> transactions
func (r *ReindexService) StartReindex(ctx context.Context) (*ReindexProgress, error) {
	r.mu.Lock()
	r.progress = &ReindexProgress{
		Status:    "in_progress",
		Phase:     "starting",
		StartedAt: time.Now(),
	}
	r.mu.Unlock()

	logrus.Info("Starting reindex operation")

	if err := r.dropCollections(ctx); err != nil {
		return r.failWithError(err, "drop_collections")
	}

	if err := r.createCollections(ctx); err != nil {
		return r.failWithError(err, "create_collections")
	}

	if err := r.indexLedgers(ctx); err != nil {
		return r.failWithError(err, "indexing_ledgers")
	}

	if err := r.indexIdentities(ctx); err != nil {
		return r.failWithError(err, "indexing_identities")
	}

	if err := r.indexBalances(ctx); err != nil {
		return r.failWithError(err, "indexing_balances")
	}

	if err := r.indexTransactions(ctx); err != nil {
		return r.failWithError(err, "indexing_transactions")
	}

	r.mu.Lock()
	now := time.Now()
	r.progress.Status = "completed"
	r.progress.Phase = "done"
	r.progress.CompletedAt = &now
	r.mu.Unlock()

	logrus.WithFields(logrus.Fields{
		"total_records":     r.progress.TotalRecords,
		"processed_records": r.progress.ProcessedRecords,
		"duration":          time.Since(r.progress.StartedAt).String(),
	}).Info("Reindex operation completed")

	return r.GetProgressPtr(), nil
}

// GetProgressPtr returns a pointer to the current progress.
func (r *ReindexService) GetProgressPtr() *ReindexProgress {
	r.mu.RLock()
	defer r.mu.RUnlock()
	progress := *r.progress
	return &progress
}

func (r *ReindexService) failWithError(err error, phase string) (*ReindexProgress, error) {
	r.mu.Lock()
	now := time.Now()
	r.progress.Status = "failed"
	r.progress.Phase = phase
	r.progress.CompletedAt = &now
	r.progress.Errors = append(r.progress.Errors, err.Error())
	r.mu.Unlock()

	logrus.WithError(err).WithField("phase", phase).Error("Reindex operation failed")
	return r.GetProgressPtr(), err
}

func (r *ReindexService) dropCollections(ctx context.Context) error {
	r.updateProgress("drop_collections", 0, 0)
	logrus.Info("Dropping all collections")

	if err := r.client.DropAllCollections(ctx); err != nil {
		return err
	}

	logrus.Info("All collections dropped successfully")
	return nil
}

func (r *ReindexService) createCollections(ctx context.Context) error {
	r.updateProgress("create_collections", 0, 0)
	logrus.Info("Creating collections")

	if err := r.client.EnsureCollectionsExist(ctx); err != nil {
		return err
	}

	logrus.Info("All collections created successfully")
	return nil
}

func (r *ReindexService) indexLedgers(ctx context.Context) error {
	r.updateProgress("indexing_ledgers", 0, 0)
	logrus.Info("Starting to index ledgers")

	var offset int
	var totalIndexed int64
	batchNum := 0

	for {
		ledgers, err := r.datasource.GetAllLedgers(r.config.BatchSize, offset)
		if err != nil {
			return err
		}

		if len(ledgers) == 0 {
			break
		}

		batchCount := len(ledgers)
		for _, ledger := range ledgers {
			data, err := toMap(ledger)
			if err != nil {
				r.addError("ledger " + ledger.LedgerID + ": " + err.Error())
				continue
			}

			if err := r.client.HandleNotification(ctx, CollectionLedgers, data); err != nil {
				r.addError("ledger " + ledger.LedgerID + ": " + err.Error())
				continue
			}
			totalIndexed++
		}

		r.updateProgress("indexing_ledgers", totalIndexed, totalIndexed)

		batchNum++
		if batchNum%100 == 0 {
			logrus.WithFields(logrus.Fields{
				"batch":   batchNum,
				"indexed": totalIndexed,
			}).Info("Ledger indexing progress")
		}

		offset += batchCount
	}

	logrus.WithField("total", totalIndexed).Info("Ledger indexing completed")
	return nil
}

func (r *ReindexService) indexIdentities(ctx context.Context) error {
	r.updateProgress("indexing_identities", r.progress.ProcessedRecords, r.progress.TotalRecords)
	logrus.Info("Starting to index identities")

	var offset int
	var totalIndexed int64
	batchNum := 0

	for {
		identities, err := r.datasource.GetAllIdentitiesPaginated(r.config.BatchSize, offset)
		if err != nil {
			return err
		}

		if len(identities) == 0 {
			break
		}

		batchCount := len(identities)
		for _, identity := range identities {
			data, err := toMap(identity)
			if err != nil {
				r.addError("identity " + identity.IdentityID + ": " + err.Error())
				continue
			}

			if err := r.client.HandleNotification(ctx, CollectionIdentities, data); err != nil {
				r.addError("identity " + identity.IdentityID + ": " + err.Error())
				continue
			}
			totalIndexed++
		}

		r.mu.Lock()
		r.progress.ProcessedRecords += totalIndexed
		r.progress.TotalRecords = r.progress.ProcessedRecords
		r.mu.Unlock()

		batchNum++
		if batchNum%100 == 0 {
			logrus.WithFields(logrus.Fields{
				"batch":   batchNum,
				"indexed": totalIndexed,
			}).Info("Identity indexing progress")
		}

		offset += batchCount
	}

	logrus.WithField("total", totalIndexed).Info("Identity indexing completed")
	return nil
}

func (r *ReindexService) indexBalances(ctx context.Context) error {
	r.updateProgress("indexing_balances", r.progress.ProcessedRecords, r.progress.TotalRecords)
	logrus.Info("Starting to index balances")

	var offset int
	var totalIndexed int64
	batchNum := 0

	for {
		balances, err := r.datasource.GetAllBalances(r.config.BatchSize, offset)
		if err != nil {
			return err
		}

		if len(balances) == 0 {
			break
		}

		batchCount := len(balances)
		for _, balance := range balances {
			data, err := toMap(balance)
			if err != nil {
				r.addError("balance " + balance.BalanceID + ": " + err.Error())
				continue
			}

			if err := r.client.HandleNotification(ctx, CollectionBalances, data); err != nil {
				r.addError("balance " + balance.BalanceID + ": " + err.Error())
				continue
			}
			totalIndexed++
		}

		r.mu.Lock()
		r.progress.ProcessedRecords += totalIndexed
		r.progress.TotalRecords = r.progress.ProcessedRecords
		r.mu.Unlock()

		batchNum++
		if batchNum%100 == 0 {
			logrus.WithFields(logrus.Fields{
				"batch":   batchNum,
				"indexed": totalIndexed,
			}).Info("Balance indexing progress")
		}

		offset += batchCount
	}

	logrus.WithField("total", totalIndexed).Info("Balance indexing completed")
	return nil
}

func (r *ReindexService) indexTransactions(ctx context.Context) error {
	r.updateProgress("indexing_transactions", r.progress.ProcessedRecords, r.progress.TotalRecords)
	logrus.Info("Starting to index transactions")

	var offset int
	var totalIndexed int64
	batchNum := 0

	for {
		transactions, err := r.datasource.GetAllTransactions(ctx, r.config.BatchSize, offset)
		if err != nil {
			return err
		}

		if len(transactions) == 0 {
			break
		}

		batchCount := len(transactions)
		for _, transaction := range transactions {
			data, err := toMap(transaction)
			if err != nil {
				r.addError("transaction " + transaction.TransactionID + ": " + err.Error())
				continue
			}

			if err := r.client.HandleNotification(ctx, CollectionTransactions, data); err != nil {
				r.addError("transaction " + transaction.TransactionID + ": " + err.Error())
				continue
			}
			totalIndexed++
		}

		r.mu.Lock()
		r.progress.ProcessedRecords += totalIndexed
		r.progress.TotalRecords = r.progress.ProcessedRecords
		r.mu.Unlock()

		batchNum++
		if batchNum%100 == 0 {
			logrus.WithFields(logrus.Fields{
				"batch":   batchNum,
				"indexed": totalIndexed,
			}).Info("Transaction indexing progress")
		}

		offset += batchCount
	}

	logrus.WithField("total", totalIndexed).Info("Transaction indexing completed")
	return nil
}

// DropCollection deletes a collection from Typesense.
func (t *TypesenseClient) DropCollection(ctx context.Context, collectionName string) error {
	_, err := t.Client.Collection(collectionName).Delete(ctx)
	if err != nil && !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "Not Found") {
		return err
	}
	return nil
}

// DropAllCollections drops all known collections from Typesense.
func (t *TypesenseClient) DropAllCollections(ctx context.Context) error {
	collections := []string{
		CollectionLedgers,
		CollectionIdentities,
		CollectionBalances,
		CollectionTransactions,
		CollectionReconciliations,
	}

	for _, c := range collections {
		logrus.WithField("collection", c).Debug("Dropping collection")
		if err := t.DropCollection(ctx, c); err != nil {
			return err
		}
	}

	return nil
}
