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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	defaultBackpressureRetryInterval = 5 * time.Second
	importChunkSize                  = 5000
	importTimeout                    = 2 * time.Minute
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
	Paused           bool       `json:"paused"`
	PauseReason      string     `json:"pause_reason,omitempty"`
	PausedAt         *time.Time `json:"paused_at,omitempty"`
}

// ReindexConfig holds configuration for reindexing.
type ReindexConfig struct {
	BatchSize                 int
	BackpressureRetryInterval time.Duration
}

// ReindexService handles reindexing operations.
type ReindexService struct {
	client       *TypesenseClient
	importClient *TypesenseClient
	datasource   database.IDataSource
	config       ReindexConfig
	progress     *ReindexProgress
	mu           sync.RWMutex
}

// NewReindexService creates a new ReindexService instance.
func NewReindexService(client *TypesenseClient, datasource database.IDataSource, config ReindexConfig) *ReindexService {
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.BackpressureRetryInterval <= 0 {
		config.BackpressureRetryInterval = defaultBackpressureRetryInterval
	}
	return &ReindexService{
		client:       client,
		importClient: client.withRequestTimeout(importTimeout),
		datasource:   datasource,
		config:       config,
		progress: &ReindexProgress{
			Status: "pending",
		},
	}
}

// GetProgress returns the current progress of the reindex operation.
func (r *ReindexService) GetProgress() ReindexProgress {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneReindexProgress(r.progress)
}

func cloneReindexProgress(progress *ReindexProgress) ReindexProgress {
	cloned := *progress
	cloned.Errors = append([]string(nil), progress.Errors...)
	if progress.CompletedAt != nil {
		completedAt := *progress.CompletedAt
		cloned.CompletedAt = &completedAt
	}
	if progress.PausedAt != nil {
		pausedAt := *progress.PausedAt
		cloned.PausedAt = &pausedAt
	}
	return cloned
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
	r.progress.Paused = false
	r.progress.PauseReason = ""
	r.progress.PausedAt = nil
	r.mu.Unlock()

	progress := r.GetProgress()
	logrus.WithFields(logrus.Fields{
		"total_records":     progress.TotalRecords,
		"processed_records": progress.ProcessedRecords,
		"duration":          time.Since(progress.StartedAt).String(),
	}).Info("Reindex operation completed")

	return r.GetProgressPtr(), nil
}

// GetProgressPtr returns a pointer to the current progress.
func (r *ReindexService) GetProgressPtr() *ReindexProgress {
	r.mu.RLock()
	defer r.mu.RUnlock()
	progress := cloneReindexProgress(r.progress)
	return &progress
}

func (r *ReindexService) failWithError(err error, phase string) (*ReindexProgress, error) {
	r.mu.Lock()
	now := time.Now()
	r.progress.Status = "failed"
	r.progress.Phase = phase
	r.progress.CompletedAt = &now
	r.progress.Errors = append(r.progress.Errors, err.Error())
	r.progress.Paused = false
	r.progress.PauseReason = ""
	r.progress.PausedAt = nil
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

	if err := r.retryWithMemoryBackpressure(ctx, func() error {
		return r.client.EnsureCollectionsExist(ctx)
	}); err != nil {
		return err
	}

	logrus.Info("All collections created successfully")
	return nil
}

func (r *ReindexService) retryWithMemoryBackpressure(ctx context.Context, operation func() error) error {
	for {
		err := operation()
		if !IsMemoryBackpressure(err) {
			r.setMemoryBackpressurePaused(false)
			return err
		}

		r.setMemoryBackpressurePaused(true)
		metrics.SearchBackpressureRetryTotal.Add(ctx, 1,
			otelmetric.WithAttributes(attribute.String("source", "reindex")),
		)
		if err := waitForRetry(ctx, r.config.BackpressureRetryInterval); err != nil {
			return err
		}
	}
}

func waitForRetry(ctx context.Context, retryInterval time.Duration) error {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *ReindexService) setMemoryBackpressurePaused(paused bool) {
	r.mu.Lock()
	if r.progress.Paused == paused {
		r.mu.Unlock()
		return
	}

	phase := r.progress.Phase
	r.progress.Paused = paused
	if paused {
		now := time.Now()
		r.progress.PauseReason = "typesense_out_of_memory"
		r.progress.PausedAt = &now
	} else {
		r.progress.PauseReason = ""
		r.progress.PausedAt = nil
	}
	r.mu.Unlock()

	if paused {
		logrus.WithField("phase", phase).Warn("Reindex paused by Typesense memory backpressure")
		return
	}
	logrus.WithField("phase", phase).Info("Reindex resumed after Typesense memory backpressure")
}

// pageFetcher returns one page of rows for a collection, with the id of each
// row in the same order, so import failures can be attributed.
type pageFetcher func(ctx context.Context, limit, offset int) (ids []string, rows []interface{}, err error)

// indexCollection records rows that cannot be mapped and documents Typesense
// rejects in progress.Errors and skips them; a datasource error or an
// unreachable Typesense aborts the phase.
func (r *ReindexService) indexCollection(ctx context.Context, phase, collection, label string, fetch pageFetcher) error {
	r.mu.Lock()
	r.progress.Phase = phase
	r.mu.Unlock()
	logrus.Infof("Starting to index %s", collection)

	var offset int
	var totalIndexed int64
	pageNum := 0

	for {
		ids, rows, err := fetch(ctx, r.config.BatchSize, offset)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		docIDs := make([]string, 0, len(rows))
		docs := make([]map[string]interface{}, 0, len(rows))
		for i, row := range rows {
			data, err := toMap(row)
			if err != nil {
				r.addError(label + " " + ids[i] + ": " + err.Error())
				continue
			}
			docIDs = append(docIDs, ids[i])
			docs = append(docs, data)
		}

		indexed, err := r.importPage(ctx, collection, label, docIDs, docs)
		totalIndexed += indexed
		r.mu.Lock()
		r.progress.ProcessedRecords += indexed
		r.progress.TotalRecords = r.progress.ProcessedRecords
		r.mu.Unlock()
		if err != nil {
			return err
		}

		pageNum++
		if pageNum%100 == 0 {
			logrus.WithFields(logrus.Fields{
				"collection": collection,
				"page":       pageNum,
				"indexed":    totalIndexed,
			}).Info("Indexing progress")
		}

		offset += len(rows)
	}

	logrus.WithFields(logrus.Fields{"collection": collection, "total": totalIndexed}).Info("Indexing completed")
	return nil
}

// importPage records per-document rejections and leaves them out of the
// count. A chunk whose bulk import fails as a whole is retried one document
// at a time so only the documents at fault are lost; an error returned here
// means Typesense itself is unavailable, and the count is what was accepted
// before that.
func (r *ReindexService) importPage(ctx context.Context, collection, label string, ids []string, docs []map[string]interface{}) (int64, error) {
	var accepted int64
	for start := 0; start < len(docs); start += importChunkSize {
		end := min(start+importChunkSize, len(docs))
		chunk := docs[start:end]

		var failures []importFailure
		err := r.retryWithMemoryBackpressure(ctx, func() error {
			var importErr error
			failures, importErr = r.importClient.importDocuments(ctx, collection, chunk)
			return importErr
		})
		if err != nil {
			if ctx.Err() != nil {
				return accepted, ctx.Err()
			}
			logrus.WithError(err).WithFields(logrus.Fields{
				"collection": collection,
				"documents":  len(chunk),
			}).Warn("Bulk import failed; indexing the chunk one document at a time")
			indexed, err := r.importIndividually(ctx, collection, label, ids[start:end], chunk)
			accepted += indexed
			if err != nil {
				return accepted, err
			}
			continue
		}

		for _, f := range failures {
			r.addError(label + " " + ids[start+f.Index] + ": " + f.Error)
		}
		accepted += int64(len(chunk) - len(failures))
	}
	return accepted, nil
}

// importIndividually relies on Typesense reporting a bad document as a
// per-line result, never as a failed request, so a single-document import
// that fails as a whole means the server is unavailable. Continuing would
// only record every remaining row as an error, so the phase aborts.
func (r *ReindexService) importIndividually(ctx context.Context, collection, label string, ids []string, docs []map[string]interface{}) (int64, error) {
	var accepted int64
	for i := range docs {
		var failures []importFailure
		err := r.retryWithMemoryBackpressure(ctx, func() error {
			var importErr error
			failures, importErr = r.importClient.importDocuments(ctx, collection, docs[i:i+1])
			return importErr
		})
		if err != nil {
			return accepted, fmt.Errorf("failed to index %s %s: %w", label, ids[i], err)
		}
		if len(failures) > 0 {
			r.addError(label + " " + ids[i] + ": " + failures[0].Error)
			continue
		}
		accepted++
	}
	return accepted, nil
}

func (r *ReindexService) indexLedgers(ctx context.Context) error {
	return r.indexCollection(ctx, "indexing_ledgers", CollectionLedgers, "ledger",
		func(_ context.Context, limit, offset int) ([]string, []interface{}, error) {
			ledgers, err := r.datasource.GetAllLedgers(limit, offset)
			if err != nil {
				return nil, nil, err
			}
			ids := make([]string, len(ledgers))
			rows := make([]interface{}, len(ledgers))
			for i, l := range ledgers {
				ids[i], rows[i] = l.LedgerID, l
			}
			return ids, rows, nil
		})
}

func (r *ReindexService) indexIdentities(ctx context.Context) error {
	return r.indexCollection(ctx, "indexing_identities", CollectionIdentities, "identity",
		func(_ context.Context, limit, offset int) ([]string, []interface{}, error) {
			identities, err := r.datasource.GetAllIdentitiesPaginated(limit, offset)
			if err != nil {
				return nil, nil, err
			}
			ids := make([]string, len(identities))
			rows := make([]interface{}, len(identities))
			for i, id := range identities {
				ids[i], rows[i] = id.IdentityID, id
			}
			return ids, rows, nil
		})
}

func (r *ReindexService) indexBalances(ctx context.Context) error {
	return r.indexCollection(ctx, "indexing_balances", CollectionBalances, "balance",
		func(_ context.Context, limit, offset int) ([]string, []interface{}, error) {
			balances, err := r.datasource.GetAllBalances(limit, offset)
			if err != nil {
				return nil, nil, err
			}
			ids := make([]string, len(balances))
			rows := make([]interface{}, len(balances))
			for i, b := range balances {
				ids[i], rows[i] = b.BalanceID, b
			}
			return ids, rows, nil
		})
}

func (r *ReindexService) indexTransactions(ctx context.Context) error {
	return r.indexCollection(ctx, "indexing_transactions", CollectionTransactions, "transaction",
		func(ctx context.Context, limit, offset int) ([]string, []interface{}, error) {
			transactions, err := r.datasource.GetAllTransactions(ctx, limit, offset)
			if err != nil {
				return nil, nil, err
			}
			ids := make([]string, len(transactions))
			rows := make([]interface{}, len(transactions))
			for i, t := range transactions {
				ids[i], rows[i] = t.TransactionID, t
			}
			return ids, rows, nil
		})
}

// DropCollection deletes a collection from Typesense. A missing collection is
// not an error; Typesense reports it variously as "not found", "Not Found",
// or a 404 with "No collection with name X found." depending on version.
func (t *TypesenseClient) DropCollection(ctx context.Context, collectionName string) error {
	_, err := t.Client.Collection(collectionName).Delete(ctx)
	if err != nil &&
		!strings.Contains(err.Error(), "not found") &&
		!strings.Contains(err.Error(), "Not Found") &&
		!strings.Contains(err.Error(), "status: 404") {
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

func dbHasData(ctx context.Context, ds database.IDataSource) bool {
	ledgers, err := ds.GetAllLedgers(2, 0)
	if err != nil {
		logrus.WithError(err).Debug("could not check ledgers for reindex")
		return false
	}
	if len(ledgers) > 1 {
		return true
	}

	txns, err := ds.GetAllTransactions(ctx, 1, 0)
	if err != nil {
		logrus.WithError(err).Debug("could not check transactions for reindex")
		return false
	}
	if len(txns) > 0 {
		return true
	}

	balances, err := ds.GetAllBalances(1, 0)
	if err != nil {
		logrus.WithError(err).Debug("could not check balances for reindex")
		return false
	}
	if len(balances) > 0 {
		return true
	}

	identities, err := ds.GetAllIdentitiesPaginated(1, 0)
	if err != nil {
		logrus.WithError(err).Debug("could not check identities for reindex")
		return false
	}
	return len(identities) > 0
}

func typesenseHasData(ctx context.Context, client *TypesenseClient) bool {
	resp, err := client.Client.Collection(CollectionTransactions).Retrieve(ctx)
	if err != nil {
		logrus.WithError(err).Debug("could not retrieve transactions collection for reindex")
		return true
	}
	if resp.NumDocuments == nil {
		return false
	}
	return *resp.NumDocuments > 0
}

func shouldReindex(ctx context.Context, client *TypesenseClient, ds database.IDataSource) bool {
	if !dbHasData(ctx, ds) {
		return false
	}
	if typesenseHasData(ctx, client) {
		return false
	}
	return true
}

func TryReindexIfNeeded(ctx context.Context, client *TypesenseClient, ds database.IDataSource) {
	if client == nil || ds == nil {
		return
	}
	if !shouldReindex(ctx, client, ds) {
		return
	}

	logrus.Info("Database has data but Typesense is empty, triggering one-time reindex")
	go func() {
		reindexCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 1000})
		_, err := svc.StartReindex(reindexCtx)
		if err != nil {
			logrus.WithError(err).Error("reindex failed")
			return
		}
		logrus.Info("reindex completed successfully")
	}()
}
