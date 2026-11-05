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
	"github.com/typesense/typesense-go/typesense/api"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	defaultBackpressureRetryInterval = 5 * time.Second
	importChunkSize                  = 5000
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
	if config.BackpressureRetryInterval <= 0 {
		config.BackpressureRetryInterval = defaultBackpressureRetryInterval
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

type importFailure struct {
	Index int
	Error string
}

// importDocuments upserts docs with one request. Typesense replicates each
// HTTP write as a single Raft log entry, so this costs one commit per page
// instead of one per document. Documents are normalized the same way
// HandleNotification normalizes them, so the two paths produce identical
// documents. A document that cannot be normalized or that Typesense rejects
// is returned as an importFailure; an error means nothing in docs was indexed.
func (t *TypesenseClient) importDocuments(ctx context.Context, table string, docs []map[string]interface{}) ([]importFailure, error) {
	config, ok := collectionConfigs[table]
	if !ok {
		return nil, fmt.Errorf("unknown collection: %s", table)
	}
	var failures []importFailure
	payload := make([]interface{}, 0, len(docs))
	positions := make([]int, 0, len(docs))
	for i, data := range docs {
		if err := t.processMetadata(data); err != nil {
			failures = append(failures, importFailure{Index: i, Error: err.Error()})
			continue
		}
		t.convertLargeNumbers(config, data)
		t.ensureSchemaFields(config, data)
		t.normalizeTimeFields(config, data)
		if id, ok := data[config.IDField].(string); ok && id != "" {
			data["id"] = id
		}
		payload = append(payload, data)
		positions = append(positions, i)
	}
	if len(payload) == 0 {
		return failures, nil
	}

	action := "upsert"
	results, err := t.Client.Collection(table).Documents().Import(ctx, payload, &api.ImportDocumentsParams{Action: &action})
	if err != nil {
		return nil, fmt.Errorf("failed to import documents into Typesense: %w", err)
	}

	for i, result := range results {
		if result.Success {
			continue
		}
		failures = append(failures, importFailure{Index: positions[i], Error: result.Error})
	}
	return failures, nil
}

// importPage returns an error only when Typesense itself is unavailable; the
// count is what was accepted before that.
func (r *ReindexService) importPage(ctx context.Context, collection, label string, ids []string, docs []map[string]interface{}) (int64, error) {
	var accepted int64
	for start := 0; start < len(docs); start += importChunkSize {
		end := min(start+importChunkSize, len(docs))
		chunk := docs[start:end]

		var failures []importFailure
		err := r.retryWithMemoryBackpressure(ctx, func() error {
			var importErr error
			failures, importErr = r.client.importDocuments(ctx, collection, chunk)
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
// that fails as a whole means the server is unavailable and the phase aborts.
func (r *ReindexService) importIndividually(ctx context.Context, collection, label string, ids []string, docs []map[string]interface{}) (int64, error) {
	var accepted int64
	for i := range docs {
		var failures []importFailure
		err := r.retryWithMemoryBackpressure(ctx, func() error {
			var importErr error
			failures, importErr = r.client.importDocuments(ctx, collection, docs[i:i+1])
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
		ids := make([]string, 0, batchCount)
		docs := make([]map[string]interface{}, 0, batchCount)
		for _, ledger := range ledgers {
			data, err := toMap(ledger)
			if err != nil {
				r.addError("ledger " + ledger.LedgerID + ": " + err.Error())
				continue
			}
			ids = append(ids, ledger.LedgerID)
			docs = append(docs, data)
		}

		indexed, err := r.importPage(ctx, CollectionLedgers, "ledger", ids, docs)
		totalIndexed += indexed
		if err != nil {
			return err
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
		ids := make([]string, 0, batchCount)
		docs := make([]map[string]interface{}, 0, batchCount)
		for _, identity := range identities {
			data, err := toMap(identity)
			if err != nil {
				r.addError("identity " + identity.IdentityID + ": " + err.Error())
				continue
			}
			ids = append(ids, identity.IdentityID)
			docs = append(docs, data)
		}

		indexed, err := r.importPage(ctx, CollectionIdentities, "identity", ids, docs)
		totalIndexed += indexed
		if err != nil {
			return err
		}

		r.mu.Lock()
		r.progress.ProcessedRecords += indexed
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
		ids := make([]string, 0, batchCount)
		docs := make([]map[string]interface{}, 0, batchCount)
		for _, balance := range balances {
			data, err := toMap(balance)
			if err != nil {
				r.addError("balance " + balance.BalanceID + ": " + err.Error())
				continue
			}
			ids = append(ids, balance.BalanceID)
			docs = append(docs, data)
		}

		indexed, err := r.importPage(ctx, CollectionBalances, "balance", ids, docs)
		totalIndexed += indexed
		if err != nil {
			return err
		}

		r.mu.Lock()
		r.progress.ProcessedRecords += indexed
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
		ids := make([]string, 0, batchCount)
		docs := make([]map[string]interface{}, 0, batchCount)
		for _, transaction := range transactions {
			data, err := toMap(transaction)
			if err != nil {
				r.addError("transaction " + transaction.TransactionID + ": " + err.Error())
				continue
			}
			ids = append(ids, transaction.TransactionID)
			docs = append(docs, data)
		}

		indexed, err := r.importPage(ctx, CollectionTransactions, "transaction", ids, docs)
		totalIndexed += indexed
		if err != nil {
			return err
		}

		r.mu.Lock()
		r.progress.ProcessedRecords += indexed
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
