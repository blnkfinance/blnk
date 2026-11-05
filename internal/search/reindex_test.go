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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/typesense/typesense-go/typesense/api"
)

func testLedger(id string) model.Ledger {
	return model.Ledger{LedgerID: id, Name: "ledger " + id, CreatedAt: time.Now().UTC()}
}

func testIdentity(id string) model.Identity {
	return model.Identity{IdentityID: id, FirstName: "Re", LastName: "Index", CreatedAt: time.Now().UTC()}
}

func testBalance(id, ledgerID string) model.Balance {
	return model.Balance{
		BalanceID:             id,
		LedgerID:              ledgerID,
		Currency:              "USD",
		Balance:               big.NewInt(1000),
		CreditBalance:         big.NewInt(1000),
		DebitBalance:          big.NewInt(0),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		CreatedAt:             time.Now().UTC(),
	}
}

func testTransaction(id, source, destination string) model.Transaction {
	return model.Transaction{
		TransactionID: id,
		Source:        source,
		Destination:   destination,
		Reference:     "ref_" + id,
		Currency:      "USD",
		Amount:        10,
		PreciseAmount: big.NewInt(1000),
		Status:        "APPLIED",
		CreatedAt:     time.Now().UTC(),
	}
}

func TestNewReindexService_ClampsBatchSize(t *testing.T) {
	for _, size := range []int{0, -5} {
		svc := NewReindexService(nil, nil, ReindexConfig{BatchSize: size})
		assert.Equal(t, 1000, svc.config.BatchSize, "batch size %d should clamp to default", size)
		assert.Equal(t, 5*time.Second, svc.config.BackpressureRetryInterval)
	}
	svc := NewReindexService(nil, nil, ReindexConfig{
		BatchSize:                 25,
		BackpressureRetryInterval: 2 * time.Second,
	})
	assert.Equal(t, 25, svc.config.BatchSize)
	assert.Equal(t, 2*time.Second, svc.config.BackpressureRetryInterval)
	assert.Equal(t, "pending", svc.GetProgress().Status)
}

func TestGetProgress_ReturnsCopy(t *testing.T) {
	svc := NewReindexService(nil, nil, ReindexConfig{})

	copy1 := svc.GetProgress()
	copy1.Status = "mutated"
	assert.Equal(t, "pending", svc.GetProgress().Status, "mutating the returned copy must not affect internal state")

	ptr := svc.GetProgressPtr()
	ptr.Status = "mutated"
	assert.Equal(t, "pending", svc.GetProgress().Status, "GetProgressPtr must also return a detached copy")
}

func TestRetryWithMemoryBackpressurePausesAndResumes(t *testing.T) {
	svc := NewReindexService(nil, nil, ReindexConfig{BackpressureRetryInterval: time.Millisecond})
	svc.updateProgress("indexing_ledgers", 0, 0)

	attempts := 0
	err := svc.retryWithMemoryBackpressure(context.Background(), func() error {
		attempts++
		if attempts == 1 {
			return errors.New("Rejecting write: running out of resource type: OUT_OF_MEMORY")
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	progress := svc.GetProgress()
	assert.False(t, progress.Paused)
	assert.Empty(t, progress.PauseReason)
	assert.Nil(t, progress.PausedAt)
	assert.Empty(t, progress.Errors)
}

func TestRetryWithMemoryBackpressureRespectsContextCancellation(t *testing.T) {
	svc := NewReindexService(nil, nil, ReindexConfig{BackpressureRetryInterval: time.Hour})
	svc.updateProgress("indexing_transactions", 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- svc.retryWithMemoryBackpressure(ctx, func() error {
			return errors.New("resource type: OUT_OF_MEMORY")
		})
	}()

	require.Eventually(t, func() bool {
		return svc.GetProgress().Paused
	}, time.Second, time.Millisecond)
	progress := svc.GetProgress()
	assert.Equal(t, "typesense_out_of_memory", progress.PauseReason)
	assert.NotNil(t, progress.PausedAt)

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("backpressure retry did not stop after context cancellation")
	}
}

func TestStartReindex_HappyPath(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()

	ledgerID := "ldg_" + gofakeit.UUID()
	identityID := "idt_" + gofakeit.UUID()
	balSrc := "bln_" + gofakeit.UUID()
	balDst := "bln_" + gofakeit.UUID()
	txnID := "txn_" + gofakeit.UUID()

	batchSize := 2
	ds := new(mocks.MockDataSource)
	// Two pages of ledgers (2 + 1) then the empty terminator page.
	ds.On("GetAllLedgers", batchSize, 0).Return([]model.Ledger{testLedger(ledgerID), testLedger("ldg_" + gofakeit.UUID())}, nil).Once()
	ds.On("GetAllLedgers", batchSize, 2).Return([]model.Ledger{testLedger("ldg_" + gofakeit.UUID())}, nil).Once()
	ds.On("GetAllLedgers", batchSize, 3).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", batchSize, 0).Return([]model.Identity{testIdentity(identityID)}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", batchSize, 1).Return([]model.Identity{}, nil).Once()
	ds.On("GetAllBalances", batchSize, 0).Return([]model.Balance{testBalance(balSrc, ledgerID), testBalance(balDst, ledgerID)}, nil).Once()
	ds.On("GetAllBalances", batchSize, 2).Return([]model.Balance{}, nil).Once()
	ds.On("GetAllTransactions", batchSize, 0).Return([]model.Transaction{testTransaction(txnID, balSrc, balDst)}, nil).Once()
	ds.On("GetAllTransactions", batchSize, 1).Return([]model.Transaction{}, nil).Once()

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: batchSize})

	// Hammer GetProgress concurrently while the reindex runs; the -race
	// detector is the assertion for the progress mutex.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = svc.GetProgress()
			}
		}
	}()

	progress, err := svc.StartReindex(ctx)
	close(stop)

	require.NoError(t, err)
	require.NotNil(t, progress)
	assert.Equal(t, "completed", progress.Status)
	assert.Equal(t, "done", progress.Phase)
	assert.NotNil(t, progress.CompletedAt)
	assert.Empty(t, progress.Errors)
	ds.AssertExpectations(t)

	// The reindexed documents must actually be searchable.
	for id, spec := range map[string]struct{ collection, field string }{
		ledgerID:   {CollectionLedgers, "ledger_id"},
		identityID: {CollectionIdentities, "identity_id"},
		balSrc:     {CollectionBalances, "balance_id"},
		txnID:      {CollectionTransactions, "transaction_id"},
	} {
		result, err := client.Search(ctx, spec.collection, &api.SearchCollectionParams{Q: id, QueryBy: spec.field})
		require.NoError(t, err)
		require.NotNil(t, result.Found)
		assert.Equal(t, 1, *result.Found, "%s should be indexed in %s after reindex", id, spec.collection)
	}
}

func TestStartReindex_DataSourceFailureMarksFailed(t *testing.T) {
	client := newTestTypesenseClient(t)

	batchSize := 5
	ledgerID := "ldg_" + gofakeit.UUID()
	ds := new(mocks.MockDataSource)
	ds.On("GetAllLedgers", batchSize, 0).Return([]model.Ledger{testLedger(ledgerID)}, nil).Once()
	ds.On("GetAllLedgers", batchSize, 1).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", batchSize, 0).Return([]model.Identity(nil), errors.New("identities table unavailable")).Once()

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: batchSize})
	progress, err := svc.StartReindex(context.Background())

	require.Error(t, err)
	require.NotNil(t, progress)
	assert.Equal(t, "failed", progress.Status, "a datasource failure must not leave the reindex in_progress")
	assert.Equal(t, "indexing_identities", progress.Phase)
	assert.NotNil(t, progress.CompletedAt)
	require.NotEmpty(t, progress.Errors)
	assert.Contains(t, progress.Errors[0], "identities table unavailable")
	ds.AssertExpectations(t)

	// Earlier phases still ran: the ledger indexed before the failure exists.
	result, searchErr := client.Search(context.Background(), CollectionLedgers, &api.SearchCollectionParams{Q: ledgerID, QueryBy: "ledger_id"})
	require.NoError(t, searchErr)
	assert.Equal(t, 1, *result.Found)
}

func TestStartReindex_TypesenseDownFails(t *testing.T) {
	// A dead Typesense must fail the run at the first phase, not hang or
	// report in_progress forever.
	client := NewTypesenseClient("any-key", []string{"http://localhost:1"}, DefaultTimeout)
	ds := new(mocks.MockDataSource)

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10})
	progress, err := svc.StartReindex(context.Background())

	require.Error(t, err)
	assert.Equal(t, "failed", progress.Status)
	assert.Equal(t, "drop_collections", progress.Phase)
	ds.AssertNotCalled(t, "GetAllLedgers")
}

func TestShouldReindex(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client) // transactions collection exists and is empty

	t.Run("db has data and typesense empty -> reindex", func(t *testing.T) {
		ds := new(mocks.MockDataSource)
		// Two ledgers (more than just the default general ledger) signal data.
		ds.On("GetAllLedgers", 2, 0).Return([]model.Ledger{testLedger("a"), testLedger("b")}, nil).Once()
		assert.True(t, shouldReindex(ctx, client, ds))
		ds.AssertExpectations(t)
	})

	t.Run("empty db -> no reindex", func(t *testing.T) {
		ds := new(mocks.MockDataSource)
		ds.On("GetAllLedgers", 2, 0).Return([]model.Ledger{testLedger("only-general")}, nil).Once()
		ds.On("GetAllTransactions", 1, 0).Return([]model.Transaction{}, nil).Once()
		ds.On("GetAllBalances", 1, 0).Return([]model.Balance{}, nil).Once()
		ds.On("GetAllIdentitiesPaginated", 1, 0).Return([]model.Identity{}, nil).Once()
		assert.False(t, shouldReindex(ctx, client, ds))
		ds.AssertExpectations(t)
	})

	t.Run("typesense already has data -> no reindex", func(t *testing.T) {
		// Transactions reference balances, so the balance must exist first
		// (Typesense enforces reference integrity on insert).
		balID := "bln_existing_" + gofakeit.UUID()
		require.NoError(t, client.HandleNotification(ctx, CollectionBalances, map[string]interface{}{
			"balance_id": balID,
			"ledger_id":  "general_ledger_id",
			"currency":   "USD",
			"created_at": time.Now(),
		}))
		require.NoError(t, client.HandleNotification(ctx, CollectionTransactions, map[string]interface{}{
			"transaction_id": "txn_existing_" + gofakeit.UUID(),
			"source":         balID,
			"destination":    balID,
			"created_at":     time.Now(),
		}))
		ds := new(mocks.MockDataSource)
		ds.On("GetAllLedgers", 2, 0).Return([]model.Ledger{testLedger("a"), testLedger("b")}, nil).Once()
		assert.False(t, shouldReindex(ctx, client, ds))
		ds.AssertExpectations(t)
	})
}

func TestTryReindexIfNeeded_NilArgsAreSafe(t *testing.T) {
	// Must not panic and must not touch the datasource.
	TryReindexIfNeeded(context.Background(), nil, nil)

	ds := new(mocks.MockDataSource)
	TryReindexIfNeeded(context.Background(), nil, ds)
	ds.AssertNotCalled(t, "GetAllLedgers")
}

// fakeTypesense answers the bulk indexing endpoint; collection create returns 201 and
// everything else an empty 200, which is enough for DropAllCollections and
// EnsureCollectionsExist to pass through.
type fakeTypesense struct {
	mu       sync.Mutex
	received [][]map[string]interface{} // documents received per bulk request
	respond  func(call int, docs []map[string]interface{}) (status int, body string)
}

func (f *fakeTypesense) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/documents/import") {
			w.Header().Set("Content-Type", "application/json")
			// typesense-go treats anything but 201 as a failed create/upsert.
			if r.Method == http.MethodPost && (r.URL.Path == "/collections" || strings.HasSuffix(r.URL.Path, "/documents")) {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"name":"fake","fields":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var docs []map[string]interface{}
		scanner := bufio.NewScanner(r.Body)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			var doc map[string]interface{}
			if err := json.Unmarshal(scanner.Bytes(), &doc); err == nil {
				docs = append(docs, doc)
			}
		}
		f.mu.Lock()
		call := len(f.received)
		f.received = append(f.received, docs)
		f.mu.Unlock()

		status, body := http.StatusOK, ""
		if f.respond != nil {
			status, body = f.respond(call, docs)
		}
		if body == "" {
			lines := make([]string, len(docs))
			for i := range lines {
				lines[i] = `{"success":true}`
			}
			body = strings.Join(lines, "\n")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func (f *fakeTypesense) requests() [][]map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]map[string]interface{}, len(f.received))
	copy(out, f.received)
	return out
}

func newFakeTypesense(t *testing.T, fake *fakeTypesense) *TypesenseClient {
	t.Helper()
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)
	return NewTypesenseClient("test-key", []string{srv.URL}, DefaultTimeout)
}

func expectLedgersOnly(ds *mocks.MockDataSource, batchSize int, ledgers []model.Ledger) {
	ds.On("GetAllLedgers", batchSize, 0).Return(ledgers, nil).Once()
	ds.On("GetAllLedgers", batchSize, len(ledgers)).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", batchSize, 0).Return([]model.Identity{}, nil).Once()
	ds.On("GetAllBalances", batchSize, 0).Return([]model.Balance{}, nil).Once()
	ds.On("GetAllTransactions", batchSize, 0).Return([]model.Transaction{}, nil).Once()
}

func rejectedLine(field string) string {
	return `{"success":false,"error":"Field ` + "`" + field + "`" + ` must be a string.","document":"{}"}`
}

func TestIndexDocuments_NormalizesAndSetsID(t *testing.T) {
	fake := &fakeTypesense{}
	client := newFakeTypesense(t, fake)

	created := time.Date(2026, 9, 22, 19, 51, 0, 0, time.UTC)
	docs := []map[string]interface{}{
		{"transaction_id": "txn_1", "created_at": created.Format(time.RFC3339), "precise_amount": float64(1e15), "meta_data": nil},
	}
	failures, err := client.indexDocuments(context.Background(), CollectionTransactions, docs)
	require.NoError(t, err)
	assert.Empty(t, failures)

	calls := fake.requests()
	require.Len(t, calls, 1)
	require.Len(t, calls[0], 1)
	sent := calls[0][0]
	assert.Equal(t, "txn_1", sent["id"], "Typesense document id must come from the collection's IDField")
	assert.EqualValues(t, created.Unix(), sent["created_at"], "time fields are normalized to unix seconds like HandleNotification")
	assert.Equal(t, "1000000000000000", sent["precise_amount"], "big-int fields are sent as strings")
	assert.Equal(t, map[string]interface{}{}, sent["meta_data"], "nil meta_data becomes an empty object")
	assert.Contains(t, sent, "amount", "required schema fields are defaulted")
}

func TestStartReindex_IndexesPagesInChunks(t *testing.T) {
	fake := &fakeTypesense{}
	client := newFakeTypesense(t, fake)

	ledgers := make([]model.Ledger, indexChunkSize+1)
	for i := range ledgers {
		ledgers[i] = testLedger(fmt.Sprintf("ldg_%d", i))
	}
	ds := new(mocks.MockDataSource)
	expectLedgersOnly(ds, len(ledgers), ledgers)

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: len(ledgers)})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, len(ledgers), progress.ProcessedRecords)
	assert.Empty(t, progress.Errors)

	calls := fake.requests()
	require.Len(t, calls, 2, "a page larger than the chunk size is split across requests")
	assert.Len(t, calls[0], indexChunkSize)
	assert.Len(t, calls[1], 1)
	ds.AssertExpectations(t)
}

func TestStartReindex_RejectedDocumentIsRecordedNotFatal(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, docs []map[string]interface{}) (int, string) {
		lines := make([]string, len(docs))
		for i, d := range docs {
			lines[i] = `{"success":true}`
			if d["id"] == "ldg_bad" {
				lines[i] = rejectedLine("name")
			}
		}
		return http.StatusOK, strings.Join(lines, "\n")
	}}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	expectLedgersOnly(ds, 10, []model.Ledger{testLedger("ldg_ok"), testLedger("ldg_bad"), testLedger("ldg_ok2")})

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, 2, progress.ProcessedRecords, "the rejected document is not counted as indexed")
	require.Len(t, progress.Errors, 1)
	assert.Contains(t, progress.Errors[0], "ledger ldg_bad")
	assert.Contains(t, progress.Errors[0], "must be a string")
}

func TestStartReindex_PausesOnOutOfMemoryThenResumes(t *testing.T) {
	fake := &fakeTypesense{respond: func(call int, _ []map[string]interface{}) (int, string) {
		if call == 0 {
			return http.StatusServiceUnavailable, `{"message":"Rejecting write: running out of resource type: OUT_OF_MEMORY"}`
		}
		return http.StatusOK, ""
	}}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	expectLedgersOnly(ds, 10, []model.Ledger{testLedger("ldg_1")})

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10, BackpressureRetryInterval: 10 * time.Millisecond})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.False(t, progress.Paused)
	assert.EqualValues(t, 1, progress.ProcessedRecords)
	assert.Len(t, fake.requests(), 2, "the rejected chunk is re-sent after the backpressure wait")
}

func TestStartReindex_BulkFailureFallsBackToSingleDocuments(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, docs []map[string]interface{}) (int, string) {
		if len(docs) > 1 {
			return http.StatusBadRequest, `{"message":"malformed"}`
		}
		if docs[0]["id"] == "ldg_bad" {
			return http.StatusOK, rejectedLine("name")
		}
		return http.StatusOK, ""
	}}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	expectLedgersOnly(ds, 10, []model.Ledger{testLedger("ldg_ok"), testLedger("ldg_bad"), testLedger("ldg_ok2")})

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, 2, progress.ProcessedRecords, "only the rejected document is skipped")
	require.Len(t, progress.Errors, 1)
	assert.Contains(t, progress.Errors[0], "ledger ldg_bad")

	calls := fake.requests()
	require.Len(t, calls, 4, "one failed bulk request, then one request per document")
	assert.Len(t, calls[0], 3)
	assert.Equal(t, "ldg_ok", calls[1][0]["id"])
	assert.Equal(t, "ldg_bad", calls[2][0]["id"])
	assert.Equal(t, "ldg_ok2", calls[3][0]["id"])
}

func TestStartReindex_FallbackAbortsWhenTypesenseIsDown(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, _ []map[string]interface{}) (int, string) {
		return http.StatusInternalServerError, `{"message":"disk full"}`
	}}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	ds.On("GetAllLedgers", 10, 0).Return([]model.Ledger{testLedger("ldg_1"), testLedger("ldg_2")}, nil).Once()

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10})
	progress, err := svc.StartReindex(context.Background())
	require.Error(t, err)
	assert.Equal(t, "failed", progress.Status)
	assert.Equal(t, "indexing_ledgers", progress.Phase)
	assert.Len(t, fake.requests(), 2, "the failed bulk request and the first single-document request stop the phase")
	ds.AssertNotCalled(t, "GetAllIdentitiesPaginated")
}
