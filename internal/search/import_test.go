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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTypesense answers the import endpoint; collection create returns 201 and
// everything else an empty 200, which is enough for DropAllCollections and
// EnsureCollectionsExist to pass through.
type fakeTypesense struct {
	mu       sync.Mutex
	imports  [][]map[string]interface{} // documents received per import request
	respond  func(call int, docs []map[string]interface{}) (status int, body string)
	callsSeq atomic.Int32
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
		call := int(f.callsSeq.Add(1)) - 1
		f.mu.Lock()
		f.imports = append(f.imports, docs)
		f.mu.Unlock()

		status, body := http.StatusOK, ""
		if f.respond != nil {
			status, body = f.respond(call, docs)
		}
		if body == "" {
			var lines []string
			for range docs {
				lines = append(lines, `{"success":true}`)
			}
			body = strings.Join(lines, "\n")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func (f *fakeTypesense) importCalls() [][]map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]map[string]interface{}, len(f.imports))
	copy(out, f.imports)
	return out
}

func newFakeTypesense(t *testing.T, fake *fakeTypesense) *TypesenseClient {
	t.Helper()
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)
	return NewTypesenseClient("test-key", []string{srv.URL})
}

func TestImportDocuments_NormalizesAndSetsID(t *testing.T) {
	fake := &fakeTypesense{}
	client := newFakeTypesense(t, fake)

	created := time.Date(2026, 9, 22, 19, 51, 0, 0, time.UTC)
	docs := []map[string]interface{}{
		{"transaction_id": "txn_1", "created_at": created.Format(time.RFC3339), "precise_amount": float64(1e15), "meta_data": nil},
	}
	failures, err := client.ImportDocuments(context.Background(), CollectionTransactions, docs)
	require.NoError(t, err)
	assert.Empty(t, failures)

	calls := fake.importCalls()
	require.Len(t, calls, 1)
	require.Len(t, calls[0], 1)
	sent := calls[0][0]
	assert.Equal(t, "txn_1", sent["id"], "Typesense document id must come from the collection's IDField")
	assert.EqualValues(t, created.Unix(), sent["created_at"], "time fields are normalized to unix seconds like HandleNotification")
	assert.Equal(t, "1000000000000000", sent["precise_amount"], "big-int fields are sent as strings")
	assert.Equal(t, map[string]interface{}{}, sent["meta_data"], "nil meta_data becomes an empty object")
	assert.Contains(t, sent, "amount", "required schema fields are defaulted")
}

func TestImportDocuments_ReportsPerDocumentRejections(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, docs []map[string]interface{}) (int, string) {
		return http.StatusOK, strings.Join([]string{
			`{"success":true}`,
			`{"success":false,"error":"Field ` + "`amount`" + ` must be a float.","document":"{}"}`,
			`{"success":true}`,
		}, "\n")
	}}
	client := newFakeTypesense(t, fake)

	docs := []map[string]interface{}{
		{"ledger_id": "ldg_a"}, {"ledger_id": "ldg_b"}, {"ledger_id": "ldg_c"},
	}
	failures, err := client.ImportDocuments(context.Background(), CollectionLedgers, docs)
	require.NoError(t, err)
	require.Len(t, failures, 1)
	assert.Equal(t, 1, failures[0].Index)
	assert.Contains(t, failures[0].Error, "must be a float")
}

func TestImportDocuments_NormalizationFailureIsPerDocument(t *testing.T) {
	fake := &fakeTypesense{}
	client := newFakeTypesense(t, fake)

	docs := []map[string]interface{}{
		{"ledger_id": "ldg_a"},
		{"ledger_id": "ldg_b", "meta_data": make(chan int)},
		{"ledger_id": "ldg_c"},
	}
	failures, err := client.ImportDocuments(context.Background(), CollectionLedgers, docs)
	require.NoError(t, err)
	require.Len(t, failures, 1)
	assert.Equal(t, 1, failures[0].Index)

	calls := fake.importCalls()
	require.Len(t, calls, 1)
	require.Len(t, calls[0], 2, "the document that could not be normalized is left out of the request")
	assert.Equal(t, "ldg_a", calls[0][0]["id"])
	assert.Equal(t, "ldg_c", calls[0][1]["id"])
}

func TestImportDocuments_ShortResponseIsAnError(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, _ []map[string]interface{}) (int, string) {
		return http.StatusOK, `{"success":true}`
	}}
	client := newFakeTypesense(t, fake)

	docs := []map[string]interface{}{{"ledger_id": "ldg_a"}, {"ledger_id": "ldg_b"}}
	_, err := client.ImportDocuments(context.Background(), CollectionLedgers, docs)
	require.Error(t, err, "fewer result lines than documents means some documents are unaccounted for")
}

func TestImportDocuments_ExtraResultsIsAnError(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, _ []map[string]interface{}) (int, string) {
		return http.StatusOK, strings.Join([]string{`{"success":true}`, `{"success":true}`}, "\n")
	}}
	client := newFakeTypesense(t, fake)

	_, err := client.ImportDocuments(context.Background(), CollectionLedgers, []map[string]interface{}{{"ledger_id": "ldg_a"}})
	require.Error(t, err, "more result lines than documents cannot be attributed")
}

func TestImportDocuments_OutOfMemoryIsBackpressure(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, _ []map[string]interface{}) (int, string) {
		return http.StatusServiceUnavailable, `{"message":"Rejecting write: running out of resource type: OUT_OF_MEMORY"}`
	}}
	client := newFakeTypesense(t, fake)

	_, err := client.ImportDocuments(context.Background(), CollectionLedgers, []map[string]interface{}{{"ledger_id": "ldg_a"}})
	require.Error(t, err)
	assert.True(t, IsMemoryBackpressure(err), "a request-level OUT_OF_MEMORY must drive the reindex pause/retry loop")
}

func TestStartReindex_ImportsPagesInChunks(t *testing.T) {
	fake := &fakeTypesense{}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	ledgers := []model.Ledger{testLedger("ldg_1"), testLedger("ldg_2"), testLedger("ldg_3"), testLedger("ldg_4"), testLedger("ldg_5")}
	ds.On("GetAllLedgers", 10, 0).Return(ledgers, nil).Once()
	ds.On("GetAllLedgers", 10, 5).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", 10, 0).Return([]model.Identity{}, nil).Once()
	ds.On("GetAllBalances", 10, 0).Return([]model.Balance{}, nil).Once()
	ds.On("GetAllTransactions", 10, 0).Return([]model.Transaction{}, nil).Once()

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10, ImportChunkSize: 2})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, 5, progress.ProcessedRecords)
	assert.Empty(t, progress.Errors)

	calls := fake.importCalls()
	require.Len(t, calls, 3)
	assert.Len(t, calls[0], 2)
	assert.Len(t, calls[1], 2)
	assert.Len(t, calls[2], 1)
	assert.Equal(t, "ldg_5", calls[2][0]["id"])
	ds.AssertExpectations(t)
}

func TestStartReindex_RejectedDocumentIsRecordedNotFatal(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, docs []map[string]interface{}) (int, string) {
		var lines []string
		for _, d := range docs {
			if d["id"] == "ldg_bad" {
				lines = append(lines, `{"success":false,"error":"Field `+"`name`"+` must be a string.","document":"{}"}`)
				continue
			}
			lines = append(lines, `{"success":true}`)
		}
		return http.StatusOK, strings.Join(lines, "\n")
	}}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	ds.On("GetAllLedgers", 10, 0).Return([]model.Ledger{testLedger("ldg_ok"), testLedger("ldg_bad"), testLedger("ldg_ok2")}, nil).Once()
	ds.On("GetAllLedgers", 10, 3).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", 10, 0).Return([]model.Identity{}, nil).Once()
	ds.On("GetAllBalances", 10, 0).Return([]model.Balance{}, nil).Once()
	ds.On("GetAllTransactions", 10, 0).Return([]model.Transaction{}, nil).Once()

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
	ds.On("GetAllLedgers", 10, 0).Return([]model.Ledger{testLedger("ldg_1")}, nil).Once()
	ds.On("GetAllLedgers", 10, 1).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", 10, 0).Return([]model.Identity{}, nil).Once()
	ds.On("GetAllBalances", 10, 0).Return([]model.Balance{}, nil).Once()
	ds.On("GetAllTransactions", 10, 0).Return([]model.Transaction{}, nil).Once()

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10, BackpressureRetryInterval: 10 * time.Millisecond})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.False(t, progress.Paused)
	assert.EqualValues(t, 1, progress.ProcessedRecords)
	assert.Len(t, fake.importCalls(), 2, "the rejected chunk is re-sent after the backpressure wait")
}

func emptyAfterLedgers(ds *mocks.MockDataSource, ledgers []model.Ledger) {
	ds.On("GetAllLedgers", 10, 0).Return(ledgers, nil).Once()
	ds.On("GetAllLedgers", 10, len(ledgers)).Return([]model.Ledger{}, nil).Once()
	ds.On("GetAllIdentitiesPaginated", 10, 0).Return([]model.Identity{}, nil).Once()
	ds.On("GetAllBalances", 10, 0).Return([]model.Balance{}, nil).Once()
	ds.On("GetAllTransactions", 10, 0).Return([]model.Transaction{}, nil).Once()
}

func bulkFailsThen(single func(doc map[string]interface{}) (int, string)) func(int, []map[string]interface{}) (int, string) {
	return func(_ int, docs []map[string]interface{}) (int, string) {
		if len(docs) > 1 {
			return http.StatusBadRequest, `{"message":"malformed"}`
		}
		return single(docs[0])
	}
}

func TestStartReindex_BulkFailureFallsBackToSingleDocuments(t *testing.T) {
	fake := &fakeTypesense{respond: bulkFailsThen(func(_ map[string]interface{}) (int, string) {
		return http.StatusOK, ""
	})}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	emptyAfterLedgers(ds, []model.Ledger{testLedger("ldg_1"), testLedger("ldg_2")})

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, 2, progress.ProcessedRecords)
	assert.Empty(t, progress.Errors)

	calls := fake.importCalls()
	require.Len(t, calls, 3, "one failed bulk request, then one request per document")
	assert.Len(t, calls[0], 2)
	assert.Equal(t, "ldg_1", calls[1][0]["id"])
	assert.Equal(t, "ldg_2", calls[2][0]["id"])
}

func TestStartReindex_FallbackSkipsOnlyTheRejectedDocument(t *testing.T) {
	fake := &fakeTypesense{respond: bulkFailsThen(func(doc map[string]interface{}) (int, string) {
		if doc["id"] == "ldg_bad" {
			return http.StatusOK, `{"success":false,"error":"Field ` + "`name`" + ` must be a string.","document":"{}"}`
		}
		return http.StatusOK, ""
	})}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	emptyAfterLedgers(ds, []model.Ledger{testLedger("ldg_ok"), testLedger("ldg_bad"), testLedger("ldg_ok2")})

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, 2, progress.ProcessedRecords)
	require.Len(t, progress.Errors, 1)
	assert.Contains(t, progress.Errors[0], "ledger ldg_bad")
	assert.Contains(t, progress.Errors[0], "must be a string")
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
	assert.Len(t, fake.importCalls(), 2, "the failed bulk request and the first single-document request stop the phase")
	ds.AssertNotCalled(t, "GetAllIdentitiesPaginated")
}

func TestStartReindex_SlowBulkImportTimesOutAndFallsBack(t *testing.T) {
	fake := &fakeTypesense{respond: func(_ int, docs []map[string]interface{}) (int, string) {
		if len(docs) > 1 {
			time.Sleep(300 * time.Millisecond)
		}
		return http.StatusOK, ""
	}}
	client := newFakeTypesense(t, fake)

	ds := new(mocks.MockDataSource)
	emptyAfterLedgers(ds, []model.Ledger{testLedger("ldg_1"), testLedger("ldg_2")})

	svc := NewReindexService(client, ds, ReindexConfig{BatchSize: 10, ImportTimeout: 50 * time.Millisecond})
	progress, err := svc.StartReindex(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "completed", progress.Status)
	assert.EqualValues(t, 2, progress.ProcessedRecords)
	assert.Len(t, fake.importCalls(), 3, "the timed-out chunk is re-sent one document at a time")
}
