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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
)

// storeReconEngineConfig installs a deterministic configuration for reconciliation tests.
// BatchSize is 100 so paginated mocks key off limit=100; MaxWorkers is 1 for determinism.
func storeReconEngineConfig() *config.Configuration {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: "localhost:6379"},
		Transaction: config.TransactionConfig{
			BatchSize:    100,
			MaxWorkers:   1,
			MaxQueueSize: 100,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Reconciliation: config.ReconciliationConfig{ProgressInterval: 100},
	}
	config.ConfigStore.Store(cnf)
	return cnf
}

// reconWaitOrFail waits for the channel to close or fails the test after the timeout.
func reconWaitOrFail(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(msg)
	}
}

// --- UploadExternalData / storeExternalTransaction ---

func TestRecon_UploadExternalData_CSV(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	var mu sync.Mutex
	var stored []model.ExternalTransaction
	var uploadIDs []string
	mockDS.On("RecordExternalTransaction", mock.Anything, mock.AnythingOfType("*model.ExternalTransaction"), mock.AnythingOfType("string")).
		Run(func(args mock.Arguments) {
			mu.Lock()
			defer mu.Unlock()
			stored = append(stored, *args.Get(1).(*model.ExternalTransaction))
			uploadIDs = append(uploadIDs, args.String(2))
		}).
		Return(nil)

	ref1, ref2 := gofakeit.UUID(), gofakeit.UUID()
	csvData := "ID,Amount,Currency,Reference,Description,Date\n" +
		fmt.Sprintf("ext-1,100.50,USD,%s,first payment,2026-01-02T15:04:05Z\n", ref1) +
		fmt.Sprintf("ext-2,0.01,USD,%s,one cent boundary,2026-01-03T00:00:00Z\n", ref2) +
		"ext-3,999999999.99,NGN,ref-3,large value,2026-01-04T10:30:00Z\n"

	uploadID, count, err := b.UploadExternalData(context.Background(), "bank-one", strings.NewReader(csvData), "statement.csv")
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(uploadID, "upload_"), "upload ID %q must have upload_ prefix", uploadID)
	assert.Equal(t, 3, count, "all three rows must be counted")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, stored, 3)
	for _, id := range uploadIDs {
		assert.Equal(t, uploadID, id, "every record must be stored under the returned upload ID")
	}
	byID := make(map[string]model.ExternalTransaction)
	for _, txn := range stored {
		byID[txn.ID] = txn
		assert.Equal(t, "bank-one", txn.Source, "source must be stamped on every record")
	}
	assert.InDelta(t, 100.50, byID["ext-1"].Amount, 1e-9)
	assert.InDelta(t, 0.01, byID["ext-2"].Amount, 1e-9)
	assert.InDelta(t, 999999999.99, byID["ext-3"].Amount, 1e-6)
	assert.Equal(t, ref1, byID["ext-1"].Reference)
	assert.Equal(t, time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC), byID["ext-1"].Date.UTC())
}

func TestRecon_UploadExternalData_StoreErrorPropagates(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("RecordExternalTransaction", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("db down"))

	csvData := "ID,Amount,Currency,Reference,Description,Date\n" +
		"ext-1,10,USD,r1,d1,2026-01-02T15:04:05Z\n"

	uploadID, count, err := b.UploadExternalData(context.Background(), "bank-one", strings.NewReader(csvData), "statement.csv")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db down")
	assert.Empty(t, uploadID)
	assert.Zero(t, count)
}

func TestRecon_UploadExternalData_UnsupportedFileType(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	uploadID, count, err := b.UploadExternalData(context.Background(), "bank-one", strings.NewReader("just some notes\n"), "notes.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported file type")
	assert.Empty(t, uploadID)
	assert.Zero(t, count)
	reconAssertNoCall(t, mockDS, "RecordExternalTransaction")
}

func TestRecon_UploadExternalData_CSVMissingRequiredColumn(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	// "Date" column missing.
	csvData := "ID,Amount,Currency,Reference,Description\next-1,10,USD,r1,d1\n"
	_, _, err := b.UploadExternalData(context.Background(), "bank-one", strings.NewReader(csvData), "statement.csv")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required column 'Date' not found")
	reconAssertNoCall(t, mockDS, "RecordExternalTransaction")
}

func TestRecon_UploadExternalData_JSON(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	var mu sync.Mutex
	var stored []model.ExternalTransaction
	mockDS.On("RecordExternalTransaction", mock.Anything, mock.AnythingOfType("*model.ExternalTransaction"), mock.AnythingOfType("string")).
		Run(func(args mock.Arguments) {
			mu.Lock()
			defer mu.Unlock()
			stored = append(stored, *args.Get(1).(*model.ExternalTransaction))
		}).
		Return(nil)

	jsonData := `[
		{"id":"j1","amount":250.75,"reference":"r1","currency":"USD","description":"d1","date":"2026-01-02T15:04:05Z"},
		{"id":"j2","amount":0.99,"reference":"r2","currency":"EUR","description":"d2","date":"2026-01-03T15:04:05Z"}
	]`

	uploadID, count, err := b.UploadExternalData(context.Background(), "psp-two", strings.NewReader(jsonData), "data.json")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(uploadID, "upload_"))
	assert.Equal(t, 2, count)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, stored, 2)
	for _, txn := range stored {
		assert.Equal(t, "psp-two", txn.Source, "source must be stamped on JSON records too")
	}
}

func TestRecon_StoreExternalTransaction(t *testing.T) {
	ctx := context.Background()
	txn := model.ExternalTransaction{ID: "ext-1", Amount: 42.42, Currency: "USD"}

	t.Run("success passes txn and upload id through", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		mockDS.On("RecordExternalTransaction", mock.Anything, mock.MatchedBy(func(p *model.ExternalTransaction) bool {
			return p != nil && p.ID == "ext-1" && p.Amount == 42.42
		}), "upload_1").Return(nil).Once()

		require.NoError(t, b.storeExternalTransaction(ctx, "upload_1", txn))
		mockDS.AssertExpectations(t)
	})

	t.Run("error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		dbErr := errors.New("write failed")
		mockDS.On("RecordExternalTransaction", mock.Anything, mock.Anything, mock.Anything).Return(dbErr).Once()
		assert.ErrorIs(t, b.storeExternalTransaction(ctx, "upload_1", txn), dbErr)
	})
}

// --- StartReconciliation / StartInstantReconciliation ---

func TestRecon_StartReconciliation_RecordErrorAborts(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	dbErr := errors.New("insert failed")
	mockDS.On("RecordReconciliation", mock.Anything, mock.Anything).Return(dbErr).Once()

	id, err := b.StartReconciliation(context.Background(), "upload_1", "one_to_one", "", nil, false)
	assert.Empty(t, id)
	assert.ErrorIs(t, err, dbErr)
	reconAssertNoCall(t, mockDS, "UpdateReconciliationStatus")
}

func TestRecon_StartReconciliation_DryRunCompletes(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	now := time.Now()
	done := make(chan struct{})
	var recorded model.Reconciliation

	mockDS.On("RecordReconciliation", mock.Anything, mock.AnythingOfType("*model.Reconciliation")).
		Run(func(args mock.Arguments) { recorded = *args.Get(1).(*model.Reconciliation) }).
		Return(nil).Once()
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusInProgress, 0, 0).Return(nil).Once()
	mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(&model.MatchingRule{
		RuleID: "rule_1",
		Name:   "exact amount and currency",
		Criteria: []model.MatchingCriteria{
			{Field: "amount", Operator: "equals", AllowableDrift: 0},
			{Field: "currency", Operator: "equals"},
		},
	}, nil).Once()
	mockDS.On("LoadReconciliationProgress", mock.Anything, mock.Anything).Return(model.ReconciliationProgress{}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, int64(0)).
		Return([]*model.ExternalTransaction{{ID: "ext_1", Amount: 100, Currency: "USD", Date: now}}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, mock.Anything).
		Return([]*model.ExternalTransaction{}, nil)
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, int64(0)).
		Return([]*model.Transaction{{TransactionID: "int_1", Amount: 100, Currency: "USD", CreatedAt: now}}, nil).Once()
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.Transaction{}, nil)
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusCompleted, 1, 0).
		Run(func(mock.Arguments) { close(done) }).
		Return(nil).Once()

	id, err := b.StartReconciliation(context.Background(), "upload_1", "one_to_one", "", []string{"rule_1"}, true)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(id, "recon_"), "reconciliation ID %q must have recon_ prefix", id)

	reconWaitOrFail(t, done, "reconciliation never reached completed status")

	assert.Equal(t, id, recorded.ReconciliationID)
	assert.Equal(t, "upload_1", recorded.UploadID)
	assert.Equal(t, StatusStarted, recorded.Status)
	assert.True(t, recorded.IsDryRun)

	// Dry run must not persist matches or unmatched records.
	reconAssertNoCall(t, mockDS, "RecordMatches")
	reconAssertNoCall(t, mockDS, "RecordUnmatched")
	mockDS.AssertExpectations(t)
}

func TestRecon_StartReconciliation_ProcessFailureMarksFailed(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	done := make(chan struct{})
	mockDS.On("RecordReconciliation", mock.Anything, mock.Anything).Return(nil).Once()
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusInProgress, 0, 0).
		Return(errors.New("status table locked")).Once()
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusFailed, 0, 0).
		Run(func(mock.Arguments) { close(done) }).
		Return(nil).Once()

	id, err := b.StartReconciliation(context.Background(), "upload_1", "one_to_one", "", nil, false)
	require.NoError(t, err, "StartReconciliation reports success; failures surface asynchronously")
	assert.NotEmpty(t, id)

	reconWaitOrFail(t, done, "reconciliation was never marked failed")
	mockDS.AssertExpectations(t)
}

func TestRecon_StartInstantReconciliation_RecordErrorAborts(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	dbErr := errors.New("insert failed")
	mockDS.On("RecordReconciliation", mock.Anything, mock.Anything).Return(dbErr).Once()

	id, err := b.StartInstantReconciliation(context.Background(), []model.ExternalTransaction{{ID: "e1"}}, "one_to_one", "", nil, true)
	assert.Empty(t, id)
	assert.ErrorIs(t, err, dbErr)
	reconAssertNoCall(t, mockDS, "RecordExternalTransaction")
}

func TestRecon_StartInstantReconciliation_StoreErrorMarksFailed(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("RecordReconciliation", mock.Anything, mock.Anything).Return(nil).Once()
	mockDS.On("RecordExternalTransaction", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("disk full")).Once()
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusFailed, 0, 0).Return(nil).Once()

	id, err := b.StartInstantReconciliation(context.Background(), []model.ExternalTransaction{{ID: "e1", Amount: 5}}, "one_to_one", "", nil, true)
	assert.Empty(t, id)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to store external transaction")
	mockDS.AssertExpectations(t)
}

func TestRecon_StartInstantReconciliation_DryRunCompletes(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	now := time.Now()
	done := make(chan struct{})
	var recorded model.Reconciliation
	var storedUploadID string

	mockDS.On("RecordReconciliation", mock.Anything, mock.AnythingOfType("*model.Reconciliation")).
		Run(func(args mock.Arguments) { recorded = *args.Get(1).(*model.Reconciliation) }).
		Return(nil).Once()
	mockDS.On("RecordExternalTransaction", mock.Anything, mock.AnythingOfType("*model.ExternalTransaction"), mock.AnythingOfType("string")).
		Run(func(args mock.Arguments) { storedUploadID = args.String(2) }).
		Return(nil).Once()
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusInProgress, 0, 0).Return(nil).Once()
	mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(&model.MatchingRule{
		RuleID:   "rule_1",
		Name:     "exact amount",
		Criteria: []model.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: 0}},
	}, nil).Once()
	mockDS.On("LoadReconciliationProgress", mock.Anything, mock.Anything).Return(model.ReconciliationProgress{}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, mock.Anything, 100, int64(0)).
		Return([]*model.ExternalTransaction{{ID: "ext_1", Amount: 75, Currency: "USD", Date: now}}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.ExternalTransaction{}, nil)
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, int64(0)).
		Return([]*model.Transaction{{TransactionID: "int_1", Amount: 75, Currency: "USD", CreatedAt: now}}, nil).Once()
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.Transaction{}, nil)
	mockDS.On("UpdateReconciliationStatus", mock.Anything, mock.Anything, StatusCompleted, 1, 0).
		Run(func(mock.Arguments) { close(done) }).
		Return(nil).Once()

	id, err := b.StartInstantReconciliation(context.Background(),
		[]model.ExternalTransaction{{ID: "ext_1", Amount: 75, Currency: "USD", Date: now}},
		"one_to_one", "", []string{"rule_1"}, true)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(id, "recon_"))

	reconWaitOrFail(t, done, "instant reconciliation never reached completed status")

	assert.True(t, strings.HasPrefix(storedUploadID, "instant_"), "instant uploads must use an instant_ temp ID, got %q", storedUploadID)
	assert.Equal(t, storedUploadID, recorded.UploadID, "reconciliation must reference the temp upload ID its transactions were stored under")
	mockDS.AssertExpectations(t)
}

// --- processReconciliation and its helpers ---

func TestRecon_ProcessReconciliation_DryRunMatchedAndUnmatched(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	now := time.Now()
	rec := model.Reconciliation{ReconciliationID: "recon_test", UploadID: "upload_1", IsDryRun: true}

	amtPtr := func(v float64) interface{} {
		return mock.MatchedBy(func(p *float64) bool { return p != nil && *p == v })
	}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_test", StatusInProgress, 0, 0).Return(nil).Once()
	mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(&model.MatchingRule{
		RuleID:   "rule_1",
		Name:     "exact amount",
		Criteria: []model.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: 0}},
	}, nil).Once()
	mockDS.On("LoadReconciliationProgress", mock.Anything, "recon_test").Return(model.ReconciliationProgress{}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, int64(0)).
		Return([]*model.ExternalTransaction{
			{ID: "ext_match", Amount: 100, Currency: "USD", Date: now},
			{ID: "ext_orphan", Amount: 555, Currency: "USD", Date: now},
		}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, mock.Anything).
		Return([]*model.ExternalTransaction{}, nil)

	// With drift 0, query bounds collapse to min == max == amount: assert the engine
	// passes the right bounds for each external transaction.
	mockDS.On("GetTransactionsByCriteria", mock.Anything, amtPtr(100), amtPtr(100), mock.Anything, mock.Anything, mock.Anything, 100, int64(0)).
		Return([]*model.Transaction{{TransactionID: "int_1", Amount: 100, Currency: "USD", CreatedAt: now}}, nil).Once()
	mockDS.On("GetTransactionsByCriteria", mock.Anything, amtPtr(100), amtPtr(100), mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.Transaction{}, nil)
	mockDS.On("GetTransactionsByCriteria", mock.Anything, amtPtr(555), amtPtr(555), mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.Transaction{}, nil)

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_test", StatusCompleted, 1, 1).Return(nil).Once()

	err := b.processReconciliation(context.Background(), rec, "one_to_one", "", []string{"rule_1"})
	require.NoError(t, err)
	reconAssertNoCall(t, mockDS, "RecordMatches")
	reconAssertNoCall(t, mockDS, "RecordUnmatched")
	mockDS.AssertExpectations(t)
}

func TestRecon_ProcessReconciliation_StatusUpdateError(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_x", StatusInProgress, 0, 0).
		Return(errors.New("locked")).Once()

	err := b.processReconciliation(context.Background(), model.Reconciliation{ReconciliationID: "recon_x"}, "one_to_one", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update reconciliation status")
}

func TestRecon_ProcessReconciliation_RuleFetchError(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_x", StatusInProgress, 0, 0).Return(nil).Once()
	mockDS.On("GetMatchingRule", mock.Anything, "rule_missing").Return((*model.MatchingRule)(nil), errors.New("no such rule")).Once()

	err := b.processReconciliation(context.Background(), model.Reconciliation{ReconciliationID: "recon_x"}, "one_to_one", "", []string{"rule_missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get matching rules")
}

func TestRecon_ProcessReconciliation_TransactionFetchError(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_x", StatusInProgress, 0, 0).Return(nil).Once()
	mockDS.On("LoadReconciliationProgress", mock.Anything, "recon_x").Return(model.ReconciliationProgress{}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, int64(0)).
		Return(([]*model.ExternalTransaction)(nil), errors.New("query timeout")).Once()

	rec := model.Reconciliation{ReconciliationID: "recon_x", UploadID: "upload_1", IsDryRun: true}
	err := b.processReconciliation(context.Background(), rec, "one_to_one", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to process transactions")
	// On synchronous failure the status update to failed happens in the caller
	// (StartReconciliation), not here.
	reconAssertNoCall(t, mockDS, "RecordMatches")
}

func TestRecon_LoadReconciliationProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		want := model.ReconciliationProgress{LastProcessedExternalTxnID: "ext_9", ProcessedCount: 9}
		mockDS.On("LoadReconciliationProgress", mock.Anything, "recon_1").Return(want, nil).Once()

		got, err := b.loadReconciliationProgress(ctx, "recon_1")
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		dbErr := errors.New("no progress")
		mockDS.On("LoadReconciliationProgress", mock.Anything, "recon_1").Return(model.ReconciliationProgress{}, dbErr).Once()

		_, err := b.loadReconciliationProgress(ctx, "recon_1")
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestRecon_InitializeReconciliationProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("returns stored progress", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		want := model.ReconciliationProgress{LastProcessedExternalTxnID: "ext_5", ProcessedCount: 5}
		mockDS.On("LoadReconciliationProgress", mock.Anything, "recon_1").Return(want, nil).Once()

		got, err := b.initializeReconciliationProgress(ctx, "recon_1")
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("load error falls back to fresh progress without failing", func(t *testing.T) {
		// Deliberate behavior: a missing progress row must not abort a new reconciliation.
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		mockDS.On("LoadReconciliationProgress", mock.Anything, "recon_1").
			Return(model.ReconciliationProgress{}, errors.New("not found")).Once()

		got, err := b.initializeReconciliationProgress(ctx, "recon_1")
		require.NoError(t, err)
		assert.Equal(t, model.ReconciliationProgress{}, got)
	})
}

func TestRecon_UpdateReconciliationStatus(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_1", StatusInProgress, 0, 0).Return(nil).Once()
	require.NoError(t, b.updateReconciliationStatus(context.Background(), "recon_1", StatusInProgress))

	dbErr := errors.New("update failed")
	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_1", StatusFailed, 0, 0).Return(dbErr).Once()
	assert.ErrorIs(t, b.updateReconciliationStatus(context.Background(), "recon_1", StatusFailed), dbErr)
	mockDS.AssertExpectations(t)
}

func TestRecon_CreateReconciler_Dispatch(t *testing.T) {
	storeReconEngineConfig()
	ctx := context.Background()

	t.Run("unknown strategy returns nil results and touches nothing", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		rec := b.createReconciler("two_to_two", "upload_1", "", nil)
		matches, unmatched := rec(ctx, []*model.Transaction{{TransactionID: "ext_1", Amount: 1}})
		assert.Nil(t, matches)
		assert.Nil(t, unmatched)
		assert.Empty(t, mockDS.Calls, "unsupported strategy must not query the datasource")
	})

	t.Run("one_to_one strategy with no input is a no-op", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		rec := b.createReconciler("one_to_one", "upload_1", "", nil)
		matches, unmatched := rec(ctx, nil)
		assert.Empty(t, matches)
		assert.Empty(t, unmatched)
	})

	t.Run("one_to_many groups internal transactions by criteria", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		mockDS.On("GroupTransactions", mock.Anything, "reference", 100, int64(0)).
			Return(map[string][]*model.Transaction{}, nil).Once()

		rec := b.createReconciler("one_to_many", "upload_1", "reference", nil)
		rec(ctx, []*model.Transaction{{TransactionID: "ext_1"}})
		mockDS.AssertExpectations(t)
	})

	t.Run("many_to_one groups external transactions by upload id and criteria", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		mockDS.On("FetchAndGroupExternalTransactions", mock.Anything, "upload_1", "reference", 100, int64(0)).
			Return(map[string][]*model.Transaction{}, nil).Once()

		rec := b.createReconciler("many_to_one", "upload_1", "reference", nil)
		rec(ctx, []*model.Transaction{{TransactionID: "int_1"}})
		mockDS.AssertExpectations(t)
	})
}

func TestRecon_CreateTransactionProcessor(t *testing.T) {
	cnf := storeReconEngineConfig()
	cnf.Reconciliation.ProgressInterval = 42
	config.ConfigStore.Store(cnf)

	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	rec := model.Reconciliation{ReconciliationID: "recon_1", IsDryRun: true}
	progress := model.ReconciliationProgress{ProcessedCount: 7}
	processor := b.createTransactionProcessor(rec, progress, func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
		return nil, nil
	})

	require.NotNil(t, processor)
	assert.Equal(t, rec, processor.reconciliation)
	assert.Equal(t, progress, processor.progress)
	assert.Equal(t, 42, processor.progressSaveCount, "progress save interval must come from configuration")
	assert.Equal(t, b, processor.blnk)

	// restore the default interval for subsequent tests
	storeReconEngineConfig()
}

// --- transactionProcessor.process / getResults ---

func TestRecon_Process_DryRunSkipsPersistence(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)

	tp := &transactionProcessor{
		reconciliation: model.Reconciliation{ReconciliationID: "recon_dry", IsDryRun: true},
		reconciler: func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
			return []model.Match{
					{ExternalTransactionID: "ext_1", InternalTransactionID: "int_1", Amount: 10},
					{ExternalTransactionID: "ext_1", InternalTransactionID: "int_2", Amount: 20},
				},
				[]string{"ext_2"}
		},
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              &Blnk{datasource: mockDS},
	}

	err := tp.process(context.Background(), &model.Transaction{TransactionID: "ext_1"})
	require.NoError(t, err)

	matched, unmatched := tp.getResults()
	assert.Equal(t, 2, matched)
	assert.Equal(t, 1, unmatched)
	assert.Equal(t, "ext_1", tp.progress.LastProcessedExternalTxnID)
	assert.Equal(t, 1, tp.progress.ProcessedCount)
	assert.Empty(t, mockDS.Calls, "dry run must not write matches, unmatched records or metadata")
}

func TestRecon_Process_RecordsMatchesUnmatchedAndMetadata(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)

	matches := []model.Match{{ExternalTransactionID: "ext_1", InternalTransactionID: "int_1", Amount: 100.5}}
	unmatchedIDs := []string{"ext_2"}

	metaDone := make(chan map[string]interface{}, 1)
	mockDS.On("RecordMatches", mock.Anything, "recon_live", matches).Return(nil).Once()
	mockDS.On("RecordUnmatched", mock.Anything, "recon_live", unmatchedIDs).Return(nil).Once()
	mockDS.On("UpdateTransactionMetadata", mock.Anything, "int_1", mock.Anything).
		Run(func(args mock.Arguments) { metaDone <- args.Get(2).(map[string]interface{}) }).
		Return(nil).Once()

	tp := &transactionProcessor{
		reconciliation: model.Reconciliation{ReconciliationID: "recon_live", IsDryRun: false},
		reconciler: func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
			return matches, unmatchedIDs
		},
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              &Blnk{datasource: mockDS},
	}

	err := tp.process(context.Background(), &model.Transaction{TransactionID: "ext_1"})
	require.NoError(t, err)

	var metadata map[string]interface{}
	select {
	case metadata = <-metaDone:
	case <-time.After(5 * time.Second):
		t.Fatal("matched transaction metadata was never updated")
	}

	assert.Equal(t, true, metadata["reconciled"])
	assert.Equal(t, "recon_live", metadata["reconciliation_id"])
	assert.Equal(t, "ext_1", metadata["external_txn_id"])
	assert.Equal(t, 100.5, metadata["reconciliation_amount"])
	mockDS.AssertExpectations(t)
}

func TestRecon_Process_RecordMatchesErrorPropagates(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)

	dbErr := errors.New("matches table down")
	mockDS.On("RecordMatches", mock.Anything, "recon_live", mock.Anything).Return(dbErr).Once()

	tp := &transactionProcessor{
		reconciliation: model.Reconciliation{ReconciliationID: "recon_live"},
		reconciler: func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
			return []model.Match{{ExternalTransactionID: "ext_1", InternalTransactionID: "int_1"}}, nil
		},
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              &Blnk{datasource: mockDS},
	}

	err := tp.process(context.Background(), &model.Transaction{TransactionID: "ext_1"})
	assert.ErrorIs(t, err, dbErr)
	reconAssertNoCall(t, mockDS, "UpdateTransactionMetadata")
}

func TestRecon_Process_RecordUnmatchedErrorPropagates(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)

	dbErr := errors.New("unmatched table down")
	mockDS.On("RecordUnmatched", mock.Anything, "recon_live", []string{"ext_1"}).Return(dbErr).Once()

	tp := &transactionProcessor{
		reconciliation: model.Reconciliation{ReconciliationID: "recon_live"},
		reconciler: func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
			return nil, []string{"ext_1"}
		},
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              &Blnk{datasource: mockDS},
	}

	err := tp.process(context.Background(), &model.Transaction{TransactionID: "ext_1"})
	assert.ErrorIs(t, err, dbErr)
}

func TestRecon_Process_SavesProgressAtInterval(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)

	var savedProgress model.ReconciliationProgress
	mockDS.On("SaveReconciliationProgress", mock.Anything, "recon_p", mock.AnythingOfType("model.ReconciliationProgress")).
		Run(func(args mock.Arguments) { savedProgress = args.Get(2).(model.ReconciliationProgress) }).
		Return(nil).Once()

	tp := &transactionProcessor{
		reconciliation:    model.Reconciliation{ReconciliationID: "recon_p", IsDryRun: true},
		reconciler:        func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) { return nil, nil },
		datasource:        mockDS,
		progressSaveCount: 2,
		blnk:              &Blnk{datasource: mockDS},
	}

	require.NoError(t, tp.process(context.Background(), &model.Transaction{TransactionID: "ext_1"}))
	require.Len(t, mockDS.Calls, 0, "progress must not be saved before the interval is reached")
	require.NoError(t, tp.process(context.Background(), &model.Transaction{TransactionID: "ext_2"}))

	mockDS.AssertExpectations(t)
	assert.Equal(t, 2, savedProgress.ProcessedCount)
	assert.Equal(t, "ext_2", savedProgress.LastProcessedExternalTxnID)
}

func TestRecon_Process_ProgressSaveErrorIsNonFatal(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	mockDS.On("SaveReconciliationProgress", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("save failed")).Once()

	tp := &transactionProcessor{
		reconciliation:    model.Reconciliation{ReconciliationID: "recon_p", IsDryRun: true},
		reconciler:        func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) { return nil, nil },
		datasource:        mockDS,
		progressSaveCount: 1,
		blnk:              &Blnk{datasource: mockDS},
	}

	assert.NoError(t, tp.process(context.Background(), &model.Transaction{TransactionID: "ext_1"}),
		"a failed progress checkpoint must not abort the reconciliation")
}

func TestRecon_GetResults(t *testing.T) {
	tp := &transactionProcessor{matches: 7, unmatched: 3}
	matched, unmatched := tp.getResults()
	assert.Equal(t, 7, matched)
	assert.Equal(t, 3, unmatched)
}

// --- processTransactions ---

func TestRecon_ProcessTransactions_UsesExternalSourceForOneToOne(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	var mu sync.Mutex
	var seen []string
	tp := &transactionProcessor{
		reconciliation: model.Reconciliation{ReconciliationID: "recon_s", IsDryRun: true},
		reconciler: func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
			mu.Lock()
			defer mu.Unlock()
			for _, txn := range txns {
				seen = append(seen, txn.TransactionID)
			}
			return nil, nil
		},
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              b,
	}

	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, int64(0)).
		Return([]*model.ExternalTransaction{{ID: "ext_1", Amount: 5, Date: time.Now()}}, nil).Once()
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, mock.Anything).
		Return([]*model.ExternalTransaction{}, nil)

	err := b.processTransactions(context.Background(), "upload_1", tp, "one_to_one")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"ext_1"}, seen)
	reconAssertNoCall(t, mockDS, "GetTransactionsPaginated")
}

func TestRecon_ProcessTransactions_UsesInternalSourceForManyToOne(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	tp := &transactionProcessor{
		reconciliation:    model.Reconciliation{ReconciliationID: "recon_s", IsDryRun: true},
		reconciler:        func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) { return nil, nil },
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              b,
	}

	// Note: getInternalTransactionsPaginated always queries with an empty id.
	mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100, int64(0)).
		Return([]*model.Transaction{{TransactionID: "int_1", Amount: 5}}, nil).Once()
	mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100, mock.Anything).
		Return([]*model.Transaction{}, nil)

	err := b.processTransactions(context.Background(), "upload_1", tp, "many_to_one")
	require.NoError(t, err)
	reconAssertNoCall(t, mockDS, "GetExternalTransactionsPaginated")
	mockDS.AssertExpectations(t)
}

func TestRecon_ProcessTransactions_FetchErrorPropagates(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	tp := &transactionProcessor{
		reconciliation:    model.Reconciliation{ReconciliationID: "recon_s", IsDryRun: true},
		reconciler:        func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) { return nil, nil },
		datasource:        mockDS,
		progressSaveCount: 100,
		blnk:              b,
	}

	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 100, int64(0)).
		Return(([]*model.ExternalTransaction)(nil), errors.New("boom")).Once()

	err := b.processTransactions(context.Background(), "upload_1", tp, "one_to_one")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

// --- finalizeReconciliation ---

func TestRecon_FinalizeReconciliation_DryRun(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_f", StatusCompleted, 3, 2).Return(nil).Once()

	rec := model.Reconciliation{ReconciliationID: "recon_f", IsDryRun: true}
	require.NoError(t, b.finalizeReconciliation(context.Background(), rec, 3, 2))
	mockDS.AssertExpectations(t)
}

func TestRecon_FinalizeReconciliation_StatusErrorPropagates(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	dbErr := errors.New("final update failed")
	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_f", StatusCompleted, 1, 0).Return(dbErr).Once()

	rec := model.Reconciliation{ReconciliationID: "recon_f", IsDryRun: true}
	assert.ErrorIs(t, b.finalizeReconciliation(context.Background(), rec, 1, 0), dbErr)
}

func TestRecon_FinalizeReconciliation_NonDryRun(t *testing.T) {
	cnf := storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	// queue with empty TypeSense DNS: queueIndexData becomes a no-op, exercising the
	// postReconciliationActions path without a real broker.
	b := &Blnk{datasource: mockDS, queue: &Queue{config: cnf}}

	mockDS.On("UpdateReconciliationStatus", mock.Anything, "recon_f", StatusCompleted, 5, 4).Return(nil).Once()

	rec := model.Reconciliation{ReconciliationID: "recon_f", IsDryRun: false}
	require.NoError(t, b.finalizeReconciliation(context.Background(), rec, 5, 4))
	mockDS.AssertExpectations(t)
}

// --- pagination helpers ---

func TestRecon_GetExternalTransactionsPaginated_ConvertsToInternal(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	date := time.Date(2026, 2, 1, 9, 30, 0, 0, time.UTC)
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 10, int64(20)).
		Return([]*model.ExternalTransaction{
			{ID: "ext_1", Amount: 12.34, Reference: "ref-1", Currency: "USD", Description: "desc-1", Date: date},
		}, nil).Once()

	txns, err := b.getExternalTransactionsPaginated(context.Background(), "upload_1", 10, 20)
	require.NoError(t, err)
	require.Len(t, txns, 1)

	assert.Equal(t, "ext_1", txns[0].TransactionID)
	assert.Equal(t, 12.34, txns[0].Amount)
	assert.Equal(t, "ref-1", txns[0].Reference)
	assert.Equal(t, "USD", txns[0].Currency)
	assert.Equal(t, "desc-1", txns[0].Description)
	assert.Equal(t, date, txns[0].CreatedAt, "external Date must map to internal CreatedAt")
	mockDS.AssertExpectations(t)
}

func TestRecon_GetExternalTransactionsPaginated_ErrorPropagates(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	dbErr := errors.New("page fetch failed")
	mockDS.On("GetExternalTransactionsPaginated", mock.Anything, "upload_1", 10, int64(0)).
		Return(([]*model.ExternalTransaction)(nil), dbErr).Once()

	txns, err := b.getExternalTransactionsPaginated(context.Background(), "upload_1", 10, 0)
	assert.Nil(t, txns)
	assert.ErrorIs(t, err, dbErr)
}

func TestRecon_GetInternalTransactionsPaginated(t *testing.T) {
	t.Run("queries with empty id regardless of input", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		want := []*model.Transaction{{TransactionID: "int_1"}}
		mockDS.On("GetTransactionsPaginated", mock.Anything, "", 10, int64(5)).Return(want, nil).Once()

		got, err := b.getInternalTransactionsPaginated(context.Background(), "some_upload_id", 10, 5)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		mockDS.AssertExpectations(t)
	})

	t.Run("error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		dbErr := errors.New("fetch failed")
		mockDS.On("GetTransactionsPaginated", mock.Anything, "", 10, int64(0)).
			Return(([]*model.Transaction)(nil), dbErr).Once()

		got, err := b.getInternalTransactionsPaginated(context.Background(), "x", 10, 0)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, dbErr)
	})
}
