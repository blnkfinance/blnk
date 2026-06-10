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
	"errors"
	"math/big"
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
	}
	svc := NewReindexService(nil, nil, ReindexConfig{BatchSize: 25})
	assert.Equal(t, 25, svc.config.BatchSize)
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
	client := NewTypesenseClient("any-key", []string{"http://localhost:1"})
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
