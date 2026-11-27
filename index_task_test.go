package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/search"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProcessIndexRefresh_LedgerUsesLatestDatabaseStateRegardlessOfTaskOrder(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}
	ctx := context.Background()

	stateA := &model.Ledger{
		LedgerID: "ldg_order",
		Name:     "Main",
		MetaData: map[string]interface{}{"step": "a"},
	}
	stateB := &model.Ledger{
		LedgerID: "ldg_order",
		Name:     "Main",
		MetaData: map[string]interface{}{"step": "b"},
	}

	mockDS.On("GetLedgerByID", "ldg_order").Return(stateA, nil).Once()
	mockDS.On("GetLedgerByID", "ldg_order").Return(stateB, nil).Once()

	var indexed []map[string]interface{}
	upsert := func(_ context.Context, collection string, document interface{}) error {
		require.Equal(t, "ledgers", collection)
		doc, err := DocumentToIndexMap(document)
		require.NoError(t, err)
		indexed = append(indexed, doc)
		return nil
	}

	task := IndexTask{Collection: "ledgers", DocumentID: "ldg_order", Mode: IndexModeRefresh}
	require.NoError(t, b.ProcessIndexTask(ctx, task, upsert))
	require.NoError(t, b.ProcessIndexTask(ctx, task, upsert))

	require.Len(t, indexed, 2)
	assert.Equal(t, "a", indexed[0]["meta_data"].(map[string]interface{})["step"])
	assert.Equal(t, "b", indexed[1]["meta_data"].(map[string]interface{})["step"],
		"second refresh must reflect the latest DB read even if tasks were enqueued out of commit order")
	mockDS.AssertExpectations(t)
}

func TestProcessIndexRefresh_TransactionScopeHydratesFlags(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}
	ctx := context.Background()

	effectiveDate := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	txn := &model.Transaction{
		TransactionID: "txn_scope",
		Amount:        100,
		Currency:      "USD",
		Status:        "INFLIGHT",
		EffectiveDate: &effectiveDate,
		MetaData: map[string]interface{}{
			"inflight":        true,
			"allow_overdraft": true,
		},
	}
	mockDS.On("ListTransactionsByMetadataScope", mock.Anything, "txn_scope", 100, int64(0)).
		Return([]*model.Transaction{txn}, nil).Once()

	var indexed map[string]interface{}
	err := b.ProcessIndexTask(ctx, IndexTask{
		Collection: "transactions",
		ScopeID:    "txn_scope",
		Mode:       IndexModeRefresh,
	}, func(_ context.Context, collection string, document interface{}) error {
		require.Equal(t, "transactions", collection)
		raw, err := json.Marshal(document)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &indexed))
		return nil
	})
	require.NoError(t, err)

	normalized := search.NormalizeTransactionDocument(indexed)
	assert.Equal(t, true, normalized["inflight"])
	assert.Equal(t, true, normalized["allow_overdraft"])
	assert.Equal(t, effectiveDate.Unix(), normalized["effective_date"])
	mockDS.AssertExpectations(t)
}

func TestProcessIndexBatch_PrimaryTransactionRereadsPostgres(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}
	ctx := context.Background()

	stale := map[string]interface{}{
		"transaction_id": "txn_batch_race",
		"status":         "INFLIGHT",
		"meta_data":      map[string]interface{}{},
	}
	fresh := &model.Transaction{
		TransactionID: "txn_batch_race",
		Status:        "INFLIGHT",
		MetaData:      map[string]interface{}{"stamp": "S"},
	}
	mockDS.On("GetTransaction", mock.Anything, "txn_batch_race").Return(fresh, nil).Once()

	batch := search.NewIndexBatch("txn_batch_race")
	batch.AddDependency("balances", "bln_src", map[string]interface{}{"balance_id": "bln_src"})
	batch.SetPrimary("transactions", "txn_batch_race", stale)

	var upserted []string
	var primary map[string]interface{}
	err := b.ProcessIndexBatch(ctx, batch, func(_ context.Context, collection string, document interface{}) error {
		upserted = append(upserted, collection)
		if collection == "transactions" {
			doc, err := DocumentToIndexMap(document)
			require.NoError(t, err)
			primary = doc
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"balances", "transactions"}, upserted)
	require.NotNil(t, primary)
	meta, ok := primary["meta_data"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "S", meta["stamp"],
		"batch worker must index the Postgres row, not the create-time snapshot, when it lands last")
	mockDS.AssertExpectations(t)
}

func TestProcessIndexBatch_CustomIndexQueuePrefixStillRereadsTransaction(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS, config: &config.Configuration{
		Transaction: config.TransactionConfig{IndexQueuePrefix: "test_index"},
	}}

	fresh := &model.Transaction{
		TransactionID: "txn_prefix",
		MetaData:      map[string]interface{}{"stamp": "S"},
	}
	mockDS.On("GetTransaction", mock.Anything, "txn_prefix").Return(fresh, nil).Once()

	batch := search.NewIndexBatch("txn_prefix")
	batch.SetPrimary("test_index", "txn_prefix", map[string]interface{}{
		"transaction_id": "txn_prefix",
		"meta_data":      map[string]interface{}{},
	})

	var indexed map[string]interface{}
	err := b.ProcessIndexBatch(context.Background(), batch, func(_ context.Context, collection string, document interface{}) error {
		assert.Equal(t, "test_index", collection)
		doc, err := DocumentToIndexMap(document)
		require.NoError(t, err)
		indexed = doc
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "S", indexed["meta_data"].(map[string]interface{})["stamp"])
	mockDS.AssertExpectations(t)
}

func TestProcessIndexBatch_FetchErrorDoesNotUpsertStalePrimary(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}
	mockDS.On("GetTransaction", mock.Anything, "txn_missing").Return((*model.Transaction)(nil), fmt.Errorf("not found")).Once()

	batch := search.NewIndexBatch("txn_missing")
	batch.AddDependency("balances", "bln_src", map[string]interface{}{"balance_id": "bln_src"})
	batch.SetPrimary("transactions", "txn_missing", map[string]interface{}{
		"transaction_id": "txn_missing",
		"meta_data":      map[string]interface{}{},
	})

	var upserted []string
	err := b.ProcessIndexBatch(context.Background(), batch, func(_ context.Context, collection string, _ interface{}) error {
		upserted = append(upserted, collection)
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, []string{"balances"}, upserted, "stale primary must not be upserted when the Postgres re-read fails")
	mockDS.AssertExpectations(t)
}

func TestProcessIndexBatch_UnknownCollectionKeepsSnapshot(t *testing.T) {
	b := &Blnk{}
	stale := map[string]interface{}{"id": "x", "meta_data": map[string]interface{}{}}
	batch := search.NewIndexBatch("unknown")
	batch.SetPrimary("not_a_collection", "x", stale)

	var got interface{}
	require.NoError(t, b.ProcessIndexBatch(context.Background(), batch, func(_ context.Context, collection string, document interface{}) error {
		assert.Equal(t, "not_a_collection", collection)
		got = document
		return nil
	}))
	assert.Equal(t, stale, got)
}

func TestProcessIndexTask_LegacySnapshotPathUnchanged(t *testing.T) {
	b := &Blnk{}
	called := false
	err := b.ProcessIndexTask(context.Background(), IndexTask{
		Collection: "ledgers",
		Payload:    map[string]interface{}{"ledger_id": "ldg_legacy", "name": "Legacy"},
	}, func(_ context.Context, collection string, document interface{}) error {
		called = true
		assert.Equal(t, "ledgers", collection)
		doc, ok := document.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "ldg_legacy", doc["ledger_id"])
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestProcessIndexRefresh_BalanceUsesWorkerTimeRead(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	postUpdate := &model.Balance{
		BalanceID:     "bln_idx_worker",
		Currency:      "USD",
		Balance:       big.NewInt(500),
		CreditBalance: big.NewInt(500),
		MetaData:      map[string]interface{}{"tier": "gold"},
	}
	mockDS.On("GetBalanceByID", "bln_idx_worker", mock.Anything, false).Return(postUpdate, nil).Once()

	var indexed map[string]interface{}
	err := b.ProcessIndexTask(context.Background(), IndexTask{
		Collection: "balances",
		DocumentID: "bln_idx_worker",
		Mode:       IndexModeRefresh,
	}, func(_ context.Context, collection string, document interface{}) error {
		doc, err := DocumentToIndexMap(document)
		require.NoError(t, err)
		indexed = doc
		return nil
	})
	require.NoError(t, err)
	assert.EqualValues(t, 500, indexed["balance"])
	mockDS.AssertExpectations(t)
}
