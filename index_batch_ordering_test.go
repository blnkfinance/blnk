package blnk

import (
	"context"
	"testing"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/search"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Each read returns its own snapshot, as a Postgres query does.
type batchOrderingDataSource struct {
	mocks.MockDataSource
	stamp string
}

func (ds *batchOrderingDataSource) GetTransaction(_ context.Context, id string) (*model.Transaction, error) {
	return &model.Transaction{
		TransactionID: id,
		MetaData:      map[string]interface{}{"stamp": ds.stamp},
	}, nil
}

func TestProcessIndexBatch_RefreshBeforePrimaryRead(t *testing.T) {
	const id = "txn_overlap"
	ctx := t.Context()
	ds := &batchOrderingDataSource{stamp: "original"}
	b := &Blnk{datasource: ds}
	batch := search.NewIndexBatch(id)
	batch.AddDependency("balances", "bln_source", map[string]interface{}{"balance_id": "bln_source"})
	batch.SetPrimary("transactions", id, map[string]interface{}{
		"transaction_id": id,
		"meta_data":      map[string]interface{}{"stamp": "queued"},
	})

	// Record the transaction documents in the order they reach the index.
	var writes []map[string]interface{}
	upsert := func(_ context.Context, collection string, document interface{}) error {
		if collection == "transactions" {
			doc, err := DocumentToIndexMap(document)
			if err != nil {
				return err
			}
			writes = append(writes, doc)
		}
		return nil
	}

	paused := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- b.ProcessIndexBatch(ctx, batch, func(ctx context.Context, collection string, document interface{}) error {
			if collection == "balances" {
				close(paused)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return upsert(ctx, collection, document)
		})
	}()

	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("batch finished before indexing the balance: %v", err)
	}

	ds.stamp = "updated"
	row, err := ds.GetTransaction(ctx, id)
	require.NoError(t, err)
	ds.On("ListTransactionsByMetadataScope", mock.Anything, id, 100, int64(0)).
		Return([]*model.Transaction{row}, nil).Once()
	refresh := IndexTask{Collection: "transactions", ScopeID: id, Mode: IndexModeRefresh}
	require.NoError(t, b.ProcessIndexTask(ctx, refresh, upsert))
	require.Len(t, writes, 1)
	require.Equal(t, id, writes[0]["transaction_id"])
	require.Equal(t, row.MetaData, writes[0]["meta_data"])

	close(resume)
	require.NoError(t, <-done)
	ds.AssertExpectations(t)
	require.Len(t, writes, 2)
	require.Equal(t, id, writes[1]["transaction_id"])
	require.Equal(t, row.MetaData, writes[1]["meta_data"], "batch overwrote refreshed transaction metadata")
}
