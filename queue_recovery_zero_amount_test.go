package blnk

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverZeroAmountQueuedTransactionWritesRejectedChild(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping zero-amount recovery integration test in short mode")
	}

	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	ctx := context.Background()

	parent := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "starbank_dust_" + model.GenerateUUIDWithSuffix("ref"),
		Source:        src.BalanceID,
		Destination:   dst.BalanceID,
		Amount:        0,
		AmountString:  "0",
		PreciseAmount: big.NewInt(0),
		Precision:     100,
		Currency:      "WBTC",
		Description:   "StarBank shadow mirror (trade)",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UTC().Add(-3 * time.Hour),
		MetaData: map[string]interface{}{
			"recovery_attempts": 3,
		},
	}

	_, err := ds.RecordTransaction(ctx, parent)
	require.NoError(t, err, "parent QUEUED row must persist with amount 0")

	processor := NewQueuedTransactionRecoveryProcessor(b)
	require.NoError(t, processor.processStuckTransaction(ctx, parent))

	children, err := ds.GetTransactionsByParent(ctx, parent.TransactionID, 10, 0)
	require.NoError(t, err)
	require.Len(t, children, 1, "recovery must write a REJECTED child")
	assert.Equal(t, StatusRejected, children[0].Status)
	assert.Equal(t, 0.0, children[0].Amount)
	assert.Equal(t, "0", children[0].PreciseAmount.String())
	assert.Equal(t, parent.TransactionID, children[0].ParentTransaction)

	stuck, err := ds.GetStuckQueuedTransactions(ctx, 2*time.Hour, 100)
	require.NoError(t, err)
	for _, txn := range stuck {
		assert.NotEqual(t, parent.TransactionID, txn.TransactionID, "rejected zero-amount parent must leave the stuck set")
	}
}
