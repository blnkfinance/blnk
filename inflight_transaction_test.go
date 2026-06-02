package blnk

import (
	"context"
	"math/big"
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newInflightFinalizationBalances() (*model.Balance, *model.Balance) {
	source := &model.Balance{
		BalanceID:             "bln_source",
		Currency:              "USD",
		InflightDebitBalance:  big.NewInt(1000),
		InflightCreditBalance: big.NewInt(0),
		InflightBalance:       big.NewInt(-1000),
		DebitBalance:          big.NewInt(0),
		CreditBalance:         big.NewInt(0),
		Balance:               big.NewInt(0),
		TrackFundLineage:      false,
	}
	destination := &model.Balance{
		BalanceID:             "bln_destination",
		Currency:              "USD",
		InflightDebitBalance:  big.NewInt(0),
		InflightCreditBalance: big.NewInt(1000),
		InflightBalance:       big.NewInt(1000),
		DebitBalance:          big.NewInt(0),
		CreditBalance:         big.NewInt(0),
		Balance:               big.NewInt(0),
		TrackFundLineage:      false,
	}
	return source, destination
}

func newInflightFinalizationTransaction() *model.Transaction {
	return &model.Transaction{
		TransactionID:  "txn_parent",
		Reference:      "ref_parent",
		Source:         "bln_source",
		Destination:    "bln_destination",
		Amount:         10,
		PreciseAmount:  big.NewInt(1000),
		Precision:      100,
		Rate:           1,
		Currency:       "USD",
		Status:         StatusInflight,
		Inflight:       true,
		AllowOverdraft: true,
		MetaData: map[string]interface{}{
			"inflight":        true,
			"allow_overdraft": true,
			"caller":          "kept",
		},
	}
}

func assertTerminalInflightCleared(t *testing.T, txn *model.Transaction) {
	t.Helper()

	require.NotNil(t, txn)
	assert.False(t, txn.Inflight)
	assert.NotContains(t, txn.MetaData, "inflight")
	assert.Equal(t, true, txn.MetaData["allow_overdraft"])
	assert.Equal(t, "kept", txn.MetaData["caller"])

	rehydrated := *txn
	rehydrated.Inflight = false
	restoreTransactionFlagsFromMetadata(&rehydrated)
	assert.False(t, rehydrated.Inflight)

	setTransactionMetadata(&rehydrated)
	assert.False(t, rehydrated.Inflight)
	assert.NotContains(t, rehydrated.MetaData, "inflight")
}

func TestClearTerminalInflightFlagRemovesPersistedMetadata(t *testing.T) {
	txn := newInflightFinalizationTransaction()

	clearTerminalInflightFlag(txn)

	assertTerminalInflightCleared(t, txn)
}

func TestBuildTransactionExecutionWorkSkipsLineageForInflightCommitAfterMetadataIsCleared(t *testing.T) {
	source, destination := newInflightFinalizationBalances()
	source.TrackFundLineage = true
	destination.TrackFundLineage = true

	txn := newInflightFinalizationTransaction()
	txn.Status = StatusCommit
	txn.ParentTransaction = "txn_parent"
	txn.TransactionID = "txn_commit"
	clearTerminalInflightFlag(txn)

	blnk := &Blnk{}
	work, skipPersist := blnk.buildTransactionExecutionWork(context.Background(), txn, source, destination)

	assert.False(t, skipPersist)
	assert.Nil(t, work.outbox)
	assert.Equal(t, StatusApplied, work.transaction.Status)
	assertTerminalInflightCleared(t, work.transaction)
}
