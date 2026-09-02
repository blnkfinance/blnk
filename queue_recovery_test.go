package blnk

import (
	"context"
	"errors"
	"math/big"
	"testing"

	dbmocks "github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func positiveStuckTxn() *model.Transaction {
	return &model.Transaction{
		TransactionID: "txn_parent",
		Reference:     "ref_1",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Currency:      "USD",
		Amount:        100,
		PreciseAmount: big.NewInt(10000),
		Precision:     100,
		Status:        StatusQueued,
		MetaData:      map[string]interface{}{},
	}
}

func TestNewQueuedTransactionRecoveryProcessor_UsesSingleWorker(t *testing.T) {
	processor := NewQueuedTransactionRecoveryProcessor(&Blnk{})

	assert.Equal(t, 1, processor.maxWorkers)
	assert.Equal(t, 100, processor.batchSize)
}

func TestProcessStuckTransaction_UsesCoalescingBeforeDirectReplay(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()

	var hotLane bool
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, gotHotLane bool) (transactionExecutionResult, error) {
		hotLane = gotHotLane
		return transactionExecutionResult{mode: transactionExecutionModeQueuedBatch, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusRecovered && metadata["recovery_attempts"] == 1
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	assert.NoError(t, err)
	assert.False(t, hotLane)
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_UsesHotLaneCoalescingWhenMarkedHot(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()
	stuckTxn.MetaData = map[string]interface{}{
		hotpairs.QueueLaneMetaKey: hotpairs.LaneHot,
	}

	var hotLane bool
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, gotHotLane bool) (transactionExecutionResult, error) {
		hotLane = gotHotLane
		return transactionExecutionResult{mode: transactionExecutionModeHotQueuedBatch, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusRecovered && metadata["recovery_attempts"] == 1
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	assert.NoError(t, err)
	assert.True(t, hotLane)
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_FallsBackToDirectReplayWhenBatchNotHandled(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()

	var hotLane bool
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, gotHotLane bool) (transactionExecutionResult, error) {
		hotLane = gotHotLane
		return transactionExecutionResult{mode: transactionExecutionModeSingle, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusRecovered && metadata["recovery_attempts"] == 1
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	assert.NoError(t, err)
	assert.False(t, hotLane)
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_ZeroAmountFillsNumericAmountOnReject(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := &model.Transaction{
		TransactionID: "txn_zero",
		Reference:     "starbank_dust",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Currency:      "WBTC",
		Amount:        0,
		PreciseAmount: big.NewInt(0),
		Precision:     100,
		Status:        StatusQueued,
		MetaData: map[string]interface{}{
			"recovery_attempts": 3,
		},
	}

	var recorded *model.Transaction
	mockDS.On("RecordTransaction", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			recorded = args.Get(1).(*model.Transaction)
		}).
		Return(&model.Transaction{}, errors.New(`pq: invalid input syntax for type numeric: ""`)).Once()

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusUnrecoverable
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	require.NoError(t, err, "a failed reject must not keep the parent in the retry loop")
	require.NotNil(t, recorded)
	assert.Equal(t, StatusRejected, recorded.Status)
	assert.Equal(t, "0", recorded.AmountString)
	assert.Equal(t, "0", recorded.PreciseAmount.String())
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_RejectFailureMarksUnrecoverable(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()
	stuckTxn.MetaData = map[string]interface{}{"recovery_attempts": 3}

	mockDS.On("RecordTransaction", mock.Anything, mock.Anything).
		Return(&model.Transaction{}, errors.New("db unavailable")).Once()
	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusUnrecoverable && metadata["recovery_attempts"] == 4
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	require.NoError(t, err)
	mockDS.AssertExpectations(t)
}

func TestQueueTransaction_RejectsZeroAmount(t *testing.T) {
	b := &Blnk{}
	_, err := b.QueueTransaction(context.Background(), &model.Transaction{
		Amount:        0,
		PreciseAmount: big.NewInt(0),
		Precision:     100,
		Currency:      "WBTC",
		Reference:     "ref_zero_ingest",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Description:   "StarBank shadow mirror (trade)",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be positive")
}
