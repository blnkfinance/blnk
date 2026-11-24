package blnk

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

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
		Return(&model.Transaction{}, errors.New("db unavailable")).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	require.Error(t, err, "a failed reject must stay visible so recovery can retry")
	assert.Contains(t, err.Error(), "db unavailable")
	require.NotNil(t, recorded)
	assert.Equal(t, StatusRejected, recorded.Status)
	assert.Equal(t, "0", recorded.AmountString)
	assert.Equal(t, "0", recorded.PreciseAmount.String())
	mockDS.AssertNotCalled(t, "UpdateTransactionMetadata", mock.Anything, mock.Anything, mock.Anything)
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_RejectFailureRemainsRetryable(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()
	stuckTxn.MetaData = map[string]interface{}{"recovery_attempts": 3}

	mockDS.On("RecordTransaction", mock.Anything, mock.Anything).
		Return(&model.Transaction{}, errors.New("db unavailable")).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db unavailable")
	mockDS.AssertNotCalled(t, "UpdateTransactionMetadata", mock.Anything, mock.Anything, mock.Anything)
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_MetadataUpdateFailureKeepsTxnRetryable(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
		return transactionExecutionResult{}, errors.New("lock contention")
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusFailed
	})).Return(errors.New("metadata write failed")).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lock contention", "the original recovery error must remain so the parent is retried")
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

func TestRecoverWithThreshold_CountsOnlySuccessfulRecoveries(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	okTxn := positiveStuckTxn()
	failTxn := positiveStuckTxn()
	failTxn.TransactionID = "txn_fail"
	failTxn.Reference = "ref_fail"

	mockDS.On("GetStuckQueuedTransactions", mock.Anything, mock.Anything, 100).
		Return([]*model.Transaction{okTxn, failTxn}, nil).Once()

	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
		if txn.ParentTransaction == failTxn.TransactionID {
			return transactionExecutionResult{}, errors.New("lock contention")
		}
		return transactionExecutionResult{mode: transactionExecutionModeSingle, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, okTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusRecovered
	})).Return(nil).Once()
	mockDS.On("UpdateTransactionMetadata", mock.Anything, failTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusFailed
	})).Return(nil).Once()

	recovered, err := processor.recoverWithThreshold(context.Background(), time.Hour)
	assert.Equal(t, 1, recovered)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recovery failed for 1 of 2 stuck transactions")
	mockDS.AssertExpectations(t)
}

func TestProcessStuckTransaction_RejectsWhenOverdraftLimitExceeded(t *testing.T) {
	// The apply path wraps the error from canProcessTransaction, so recovery
	// must match the sentinel through the wrap, not the sentence.
	overLimit := fmt.Errorf("failed to apply transaction to balances: %w", model.ErrOverdraftLimitExceeded)

	t.Run("reject write fails", func(t *testing.T) {
		mockDS := &dbmocks.MockDataSource{}
		blnk := &Blnk{datasource: mockDS}
		processor := NewQueuedTransactionRecoveryProcessor(blnk)

		stuckTxn := positiveStuckTxn()
		processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
			return transactionExecutionResult{}, overLimit
		}

		var recorded *model.Transaction
		mockDS.On("RecordTransaction", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				recorded = args.Get(1).(*model.Transaction)
			}).
			Return(&model.Transaction{}, errors.New("db unavailable")).Once()

		err := processor.processStuckTransaction(context.Background(), stuckTxn)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db unavailable", "only a failed reject write is returned")
		require.NotNil(t, recorded, "a transaction over its overdraft limit is rejected, not left QUEUED")
		assert.Equal(t, StatusRejected, recorded.Status)
		mockDS.AssertNotCalled(t, "UpdateTransactionMetadata", mock.Anything, mock.Anything, mock.Anything)
		mockDS.AssertExpectations(t)
	})

	t.Run("reject write succeeds", func(t *testing.T) {
		mockDS := &dbmocks.MockDataSource{}
		blnk, _, _ := setupMetadataQueuesBlnk(t, mockDS, "")
		processor := NewQueuedTransactionRecoveryProcessor(blnk)

		stuckTxn := positiveStuckTxn()
		processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
			return transactionExecutionResult{}, overLimit
		}

		var recorded *model.Transaction
		mockDS.On("RecordTransaction", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				recorded = args.Get(1).(*model.Transaction)
			}).
			Return(&model.Transaction{TransactionID: "txn_rejected", Status: StatusRejected}, nil).Once()

		err := processor.processStuckTransaction(context.Background(), stuckTxn)
		require.NoError(t, err)
		require.NotNil(t, recorded)
		assert.Equal(t, StatusRejected, recorded.Status)
		assert.Equal(t, overLimit.Error(), recorded.MetaData["blnk_rejection_reason"])
		mockDS.AssertNotCalled(t, "UpdateTransactionMetadata", mock.Anything, mock.Anything, mock.Anything)
		mockDS.AssertExpectations(t)
	})
}

func TestProcessStuckTransaction_OverdraftSentenceAloneIsNotRejected(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := positiveStuckTxn()
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
		return transactionExecutionResult{}, errors.New("upstream: transaction exceeds overdraft limit")
	}
	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == recoveryStatusFailed
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	require.Error(t, err)
	mockDS.AssertNotCalled(t, "RecordTransaction", mock.Anything, mock.Anything)
	mockDS.AssertExpectations(t)
}

func TestCreateQueueCopyDoesNotShareMetadata(t *testing.T) {
	original := &model.Transaction{
		TransactionID: "txn_parent",
		Reference:     "ref",
		MetaData:      map[string]interface{}{"inflight": true},
	}

	queueCopy := createQueueCopy(original, original.Reference)
	queueCopy.MetaData["recovery_attempts"] = 1

	_, shared := original.MetaData["recovery_attempts"]
	assert.False(t, shared)
	assert.Equal(t, true, queueCopy.MetaData["inflight"])
}
