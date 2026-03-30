package blnk

import (
	"context"
	"testing"

	dbmocks "github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewQueuedTransactionRecoveryProcessor_UsesSingleWorker(t *testing.T) {
	processor := NewQueuedTransactionRecoveryProcessor(&Blnk{})

	assert.Equal(t, 1, processor.maxWorkers)
	assert.Equal(t, 100, processor.batchSize)
}

func TestProcessStuckTransaction_UsesCoalescingBeforeDirectReplay(t *testing.T) {
	mockDS := &dbmocks.MockDataSource{}
	blnk := &Blnk{datasource: mockDS}
	processor := NewQueuedTransactionRecoveryProcessor(blnk)

	stuckTxn := &model.Transaction{
		TransactionID: "txn_parent",
		Reference:     "ref_1",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Currency:      "USD",
		Status:        StatusQueued,
		MetaData:      map[string]interface{}{},
	}

	var hotLane bool
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, gotHotLane bool) (transactionExecutionResult, error) {
		hotLane = gotHotLane
		return transactionExecutionResult{mode: transactionExecutionModeQueuedBatch, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == "recovered" && metadata["recovery_attempts"] == 1
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

	stuckTxn := &model.Transaction{
		TransactionID: "txn_parent",
		Reference:     "ref_1",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Currency:      "USD",
		Status:        StatusQueued,
		MetaData: map[string]interface{}{
			hotpairs.QueueLaneMetaKey: hotpairs.LaneHot,
		},
	}

	var hotLane bool
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, gotHotLane bool) (transactionExecutionResult, error) {
		hotLane = gotHotLane
		return transactionExecutionResult{mode: transactionExecutionModeHotQueuedBatch, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == "recovered" && metadata["recovery_attempts"] == 1
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

	stuckTxn := &model.Transaction{
		TransactionID: "txn_parent",
		Reference:     "ref_1",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Currency:      "USD",
		Status:        StatusQueued,
		MetaData:      map[string]interface{}{},
	}

	var hotLane bool
	processor.processQueuedTransaction = func(ctx context.Context, txn *model.Transaction, gotHotLane bool) (transactionExecutionResult, error) {
		hotLane = gotHotLane
		return transactionExecutionResult{mode: transactionExecutionModeSingle, transaction: txn}, nil
	}

	mockDS.On("UpdateTransactionMetadata", mock.Anything, stuckTxn.TransactionID, mock.MatchedBy(func(metadata map[string]interface{}) bool {
		return metadata["recovery_status"] == "recovered" && metadata["recovery_attempts"] == 1
	})).Return(nil).Once()

	err := processor.processStuckTransaction(context.Background(), stuckTxn)
	assert.NoError(t, err)
	assert.False(t, hotLane)
	mockDS.AssertExpectations(t)
}
