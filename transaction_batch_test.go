package blnk

import (
	"context"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTryRecordQueuedTransactionBatchSkipsWithoutSiblingTransactions(t *testing.T) {
	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{NumberOfQueues: 1},
	})

	ds := &mocks.MockDataSource{}
	createdAt := time.Now().UTC()
	ds.On(
		"GetQueuedTransactionsForCoalescing",
		mock.Anything,
		"bln_income",
		"bln_fee",
		"NGN",
		"txn_parent",
		createdAt,
		9,
	).Return([]*model.Transaction{}, nil).Once()

	blnkInstance := &Blnk{
		datasource: ds,
		config: &config.Configuration{
			Transaction: config.TransactionConfig{EnableCoalescing: true, BatchSize: 10},
		},
	}

	handled, err := blnkInstance.TryRecordQueuedTransactionBatch(context.Background(), &model.Transaction{
		TransactionID:     "txn_current_q",
		ParentTransaction: "txn_parent",
		Source:            "bln_income",
		Destination:       "bln_fee",
		Currency:          "NGN",
		Status:            StatusQueued,
		CreatedAt:         createdAt,
	})

	assert.NoError(t, err)
	assert.False(t, handled)
	ds.AssertExpectations(t)
}

func TestTryRecordQueuedTransactionBatchFailsOpenOnDiscoveryError(t *testing.T) {
	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{NumberOfQueues: 1},
	})

	ds := &mocks.MockDataSource{}
	createdAt := time.Now().UTC()
	ds.On(
		"GetQueuedTransactionsForCoalescing",
		mock.Anything,
		"bln_income",
		"bln_fee",
		"NGN",
		"txn_parent",
		createdAt,
		9,
	).Return(nil, assert.AnError).Once()

	blnkInstance := &Blnk{
		datasource: ds,
		config: &config.Configuration{
			Transaction: config.TransactionConfig{EnableCoalescing: true, BatchSize: 10},
		},
	}

	handled, err := blnkInstance.TryRecordQueuedTransactionBatch(context.Background(), &model.Transaction{
		TransactionID:     "txn_current_q",
		ParentTransaction: "txn_parent",
		Source:            "bln_income",
		Destination:       "bln_fee",
		Currency:          "NGN",
		Status:            StatusQueued,
		CreatedAt:         createdAt,
	})

	assert.NoError(t, err)
	assert.False(t, handled)
	ds.AssertExpectations(t)
}

func TestRestoreTransactionFlagsFromMetadata(t *testing.T) {
	txn := &model.Transaction{
		MetaData: map[string]interface{}{
			"inflight":        true,
			"atomic":          true,
			"allow_overdraft": true,
		},
	}

	restoreTransactionFlagsFromMetadata(txn)

	assert.True(t, txn.Inflight)
	assert.True(t, txn.Atomic)
	assert.True(t, txn.AllowOverdraft)
}
