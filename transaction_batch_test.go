package blnk

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
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
	ds.On(
		"GetQueuedTransactionsForSourceCoalescing",
		mock.Anything,
		"bln_income",
		"NGN",
		"txn_parent",
		createdAt,
		9,
	).Return([]*model.Transaction{}, nil).Once()
	ds.On(
		"GetQueuedTransactionsForDestinationCoalescing",
		mock.Anything,
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

func TestBuildQueuedCoalescingBatchFallsBackToSourceScope(t *testing.T) {
	ds := &mocks.MockDataSource{}
	createdAt := time.Now().UTC()
	leader := &model.Transaction{
		TransactionID:     "txn_current_q",
		ParentTransaction: "txn_parent",
		Source:            "bln_income",
		Destination:       "bln_fee",
		Currency:          "NGN",
		Status:            StatusQueued,
		CreatedAt:         createdAt,
		Reference:         "ref_current_q",
	}

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
	ds.On(
		"GetQueuedTransactionsForSourceCoalescing",
		mock.Anything,
		"bln_income",
		"NGN",
		"txn_parent",
		createdAt,
		9,
	).Return([]*model.Transaction{
		{
			TransactionID:     "txn_sibling",
			ParentTransaction: "txn_sibling_parent",
			Source:            "bln_income",
			Destination:       "bln_tax",
			Currency:          "NGN",
			Status:            StatusQueued,
			Reference:         "ref_sibling",
		},
	}, nil).Once()

	blnkInstance := &Blnk{
		datasource: ds,
		config: &config.Configuration{
			Transaction: config.TransactionConfig{EnableCoalescing: true, BatchSize: 10},
		},
	}

	batch, scope, err := blnkInstance.buildQueuedCoalescingBatch(context.Background(), leader, 10)
	assert.NoError(t, err)
	assert.Equal(t, queuedCoalescingScopeSource, scope)
	assert.Len(t, batch, 2)
	assert.Equal(t, "ref_sibling_q", batch[1].Reference)
	ds.AssertExpectations(t)
}

func TestBuildQueuedCoalescingBatchFallsBackToDestinationScope(t *testing.T) {
	ds := &mocks.MockDataSource{}
	createdAt := time.Now().UTC()
	leader := &model.Transaction{
		TransactionID:     "txn_current_q",
		ParentTransaction: "txn_parent",
		Source:            "bln_income",
		Destination:       "bln_fee",
		Currency:          "NGN",
		Status:            StatusQueued,
		CreatedAt:         createdAt,
		Reference:         "ref_current_q",
	}

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
	ds.On(
		"GetQueuedTransactionsForSourceCoalescing",
		mock.Anything,
		"bln_income",
		"NGN",
		"txn_parent",
		createdAt,
		9,
	).Return([]*model.Transaction{}, nil).Once()
	ds.On(
		"GetQueuedTransactionsForDestinationCoalescing",
		mock.Anything,
		"bln_fee",
		"NGN",
		"txn_parent",
		createdAt,
		9,
	).Return([]*model.Transaction{
		{
			TransactionID:     "txn_sibling",
			ParentTransaction: "txn_sibling_parent",
			Source:            "bln_vat",
			Destination:       "bln_fee",
			Currency:          "NGN",
			Status:            StatusQueued,
			Reference:         "ref_sibling",
		},
	}, nil).Once()

	blnkInstance := &Blnk{
		datasource: ds,
		config: &config.Configuration{
			Transaction: config.TransactionConfig{EnableCoalescing: true, BatchSize: 10},
		},
	}

	batch, scope, err := blnkInstance.buildQueuedCoalescingBatch(context.Background(), leader, 10)
	assert.NoError(t, err)
	assert.Equal(t, queuedCoalescingScopeDestination, scope)
	assert.Len(t, batch, 2)
	assert.Equal(t, "ref_sibling_q", batch[1].Reference)
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

func TestValidateQueuedBatchTransactionReferenceUsesPrefetchedSet(t *testing.T) {
	blnkInstance := &Blnk{}
	prefetched := map[string]struct{}{
		"ref_1_q": {},
	}
	existing := map[string]struct{}{}
	batch := make(map[string]struct{})

	err := blnkInstance.validateQueuedBatchTransactionReference(context.Background(), &model.Transaction{
		Reference: "ref_1_q",
	}, prefetched, existing, batch)
	assert.NoError(t, err)
	assert.Contains(t, batch, "ref_1_q")

	err = blnkInstance.validateQueuedBatchTransactionReference(context.Background(), &model.Transaction{
		Reference: "ref_1_q",
	}, prefetched, existing, batch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already been used")
}

func TestValidateQueuedBatchTransactionReferenceNotifiesDuplicateReference(t *testing.T) {
	if currentConfig, err := config.Fetch(); err == nil {
		t.Cleanup(func() { config.MockConfig(currentConfig) })
	}
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://test"},
		Redis:      config.RedisConfig{Dns: "redis://test"},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{Url: "http://example.com/system-error"},
		},
	})

	events := make(chan string, 1)
	notification.RegisterWebhookSender(func(event string, payload interface{}) error {
		events <- event
		return nil
	})
	t.Cleanup(func() { notification.RegisterWebhookSender(nil) })

	blnkInstance := &Blnk{}
	err := blnkInstance.validateQueuedBatchTransactionReference(context.Background(), &model.Transaction{
		Reference: "duplicate_ref_q",
	}, map[string]struct{}{"duplicate_ref_q": {}}, map[string]struct{}{"duplicate_ref_q": {}}, map[string]struct{}{})

	assert.Error(t, err)
	assert.True(t, IsDuplicateReferenceError(err))

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event)
	case <-time.After(time.Second):
		t.Fatal("expected system.error webhook event for duplicate reference")
	}
}

func TestIsDuplicateReferenceError(t *testing.T) {
	rawPQErr := &pq.Error{
		Code:       "23505",
		Constraint: "idx_transactions_reference_unique",
		Message:    "duplicate key value violates unique constraint \"idx_transactions_reference_unique\"",
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "application duplicate message",
			err:  errors.New("reference ref_1 has already been used"),
			want: true,
		},
		{
			name: "raw pq unique violation",
			err:  rawPQErr,
			want: true,
		},
		{
			name: "wrapped pq unique violation",
			err:  fmt.Errorf("failed to persist transaction: %w", rawPQErr),
			want: true,
		},
		{
			name: "wrapped duplicate string without unwrap chain",
			err:  errors.New("INTERNAL_SERVER_ERROR: pq: duplicate key value violates unique constraint \"idx_transactions_reference_unique\""),
			want: true,
		},
		{
			name: "non-reference unique violation",
			err: &pq.Error{
				Code:       "23505",
				Constraint: "transactions_pkey",
				Message:    "duplicate key value violates unique constraint \"transactions_pkey\"",
			},
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsDuplicateReferenceError(tt.err))
		})
	}
}

func TestBatchReferenceCheckEnabled(t *testing.T) {
	blnkInstance := &Blnk{
		config: &config.Configuration{
			Transaction: config.TransactionConfig{
				DisableBatchReferenceCheck: false,
			},
		},
	}
	assert.True(t, blnkInstance.batchReferenceCheckEnabled())

	blnkInstance.config.Transaction.DisableBatchReferenceCheck = true
	assert.False(t, blnkInstance.batchReferenceCheckEnabled())
}
