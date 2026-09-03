package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetEntityTypeFromID(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		want     string
		wantErr  bool
		errorMsg string
	}{
		{"Transaction ID", "txn_123", "transactions", false, ""},
		{"Bulk Transaction ID", "bulk_123", "transactions", false, ""},
		{"Ledger ID", "ldg_123", "ledgers", false, ""},
		{"Balance ID", "bln_123", "balances", false, ""},
		{"Identity ID", "idt_123", "identities", false, ""},
		{"Invalid ID", "invalid_123", "", true, "invalid entity ID format: invalid_123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getEntityTypeFromID(tt.id)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errorMsg, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestUpdateMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Update Ledger Metadata", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		blnk := &Blnk{datasource: mockDS}

		existingMetadata := map[string]interface{}{"existing": "value"}
		ledger := &model.Ledger{MetaData: existingMetadata}
		mockDS.On("GetLedgerByID", "ldg_123").Return(ledger, nil)
		mockDS.On("UpdateLedgerMetadata", "ldg_123", mock.Anything).Return(nil)

		newMetadata := map[string]interface{}{"new": "value"}
		result, err := blnk.UpdateMetadata(ctx, "ldg_123", newMetadata)

		assert.NoError(t, err)
		assert.Contains(t, result, "existing")
		assert.Contains(t, result, "new")
		mockDS.AssertExpectations(t)
	})

	t.Run("Update Transaction Metadata", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		blnk := &Blnk{datasource: mockDS}

		// Set up expectations for transaction exists check
		mockDS.On("TransactionExistsByIDOrParentID", ctx, "txn_123").Return(true, nil)

		// Set up expectation for metadata update
		newMetadata := map[string]interface{}{"new": "value"}
		mockDS.On("UpdateTransactionMetadata", ctx, "txn_123", newMetadata).Return(nil)

		result, err := blnk.UpdateMetadata(ctx, "txn_123", newMetadata)

		assert.NoError(t, err)
		assert.Equal(t, newMetadata, result) // Should return just the new metadata
		mockDS.AssertExpectations(t)
	})

	t.Run("Update Balance Metadata", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		blnk := &Blnk{datasource: mockDS}

		existingMetadata := map[string]interface{}{"existing": "value"}
		balance := &model.Balance{MetaData: existingMetadata}

		mockDS.On("GetBalanceByID", "bln_123", mock.Anything, false).Return(balance, nil)
		mockDS.On("UpdateBalanceMetadata", mock.Anything, "bln_123", mock.Anything).Return(nil)

		newMetadata := map[string]interface{}{"new": "value"}
		result, err := blnk.UpdateMetadata(ctx, "bln_123", newMetadata)

		assert.NoError(t, err)
		assert.Contains(t, result, "existing")
		assert.Contains(t, result, "new")
		mockDS.AssertExpectations(t)
	})

	t.Run("Update Identity Metadata", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		blnk := &Blnk{datasource: mockDS}

		existingMetadata := map[string]interface{}{"existing": "value"}
		identity := &model.Identity{MetaData: existingMetadata}

		mockDS.On("GetIdentityByID", "idt_123").Return(identity, nil)
		mockDS.On("UpdateIdentityMetadata", "idt_123", mock.Anything).Return(nil)

		newMetadata := map[string]interface{}{"new": "value"}
		result, err := blnk.UpdateMetadata(ctx, "idt_123", newMetadata)

		assert.NoError(t, err)
		assert.Contains(t, result, "existing")
		assert.Contains(t, result, "new")
		mockDS.AssertExpectations(t)
	})

	t.Run("Invalid Entity ID", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		blnk := &Blnk{datasource: mockDS}

		_, err := blnk.UpdateMetadata(ctx, "invalid_123", map[string]interface{}{})
		assert.Error(t, err)
	})
}

func TestMergeMetadata(t *testing.T) {
	tests := []struct {
		name     string
		current  map[string]interface{}
		new      map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:     "Merge with empty current",
			current:  nil,
			new:      map[string]interface{}{"new": "value"},
			expected: map[string]interface{}{"new": "value"},
		},
		{
			name:     "Merge with existing values",
			current:  map[string]interface{}{"existing": "value"},
			new:      map[string]interface{}{"new": "value"},
			expected: map[string]interface{}{"existing": "value", "new": "value"},
		},
		{
			name:    "Override existing values",
			current: map[string]interface{}{"key": "old"},
			new:     map[string]interface{}{"key": "new"},
			expected: map[string]interface{}{
				"key": "new",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeMetadata(tt.current, tt.new)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMetadataUpdatedEventName(t *testing.T) {
	assert.Equal(t, "ledger.metadata.updated", metadataUpdatedEventName("ledgers"))
	assert.Equal(t, "balance.metadata.updated", metadataUpdatedEventName("balances"))
	assert.Equal(t, "identity.metadata.updated", metadataUpdatedEventName("identities"))
	assert.Equal(t, "transaction.metadata.updated", metadataUpdatedEventName("transactions"))
}

// setupMetadataWebhookBlnk wires a Blnk instance against miniredis with a
// unique webhook queue and the given mock datasource.
func setupMetadataWebhookBlnk(t *testing.T, mockDS *mocks.MockDataSource, webhookURL string) (*Blnk, string) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	queueName := fmt.Sprintf("webhook_metadata_%d", time.Now().UnixNano())
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   queueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: webhookURL,
			},
		},
	}
	config.ConfigStore.Store(cnf)

	b, err := NewBlnk(mockDS)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b, queueName
}

func waitForWebhookTask(t *testing.T, redisAddr, queueName string) *asynq.TaskInfo {
	t.Helper()

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisAddr})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
		_ = inspector.Close()
	})

	var task *asynq.TaskInfo
	require.Eventually(t, func() bool {
		tasks, err := inspector.ListPendingTasks(queueName)
		if err != nil || len(tasks) == 0 {
			return false
		}
		task = tasks[0]
		return true
	}, 2*time.Second, 20*time.Millisecond, "expected a metadata webhook task on %s", queueName)

	return task
}

func TestUpdateMetadata_EmitsWebhookForEachEntity(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name       string
		entityID   string
		event      string
		setupMock  func(mockDS *mocks.MockDataSource, newMetadata map[string]interface{})
		assertData func(t *testing.T, data map[string]interface{})
	}{
		{
			name:     "ledger",
			entityID: "ldg_wh_1",
			event:    "ledger.metadata.updated",
			setupMock: func(mockDS *mocks.MockDataSource, newMetadata map[string]interface{}) {
				ledger := &model.Ledger{
					LedgerID: "ldg_wh_1",
					Name:     "Main",
					MetaData: map[string]interface{}{"existing": "value"},
				}
				updated := &model.Ledger{
					LedgerID: "ldg_wh_1",
					Name:     "Main",
					MetaData: map[string]interface{}{"existing": "value", "new": "value"},
				}
				mockDS.On("GetLedgerByID", "ldg_wh_1").Return(ledger, nil).Once()
				mockDS.On("UpdateLedgerMetadata", "ldg_wh_1", mock.Anything).Return(nil).Once()
				mockDS.On("GetLedgerByID", "ldg_wh_1").Return(updated, nil).Once()
			},
			assertData: func(t *testing.T, data map[string]interface{}) {
				assert.Equal(t, "ldg_wh_1", data["ledger_id"])
				meta, ok := data["meta_data"].(map[string]interface{})
				require.True(t, ok)
				assert.Equal(t, "value", meta["new"])
			},
		},
		{
			name:     "balance",
			entityID: "bln_wh_1",
			event:    "balance.metadata.updated",
			setupMock: func(mockDS *mocks.MockDataSource, newMetadata map[string]interface{}) {
				balance := &model.Balance{
					BalanceID: "bln_wh_1",
					Currency:  "USD",
					MetaData:  map[string]interface{}{"existing": "value"},
				}
				updated := &model.Balance{
					BalanceID: "bln_wh_1",
					Currency:  "USD",
					MetaData:  map[string]interface{}{"existing": "value", "new": "value"},
				}
				mockDS.On("GetBalanceByID", "bln_wh_1", mock.Anything, false).Return(balance, nil).Once()
				mockDS.On("UpdateBalanceMetadata", mock.Anything, "bln_wh_1", mock.Anything).Return(nil).Once()
				mockDS.On("GetBalanceByID", "bln_wh_1", mock.Anything, false).Return(updated, nil).Once()
			},
			assertData: func(t *testing.T, data map[string]interface{}) {
				assert.Equal(t, "bln_wh_1", data["balance_id"])
			},
		},
		{
			name:     "identity",
			entityID: "idt_wh_1",
			event:    "identity.metadata.updated",
			setupMock: func(mockDS *mocks.MockDataSource, newMetadata map[string]interface{}) {
				identity := &model.Identity{
					IdentityID: "idt_wh_1",
					MetaData:   map[string]interface{}{"existing": "value"},
				}
				updated := &model.Identity{
					IdentityID: "idt_wh_1",
					MetaData:   map[string]interface{}{"existing": "value", "new": "value"},
				}
				mockDS.On("GetIdentityByID", "idt_wh_1").Return(identity, nil).Once()
				mockDS.On("UpdateIdentityMetadata", "idt_wh_1", mock.Anything).Return(nil).Once()
				mockDS.On("GetIdentityByID", "idt_wh_1").Return(updated, nil).Once()
			},
			assertData: func(t *testing.T, data map[string]interface{}) {
				assert.Equal(t, "idt_wh_1", data["identity_id"])
			},
		},
		{
			name:     "transaction",
			entityID: "txn_wh_1",
			event:    "transaction.metadata.updated",
			setupMock: func(mockDS *mocks.MockDataSource, newMetadata map[string]interface{}) {
				txn := &model.Transaction{
					TransactionID: "txn_wh_1",
					MetaData:      map[string]interface{}{"new": "value"},
				}
				mockDS.On("TransactionExistsByIDOrParentID", mock.Anything, "txn_wh_1").Return(true, nil).Once()
				mockDS.On("UpdateTransactionMetadata", mock.Anything, "txn_wh_1", newMetadata).Return(nil).Once()
				mockDS.On("GetTransaction", mock.Anything, "txn_wh_1").Return(txn, nil).Once()
			},
			assertData: func(t *testing.T, data map[string]interface{}) {
				assert.Equal(t, "txn_wh_1", data["transaction_id"])
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockDS := new(mocks.MockDataSource)
			newMetadata := map[string]interface{}{"new": "value"}
			tc.setupMock(mockDS, newMetadata)

			b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")
			_, err := b.UpdateMetadata(ctx, tc.entityID, newMetadata)
			require.NoError(t, err)

			task := waitForWebhookTask(t, b.Config().Redis.Dns, queueName)
			assert.Equal(t, queueName, task.Type)

			var event NewWebhook
			require.NoError(t, json.Unmarshal(task.Payload, &event))
			assert.Equal(t, tc.event, event.Event)

			data, ok := event.Payload.(map[string]interface{})
			require.True(t, ok)
			tc.assertData(t, data)
			mockDS.AssertExpectations(t)
		})
	}
}

func TestUpdateMetadata_NoWebhookWhenURLNotConfigured(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	ledger := &model.Ledger{
		LedgerID: "ldg_nowh",
		MetaData: map[string]interface{}{},
	}
	mockDS.On("GetLedgerByID", "ldg_nowh").Return(ledger, nil)
	mockDS.On("UpdateLedgerMetadata", "ldg_nowh", mock.Anything).Return(nil)

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "")
	_, err := b.UpdateMetadata(context.Background(), "ldg_nowh", map[string]interface{}{"k": "v"})
	require.NoError(t, err)

	// Give the async post-actions goroutine time to run (and no-op).
	time.Sleep(150 * time.Millisecond)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: b.Config().Redis.Dns})
	t.Cleanup(func() { _ = inspector.Close() })
	tasks, err := inspector.ListPendingTasks(queueName)
	if err == nil {
		assert.Empty(t, tasks, "nothing should be enqueued when webhook URL is empty")
	}
}

// Internal writers (reconciliation, queue recovery, lineage) call
// updateEntityMetadata or the datasource directly — not UpdateMetadata —
// so they must not enqueue metadata webhooks.
func TestUpdateEntityMetadata_DoesNotEmitWebhook(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	mockDS.On("UpdateLedgerMetadata", "ldg_internal", mock.Anything).Return(nil)

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")
	err := b.updateEntityMetadata(context.Background(), "ledgers", "ldg_internal", map[string]interface{}{"internal": true})
	require.NoError(t, err)

	time.Sleep(150 * time.Millisecond)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: b.Config().Redis.Dns})
	t.Cleanup(func() { _ = inspector.Close() })
	tasks, err := inspector.ListPendingTasks(queueName)
	if err == nil {
		assert.Empty(t, tasks, "internal metadata writes must not enqueue webhooks")
	}
	mockDS.AssertExpectations(t)
}

func TestUpdateMetadata_TransactionFallbackPayloadWhenRefetchFails(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	newMetadata := map[string]interface{}{"tag": "bulk"}
	mockDS.On("TransactionExistsByIDOrParentID", mock.Anything, "bulk_wh_1").Return(true, nil).Once()
	mockDS.On("UpdateTransactionMetadata", mock.Anything, "bulk_wh_1", newMetadata).Return(nil).Once()
	mockDS.On("GetTransaction", mock.Anything, "bulk_wh_1").Return(nil, assert.AnError).Once()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")
	_, err := b.UpdateMetadata(context.Background(), "bulk_wh_1", newMetadata)
	require.NoError(t, err)

	task := waitForWebhookTask(t, b.Config().Redis.Dns, queueName)
	var event NewWebhook
	require.NoError(t, json.Unmarshal(task.Payload, &event))
	assert.Equal(t, "transaction.metadata.updated", event.Event)

	data, ok := event.Payload.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "bulk_wh_1", data["transaction_id"])
	meta, ok := data["meta_data"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "bulk", meta["tag"])
	mockDS.AssertExpectations(t)
}
