package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

func listWebhookTasks(t *testing.T, redisAddr, queueName string) []*asynq.TaskInfo {
	t.Helper()
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisAddr})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
		_ = inspector.Close()
	})
	tasks, err := inspector.ListPendingTasks(queueName)
	if err != nil {
		// Queue does not exist until the first task is enqueued.
		return nil
	}
	return tasks
}

func decodeMetadataWebhook(t *testing.T, task *asynq.TaskInfo) (event string, data map[string]interface{}) {
	t.Helper()
	var wh NewWebhook
	require.NoError(t, json.Unmarshal(task.Payload, &wh))
	data, ok := wh.Payload.(map[string]interface{})
	require.True(t, ok, "payload must decode as an object")
	return wh.Event, data
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
				mockDS.On("GetLedgerByID", "ldg_wh_1").Return(ledger, nil).Once()
				mockDS.On("UpdateLedgerMetadata", "ldg_wh_1", mock.Anything).Return(nil).Once()
			},
			assertData: func(t *testing.T, data map[string]interface{}) {
				require.NotEmpty(t, data["event_id"])
				require.NotEmpty(t, data["timestamp"])
				_, hasEntity := data["entity"]
				assert.False(t, hasEntity, "resource fields must sit at the top of data, not nested under entity")
				assert.Equal(t, "ldg_wh_1", data["ledger_id"])
				meta, ok := data["meta_data"].(map[string]interface{})
				require.True(t, ok)
				assert.Equal(t, "value", meta["existing"])
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
				mockDS.On("GetBalanceByID", "bln_wh_1", mock.Anything, false).Return(balance, nil).Once()
				mockDS.On("UpdateBalanceMetadata", mock.Anything, "bln_wh_1", mock.Anything).Return(nil).Once()
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
				mockDS.On("GetIdentityByID", "idt_wh_1").Return(identity, nil).Once()
				mockDS.On("UpdateIdentityMetadata", "idt_wh_1", mock.Anything).Return(nil).Once()
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
				mockDS.On("TransactionExistsByIDOrParentID", mock.Anything, "txn_wh_1").Return(true, nil).Once()
				mockDS.On("UpdateTransactionMetadata", mock.Anything, "txn_wh_1", newMetadata).Return(nil).Once()
				// Metadata index reindexes the full row asynchronously; stub it
				// with .Maybe() since this test only asserts the webhook payload.
				mockDS.On("GetTransaction", mock.Anything, "txn_wh_1").Return(&model.Transaction{TransactionID: "txn_wh_1"}, nil).Maybe()
			},
			assertData: func(t *testing.T, data map[string]interface{}) {
				assert.Equal(t, "txn_wh_1", data["transaction_id"])
				_, hasMerged := data["meta_data"]
				assert.False(t, hasMerged, "transaction events must not claim merged meta_data")
				meta, ok := data["meta_data_patch"].(map[string]interface{})
				require.True(t, ok)
				assert.Equal(t, "value", meta["new"])
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

			// Enqueue is synchronous: the task must already be pending.
			tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
			require.Len(t, tasks, 1)
			assert.Equal(t, queueName, tasks[0].Type)

			event, data := decodeMetadataWebhook(t, tasks[0])
			assert.Equal(t, tc.event, event)
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
	mockDS.On("GetLedgerByID", "ldg_nowh").Return(ledger, nil).Once()
	mockDS.On("UpdateLedgerMetadata", "ldg_nowh", mock.Anything).Return(nil).Once()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "")
	_, err := b.UpdateMetadata(context.Background(), "ldg_nowh", map[string]interface{}{"k": "v"})
	require.NoError(t, err)

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	assert.Empty(t, tasks, "nothing should be enqueued when webhook URL is empty")
	mockDS.AssertExpectations(t)
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

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	assert.Empty(t, tasks, "internal metadata writes must not enqueue webhooks")
	mockDS.AssertExpectations(t)
}

func TestUpdateMetadata_TransactionPatchPayloadWithoutRefetch(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	newMetadata := map[string]interface{}{"tag": "bulk"}
	mockDS.On("TransactionExistsByIDOrParentID", mock.Anything, "bulk_wh_1").Return(true, nil).Once()
	mockDS.On("UpdateTransactionMetadata", mock.Anything, "bulk_wh_1", newMetadata).Return(nil).Once()
	// Webhook payload is built from the committed patch, without refetching.
	// Metadata index reindexes the full row asynchronously and independently;
	// stub it with .Maybe() since it is not the concern of this test.
	mockDS.On("GetTransaction", mock.Anything, "bulk_wh_1").Return(&model.Transaction{TransactionID: "bulk_wh_1"}, nil).Maybe()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")
	_, err := b.UpdateMetadata(context.Background(), "bulk_wh_1", newMetadata)
	require.NoError(t, err)

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	require.Len(t, tasks, 1)
	event, data := decodeMetadataWebhook(t, tasks[0])
	assert.Equal(t, "transaction.metadata.updated", event)
	require.NotEmpty(t, data["event_id"])

	assert.Equal(t, "bulk_wh_1", data["transaction_id"])
	meta, ok := data["meta_data_patch"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "bulk", meta["tag"])
	mockDS.AssertExpectations(t)
}

// TestUpdateMetadata_TransactionIndexUsesFullDocumentNotPatch guards against a
// regression where the Typesense index payload for a transaction metadata
// update was the webhook patch stub ({transaction_id, meta_data_patch}). That
// stub, upserted directly, would overwrite the full Typesense document
// (amount, source, destination, status, ...) with a near-empty record.
// Indexing must instead re-fetch and upsert the full transaction row.
func TestUpdateMetadata_TransactionIndexUsesFullDocumentNotPatch(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	newMetadata := map[string]interface{}{"invoice": "INV-1"}
	fullTxn := &model.Transaction{
		TransactionID: "txn_idx_1",
		Amount:        100,
		Currency:      "USD",
		Source:        "bln_src",
		Destination:   "bln_dst",
		Status:        "APPLIED",
		MetaData:      map[string]interface{}{"invoice": "INV-1"},
	}
	mockDS.On("TransactionExistsByIDOrParentID", mock.Anything, "txn_idx_1").Return(true, nil).Once()
	mockDS.On("UpdateTransactionMetadata", mock.Anything, "txn_idx_1", newMetadata).Return(nil).Once()
	// Index path must call GetTransaction to fetch the full row. Record completion
	// via an atomic flag in Run — polling mock.Calls races with the async goroutine
	// under -race because testify mutates Calls concurrently.
	var indexFetchDone atomic.Bool
	mockDS.On("GetTransaction", mock.Anything, "txn_idx_1").
		Return(fullTxn, nil).
		Run(func(mock.Arguments) { indexFetchDone.Store(true) }).
		Once()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")
	// Give this Blnk instance a queue so queueTransactionMetadataIndex is not
	// skipped; NewBlnk already wires one from config, but assert it explicitly.
	require.NotNil(t, b.queue)

	_, err := b.UpdateMetadata(context.Background(), "txn_idx_1", newMetadata)
	require.NoError(t, err)

	// Webhook payload is still the patch, not the full document.
	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	require.Len(t, tasks, 1)
	_, data := decodeMetadataWebhook(t, tasks[0])
	_, hasAmount := data["amount"]
	assert.False(t, hasAmount, "webhook payload must remain the patch, not the full transaction")

	require.Eventually(t, func() bool {
		return indexFetchDone.Load()
	}, 2*time.Second, 10*time.Millisecond, "expected GetTransaction to be called to build the full index document")

	mockDS.AssertExpectations(t)
}

func TestUpdateMetadata_SequentialUpdatesPreserveEachCommit(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	ledger := &model.Ledger{
		LedgerID: "ldg_race",
		Name:     "Race",
		MetaData: map[string]interface{}{"base": "1"},
	}
	// Each UpdateMetadata loads then writes once. Sequence the two calls.
	mockDS.On("GetLedgerByID", "ldg_race").Return(ledger, nil).Twice()
	mockDS.On("UpdateLedgerMetadata", "ldg_race", mock.Anything).Return(nil).Twice()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")

	_, err := b.UpdateMetadata(context.Background(), "ldg_race", map[string]interface{}{"step": "a"})
	require.NoError(t, err)
	_, err = b.UpdateMetadata(context.Background(), "ldg_race", map[string]interface{}{"step": "b"})
	require.NoError(t, err)

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	require.Len(t, tasks, 2)

	eventIDs := map[string]struct{}{}
	steps := make([]string, 0, 2)
	for _, task := range tasks {
		event, data := decodeMetadataWebhook(t, task)
		assert.Equal(t, "ledger.metadata.updated", event)
		id, _ := data["event_id"].(string)
		require.NotEmpty(t, id)
		eventIDs[id] = struct{}{}

		assert.Equal(t, "ldg_race", data["ledger_id"])
		meta, ok := data["meta_data"].(map[string]interface{})
		require.True(t, ok)
		step, _ := meta["step"].(string)
		steps = append(steps, step)
	}
	assert.Len(t, eventIDs, 2, "each update must carry a distinct event_id")
	assert.Equal(t, []string{"a", "b"}, steps, "each queued event must reflect its own committed patch")
	mockDS.AssertExpectations(t)
}

// TestUpdateMetadata_EnqueueFailureStillReportsCommittedWrite pins the
// resolution of the partial-success problem: the merge is committed before the
// event is enqueued, so an enqueue failure must not be reported as a failed
// write.
//
// Returning 5xx here would tell the client a write failed that had in fact
// applied. The client would retry, the retry would merge again and emit a
// second event with a fresh event_id, and consumers would have two
// undedupable events for one logical update. Reporting the commit honestly is
// what keeps event_id usable as a dedupe key. The dropped notification is
// logged and sent to notification.NotifyError instead.
func TestUpdateMetadata_EnqueueFailureStillReportsCommittedWrite(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	ledger := &model.Ledger{
		LedgerID: "ldg_enq_fail",
		MetaData: map[string]interface{}{"existing": "value"},
	}
	mockDS.On("GetLedgerByID", "ldg_enq_fail").Return(ledger, nil).Once()
	mockDS.On("UpdateLedgerMetadata", "ldg_enq_fail", mock.Anything).Return(nil).Once()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")
	// Closing the client breaks the enqueue while leaving the persist mocked
	// as successful, which is exactly the partial-success window.
	require.NoError(t, b.Close())

	merged, err := b.UpdateMetadata(context.Background(), "ldg_enq_fail", map[string]interface{}{"k": "v"})
	require.NoError(t, err, "the merge was committed; a failed enqueue must not be reported as a failed write")

	// The caller still receives the authoritative merged state.
	assert.Equal(t, "value", merged["existing"])
	assert.Equal(t, "v", merged["k"])

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	assert.Empty(t, tasks, "enqueue failed, so no event should be pending")
	mockDS.AssertExpectations(t)
}

// TestUpdateMetadata_PersistFailureEmitsNoEvent pins the other half of the
// ordering: nothing is published unless the merge actually persisted, so
// consumers never see an event for a write that did not happen.
func TestUpdateMetadata_PersistFailureEmitsNoEvent(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	ledger := &model.Ledger{
		LedgerID: "ldg_persist_fail",
		MetaData: map[string]interface{}{},
	}
	mockDS.On("GetLedgerByID", "ldg_persist_fail").Return(ledger, nil).Once()
	mockDS.On("UpdateLedgerMetadata", "ldg_persist_fail", mock.Anything).
		Return(errors.New("write conflict")).Once()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")

	_, err := b.UpdateMetadata(context.Background(), "ldg_persist_fail", map[string]interface{}{"k": "v"})
	require.Error(t, err, "a failed persist must fail the call")

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	assert.Empty(t, tasks, "no event may be emitted for a merge that was never committed")
	mockDS.AssertExpectations(t)
}

// TestUpdateMetadata_BulkTransactionAppliesRawPatch covers the bulk_ entity
// prefix, where a single ID addresses a parent and all of its child rows.
//
// The patch must reach the datasource unmerged so Postgres performs the JSONB
// merge per row: pre-merging against one row's metadata would overwrite
// sibling rows with the wrong state. The event therefore reports
// meta_data_patch rather than claiming a merged document, and the search index
// is rebuilt from a re-read instead of that patch.
func TestUpdateMetadata_BulkTransactionAppliesRawPatch(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	patch := map[string]interface{}{"settlement_batch": "B-42"}

	mockDS.On("TransactionExistsByIDOrParentID", mock.Anything, "bulk_meta_1").Return(true, nil).Once()
	// mock.Anything is deliberately not used for the payload: the exact patch
	// must be handed to the DB so the JSONB merge stays server-side.
	mockDS.On("UpdateTransactionMetadata", mock.Anything, "bulk_meta_1", patch).Return(nil).Once()

	var indexFetchDone atomic.Bool
	mockDS.On("GetTransaction", mock.Anything, "bulk_meta_1").
		Return(&model.Transaction{TransactionID: "bulk_meta_1", Amount: 250, Currency: "USD"}, nil).
		Run(func(mock.Arguments) { indexFetchDone.Store(true) }).
		Maybe()

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")

	returned, err := b.UpdateMetadata(context.Background(), "bulk_meta_1", patch)
	require.NoError(t, err)
	assert.Equal(t, patch, returned, "bulk updates report the applied patch, not a synthesized merge")

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	require.Len(t, tasks, 1)
	event, data := decodeMetadataWebhook(t, tasks[0])
	assert.Equal(t, "transaction.metadata.updated", event)
	assert.Equal(t, "bulk_meta_1", data["transaction_id"])

	_, claimsMerged := data["meta_data"]
	assert.False(t, claimsMerged, "the event must not claim a merged document it never read")
	appliedPatch, ok := data["meta_data_patch"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "B-42", appliedPatch["settlement_batch"])

	require.Eventually(t, func() bool {
		return indexFetchDone.Load()
	}, 2*time.Second, 10*time.Millisecond, "bulk reindex must re-read the rows rather than index the patch")
	mockDS.AssertExpectations(t)
}

// TestUpdateMetadata_ConcurrentUpdatesEachCarryOwnCommit runs genuinely
// concurrent updates against one entity, which is what the earlier sequential
// test could not exercise.
//
// Each queued event must carry the patch its own call committed. The payload is
// built from a cloned snapshot, so a shared or late-read map would show up here
// as a duplicated or missing step value, and any shared mutation would be
// reported by -race in CI.
func TestUpdateMetadata_ConcurrentUpdatesEachCarryOwnCommit(t *testing.T) {
	const updates = 8

	mockDS := new(mocks.MockDataSource)
	ledger := &model.Ledger{
		LedgerID: "ldg_concurrent",
		Name:     "Concurrent",
		MetaData: map[string]interface{}{"base": "1"},
	}
	mockDS.On("GetLedgerByID", "ldg_concurrent").Return(ledger, nil).Times(updates)
	mockDS.On("UpdateLedgerMetadata", "ldg_concurrent", mock.Anything).Return(nil).Times(updates)

	b, queueName := setupMetadataWebhookBlnk(t, mockDS, "http://localhost:1/webhooks")

	var wg sync.WaitGroup
	errs := make(chan error, updates)
	start := make(chan struct{})
	for i := 0; i < updates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // maximise overlap
			_, err := b.UpdateMetadata(context.Background(), "ldg_concurrent",
				map[string]interface{}{"step": fmt.Sprintf("s%d", i)})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	tasks := listWebhookTasks(t, b.Config().Redis.Dns, queueName)
	require.Len(t, tasks, updates, "every committed update must enqueue exactly one event")

	eventIDs := map[string]struct{}{}
	steps := map[string]struct{}{}
	for _, task := range tasks {
		event, data := decodeMetadataWebhook(t, task)
		assert.Equal(t, "ledger.metadata.updated", event)
		assert.Equal(t, "ldg_concurrent", data["ledger_id"])

		id, _ := data["event_id"].(string)
		require.NotEmpty(t, id)
		eventIDs[id] = struct{}{}

		meta, ok := data["meta_data"].(map[string]interface{})
		require.True(t, ok)
		// The pre-existing key survives, proving the payload is a real merge
		// of the loaded state and not just the patch.
		assert.Equal(t, "1", meta["base"])
		step, _ := meta["step"].(string)
		require.NotEmpty(t, step)
		steps[step] = struct{}{}
	}

	assert.Len(t, eventIDs, updates, "each event needs its own event_id for consumer dedupe")
	assert.Len(t, steps, updates, "each event must carry the patch its own call committed, with no cross-talk")
	mockDS.AssertExpectations(t)
}

func TestMetadataUpdatedWebhookDataFlattensResourceFields(t *testing.T) {
	ledger := model.Ledger{
		LedgerID: "ldg_flat",
		Name:     "Main",
		MetaData: map[string]interface{}{"k": "v"},
	}
	data, err := metadataUpdatedWebhookData(ledger)
	require.NoError(t, err)
	assert.Equal(t, "ldg_flat", data["ledger_id"])
	assert.Equal(t, "Main", data["name"])
	require.NotEmpty(t, data["event_id"])
	require.NotEmpty(t, data["timestamp"])
	_, nested := data["entity"]
	assert.False(t, nested)
}

func TestMergeMetadataDoesNotMutateInputs(t *testing.T) {
	current := map[string]interface{}{"a": 1}
	newMeta := map[string]interface{}{"b": 2}
	merged := mergeMetadata(current, newMeta)
	assert.Equal(t, map[string]interface{}{"a": 1, "b": 2}, merged)
	assert.Equal(t, map[string]interface{}{"a": 1}, current)
	assert.Equal(t, map[string]interface{}{"b": 2}, newMeta)
}
