/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blnk

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/hooks"
	"github.com/blnkfinance/blnk/model"
)

func TestRejectTransaction_QueuesPostTransactionHook(t *testing.T) {
	mr := miniredis.RunT(t)

	webhookQueue := "webhook_reject_hook"
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: mr.Addr()},
		Queue: config.QueueConfig{
			WebhookQueue:   webhookQueue,
			IndexQueue:     "index_reject_hook",
			NumberOfQueues: 1,
		},
		Server: config.ServerConfig{SecretKey: "test-secret"},
	}
	config.ConfigStore.Store(cnf)

	txn := &model.Transaction{
		TransactionID: "txn_reject_hook",
		Reference:     "visa_settlement_hook_q",
		Source:        "bln_source",
		Destination:   "bln_dest",
		Amount:        10,
		Precision:     100,
		Currency:      "MXN",
		Status:        StatusQueued,
		MetaData:      map[string]interface{}{},
	}
	model.ApplyPrecision(txn)

	mockDS := new(mocks.MockDataSource)
	mockDS.On("RecordTransaction", mock.Anything, mock.Anything).Return(txn, nil)

	b, err := NewBlnk(mockDS)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })

	ctx := context.Background()
	require.NoError(t, b.Hooks.RegisterHook(ctx, &hooks.Hook{
		Name:       "flexo-visa-settlement",
		URL:        "http://127.0.0.1:3000/webhooks/visa",
		Type:       hooks.PostTransaction,
		Active:     true,
		Timeout:    30,
		RetryCount: 5,
	}))

	reason := "failed to apply transaction to balances: insufficient funds in source balance"
	rejected, err := b.RejectTransaction(ctx, txn, reason)
	require.NoError(t, err)
	require.Equal(t, StatusRejected, rejected.Status)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: mr.Addr()})
	t.Cleanup(func() { _ = inspector.Close() })

	tasks, err := inspector.ListPendingTasks(webhookQueue)
	if err != nil {
		tasks = nil
	}

	var found bool
	for _, task := range tasks {
		if task.Type != "new:hook_execution" {
			continue
		}
		var payload hooks.HookTaskPayload
		require.NoError(t, json.Unmarshal(task.Payload, &payload))
		require.Equal(t, hooks.PostTransaction, payload.Payload.HookType)
		require.Equal(t, rejected.TransactionID, payload.Payload.TransactionID)

		var body model.Transaction
		require.NoError(t, json.Unmarshal(payload.Payload.Data, &body))
		require.Equal(t, StatusRejected, body.Status)
		require.Equal(t, reason, body.MetaData["blnk_rejection_reason"])
		found = true
	}
	require.True(t, found, "a rejected transaction must queue its POST_TRANSACTION hooks")
}
