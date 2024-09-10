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

	"github.com/hibiken/asynq"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/stretchr/testify/assert"
)

func TestEnqueueImmediateTransactionSuccess(t *testing.T) {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})

	q := NewQueue(&config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
	})
	q.Client = client
	q.Inspector = inspector

	transaction := getTransactionMock(100, false)
	_, err := json.Marshal(transaction)
	assert.NoError(t, err)

	err = q.Enqueue(context.Background(), &transaction)
	assert.NoError(t, err)

	task, err := inspector.GetTaskInfo(WEBHOOK_QUEUE, transaction.TransactionID)
	if err != nil {
		return
	}

	assert.Equal(t, "tx_123", task.ID)
}

func TestEnqueueScheduledTransaction(t *testing.T) {

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})

	q := NewQueue(&config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
	})
	q.Client = client
	q.Inspector = inspector

	transaction := getTransactionMock(100, false)

	_, err := json.Marshal(transaction)
	assert.NoError(t, err)

	err = q.Enqueue(context.Background(), &transaction)
	assert.NoError(t, err)

	task, err := inspector.GetTaskInfo(WEBHOOK_QUEUE, transaction.TransactionID)
	if err != nil {
		return
	}
	assert.Equal(t, "tx_123", task.ID)
}

func TestEnqueueWithAsynqClientEnqueueError(t *testing.T) {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})

	q := NewQueue(&config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
	})
	q.Client = client
	q.Inspector = inspector

	transaction := getTransactionMock(100, false)

	_, err := json.Marshal(transaction)
	assert.NoError(t, err)

	err = q.Enqueue(context.Background(), &transaction)
	assert.NoError(t, err)

	task, err := inspector.GetTaskInfo(WEBHOOK_QUEUE, "tx_1235")
	if err != nil {
		return
	}

	assert.Equal(t, "tx_123", task.ID)

}
