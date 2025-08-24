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
	"log"
	"testing"

	"github.com/blnkfinance/blnk/config"
	redis_db "github.com/blnkfinance/blnk/internal/redis-db"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
)

func TestEnqueueImmediateTransactionSuccess(t *testing.T) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
	}
	config.ConfigStore.Store(cnf)

	redisOption, err := redis_db.ParseRedisURL("localhost:6379", false)
	if err != nil {
		log.Fatalf("Error parsing Redis URL: %v", err)
	}
	queueOptions := asynq.RedisClientOpt{Addr: redisOption.Addr, Password: redisOption.Password, DB: redisOption.DB, TLSConfig: redisOption.TLSConfig}
	client := asynq.NewClient(queueOptions)
	inspector := asynq.NewInspector(queueOptions)

	q := NewQueue(cnf)
	q.Client = client
	q.Inspector = inspector

	transaction := getTransactionMock(100, false)
	_, err = json.Marshal(transaction)
	assert.NoError(t, err)

	err = q.Enqueue(context.Background(), &transaction)
	assert.NoError(t, err)

	task, err := inspector.GetTaskInfo(cnf.Queue.WebhookQueue, transaction.TransactionID)
	if err != nil {
		return
	}

	assert.Equal(t, "tx_123", task.ID)
}

func TestEnqueueScheduledTransaction(t *testing.T) {
	conf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
	}
	config.ConfigStore.Store(conf)

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

	task, err := inspector.GetTaskInfo(conf.Queue.WebhookQueue, transaction.TransactionID)
	if err != nil {
		return
	}
	assert.Equal(t, "tx_123", task.ID)
}

func TestEnqueueWithAsynqClientEnqueueError(t *testing.T) {
	conf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
	}

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

	task, err := inspector.GetTaskInfo(conf.Queue.WebhookQueue, "tx_1235")
	if err != nil {
		return
	}

	assert.Equal(t, "tx_123", task.ID)
}
