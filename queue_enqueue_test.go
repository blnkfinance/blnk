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
	"fmt"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testRedisAddr = "localhost:6379"

// uniqueQueueName returns a queue name unique to this test run so concurrent
// or repeated runs never see each other's tasks.
func uniqueQueueName(prefix string) string {
	return fmt.Sprintf("%s_%s_%d", prefix, gofakeit.LetterN(6), time.Now().UnixNano())
}

// newTestQueue builds a Queue backed by the real Redis instance, registering
// cleanup that removes every queue named in cleanupQueues.
func newTestQueue(t *testing.T, conf *config.Configuration, cleanupQueues ...string) (*Queue, *asynq.Inspector) {
	t.Helper()
	if conf.Redis.Dns == "" {
		conf.Redis.Dns = testRedisAddr
	}
	config.ConfigStore.Store(conf)

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: testRedisAddr})
	q := NewQueue(conf, client)
	inspector := q.Inspector

	t.Cleanup(func() {
		for _, name := range cleanupQueues {
			_, _ = inspector.DeleteAllPendingTasks(name)
			_, _ = inspector.DeleteAllScheduledTasks(name)
			_ = inspector.DeleteQueue(name, true)
		}
		_ = client.Close()
	})
	return q, inspector
}

func TestQueueInflightExpiry_ScheduledExactlyAtExpiryTime(t *testing.T) {
	expiryQueue := uniqueQueueName("inflight_expiry")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			InflightExpiryQueue: expiryQueue,
			NumberOfQueues:      1,
		},
	}
	q, inspector := newTestQueue(t, conf, expiryQueue)

	txn := getTransactionMock(500, false)
	expiresAt := time.Now().Add(45 * time.Minute)
	txn.InflightExpiryDate = expiresAt

	require.NoError(t, q.QueueInflightExpiry(context.Background(), &txn))

	task, err := inspector.GetTaskInfo(expiryQueue, txn.TransactionID)
	require.NoError(t, err, "expiry task must exist on the inflight expiry queue keyed by transaction ID")
	require.NotNil(t, task)

	assert.Equal(t, expiryQueue, task.Queue)
	assert.Equal(t, expiryQueue, task.Type, "task type must match the queue name for worker routing")
	assert.Equal(t, asynq.TaskStateScheduled, task.State, "future expiry must be scheduled, not pending")

	// The whole point of the task: it must fire AT the expiry time.
	assert.WithinDuration(t, expiresAt, task.NextProcessAt, 5*time.Second,
		"expiry task must be scheduled to process at the inflight expiry timestamp")

	// Payload is the JSON-encoded transaction ID string.
	var payloadID string
	require.NoError(t, json.Unmarshal(task.Payload, &payloadID))
	assert.Equal(t, txn.TransactionID, payloadID)
}

func TestQueueInflightExpiry_ZeroExpiryDateIsSkipped(t *testing.T) {
	expiryQueue := uniqueQueueName("inflight_expiry")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			InflightExpiryQueue: expiryQueue,
			NumberOfQueues:      1,
		},
	}
	q, inspector := newTestQueue(t, conf, expiryQueue)

	txn := getTransactionMock(500, false)
	txn.InflightExpiryDate = time.Time{} // zero: no expiry configured

	require.NoError(t, q.QueueInflightExpiry(context.Background(), &txn))

	task, err := inspector.GetTaskInfo(expiryQueue, txn.TransactionID)
	assert.Error(t, err, "no task should exist for a transaction without an expiry date")
	assert.Nil(t, task)
}

func TestQueueInflightExpiry_DuplicateTransactionIDConflicts(t *testing.T) {
	expiryQueue := uniqueQueueName("inflight_expiry")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			InflightExpiryQueue: expiryQueue,
			NumberOfQueues:      1,
		},
	}
	q, _ := newTestQueue(t, conf, expiryQueue)

	txn := getTransactionMock(500, false)
	txn.InflightExpiryDate = time.Now().Add(30 * time.Minute)

	require.NoError(t, q.QueueInflightExpiry(context.Background(), &txn))

	// Re-queuing the same transaction must not silently create a second timer.
	err := q.QueueInflightExpiry(context.Background(), &txn)
	assert.Error(t, err, "duplicate expiry enqueue for the same transaction ID must conflict")
}

func TestQueueInflightExpiry_PastExpiryBecomesImmediatelyPending(t *testing.T) {
	expiryQueue := uniqueQueueName("inflight_expiry")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			InflightExpiryQueue: expiryQueue,
			NumberOfQueues:      1,
		},
	}
	q, inspector := newTestQueue(t, conf, expiryQueue)

	txn := getTransactionMock(500, false)
	txn.InflightExpiryDate = time.Now().Add(-10 * time.Minute) // already expired

	require.NoError(t, q.QueueInflightExpiry(context.Background(), &txn))

	task, err := inspector.GetTaskInfo(expiryQueue, txn.TransactionID)
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, asynq.TaskStatePending, task.State,
		"an expiry in the past must be processed immediately, not scheduled into the future")
}

func TestQueueIndexData_SkippedWhenTypesenseNotConfigured(t *testing.T) {
	indexQueue := uniqueQueueName("index")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			IndexQueue:     indexQueue,
			NumberOfQueues: 1,
		},
		TypeSense: config.TypeSenseConfig{Dns: ""}, // search disabled
	}
	q, inspector := newTestQueue(t, conf, indexQueue)

	require.NoError(t, q.queueIndexData("doc_1", "transactions", map[string]string{"id": "doc_1"}))

	tasks, err := inspector.ListPendingTasks(indexQueue)
	if err == nil {
		assert.Empty(t, tasks, "nothing should be enqueued when TypeSense is not configured")
	}
	// err != nil means the queue was never created, which is equally correct.
}

func TestQueueIndexData_EnqueuesCollectionAndPayload(t *testing.T) {
	indexQueue := uniqueQueueName("index")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			IndexQueue:     indexQueue,
			NumberOfQueues: 1,
		},
		TypeSense: config.TypeSenseConfig{Dns: "http://localhost:8108"},
	}
	q, inspector := newTestQueue(t, conf, indexQueue)

	doc := map[string]interface{}{"balance_id": "bln_777", "currency": "USD"}
	require.NoError(t, q.queueIndexData("bln_777", "balances", doc))

	tasks, err := inspector.ListPendingTasks(indexQueue)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, indexQueue, task.Queue)
	assert.Equal(t, indexQueue, task.Type, "single-document index tasks use the queue name as task type")

	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(task.Payload, &payload))
	assert.Equal(t, "balances", payload["collection"])
	inner, ok := payload["payload"].(map[string]interface{})
	require.True(t, ok, "payload field must carry the document")
	assert.Equal(t, "bln_777", inner["balance_id"])
	assert.Equal(t, "USD", inner["currency"])
}

func TestQueueIndexData_UnserializablePayloadReturnsError(t *testing.T) {
	indexQueue := uniqueQueueName("index")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			IndexQueue:     indexQueue,
			NumberOfQueues: 1,
		},
		TypeSense: config.TypeSenseConfig{Dns: "http://localhost:8108"},
	}
	q, inspector := newTestQueue(t, conf, indexQueue)

	err := q.queueIndexData("bad_doc", "transactions", make(chan int))
	assert.Error(t, err, "non-JSON-serializable data must fail loudly")

	tasks, listErr := inspector.ListPendingTasks(indexQueue)
	if listErr == nil {
		assert.Empty(t, tasks)
	}
}

func TestQueueIndexBatch_EnqueuesBatchTaskType(t *testing.T) {
	indexQueue := uniqueQueueName("index")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			IndexQueue:     indexQueue,
			NumberOfQueues: 1,
		},
		TypeSense: config.TypeSenseConfig{Dns: "http://localhost:8108"},
	}
	q, inspector := newTestQueue(t, conf, indexQueue)

	batch := map[string]interface{}{
		"dependencies": []map[string]interface{}{
			{"collection": "balances", "payload": map[string]string{"balance_id": "bln_1"}},
		},
		"primary": map[string]interface{}{
			"collection": "transactions",
			"payload":    map[string]string{"transaction_id": "txn_1"},
		},
	}
	require.NoError(t, q.queueIndexBatch(batch))

	tasks, err := inspector.ListPendingTasks(indexQueue)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, indexQueue, task.Queue, "batch tasks ride on the index queue")
	assert.Equal(t, "new:index:batch", task.Type, "batch tasks must use the dedicated batch task type for routing")

	var roundTripped map[string]interface{}
	require.NoError(t, json.Unmarshal(task.Payload, &roundTripped))
	assert.Contains(t, roundTripped, "dependencies")
	assert.Contains(t, roundTripped, "primary")
}

func TestQueueIndexBatch_SkippedWithoutTypesenseAndFailsOnBadBatch(t *testing.T) {
	indexQueue := uniqueQueueName("index")

	t.Run("skipped when TypeSense disabled", func(t *testing.T) {
		conf := &config.Configuration{
			Redis: config.RedisConfig{Dns: testRedisAddr},
			Queue: config.QueueConfig{
				IndexQueue:     indexQueue,
				NumberOfQueues: 1,
			},
			TypeSense: config.TypeSenseConfig{Dns: ""},
		}
		q, inspector := newTestQueue(t, conf, indexQueue)

		require.NoError(t, q.queueIndexBatch(map[string]string{"k": "v"}))
		tasks, err := inspector.ListPendingTasks(indexQueue)
		if err == nil {
			assert.Empty(t, tasks)
		}
	})

	t.Run("unserializable batch returns error", func(t *testing.T) {
		conf := &config.Configuration{
			Redis: config.RedisConfig{Dns: testRedisAddr},
			Queue: config.QueueConfig{
				IndexQueue:     indexQueue,
				NumberOfQueues: 1,
			},
			TypeSense: config.TypeSenseConfig{Dns: "http://localhost:8108"},
		}
		q, _ := newTestQueue(t, conf, indexQueue)

		assert.Error(t, q.queueIndexBatch(make(chan int)))
	})
}

func TestGetTransactionFromQueue_FindsEnqueuedTransaction(t *testing.T) {
	txQueuePrefix := uniqueQueueName("tx")
	numberOfQueues := 4
	cleanup := make([]string, 0, numberOfQueues)
	for i := 1; i <= numberOfQueues; i++ {
		cleanup = append(cleanup, fmt.Sprintf("%s_%d", txQueuePrefix, i))
	}

	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			TransactionQueue: txQueuePrefix,
			NumberOfQueues:   numberOfQueues,
			MaxRetryAttempts: 3,
		},
	}
	q, _ := newTestQueue(t, conf, cleanup...)

	txn := getTransactionMock(250.75, false)
	txn.Source = "bln_source_balance"

	require.NoError(t, q.Enqueue(context.Background(), &txn))

	got, err := q.GetTransactionFromQueue(txn.TransactionID)
	require.NoError(t, err)
	require.NotNil(t, got, "enqueued transaction must be retrievable from one of the sharded queues")
	assert.Equal(t, txn.TransactionID, got.TransactionID)
	assert.Equal(t, txn.Reference, got.Reference)
	assert.Equal(t, txn.Source, got.Source)
	assert.Equal(t, 250.75, got.Amount)
}

func TestGetTransactionFromQueue_UnknownIDReturnsNilNil(t *testing.T) {
	txQueuePrefix := uniqueQueueName("tx")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			TransactionQueue: txQueuePrefix,
			NumberOfQueues:   2,
			MaxRetryAttempts: 3,
		},
	}
	q, _ := newTestQueue(t, conf, txQueuePrefix+"_1", txQueuePrefix+"_2")

	got, err := q.GetTransactionFromQueue("txn_does_not_exist_" + gofakeit.UUID())
	assert.NoError(t, err)
	assert.Nil(t, got, "missing transaction must be (nil, nil), not an error")
}

func TestEnqueue_HotLaneRoutingAndLookup(t *testing.T) {
	txQueuePrefix := uniqueQueueName("tx")
	hotQueue := uniqueQueueName("hot_tx")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			TransactionQueue: txQueuePrefix,
			NumberOfQueues:   2,
			MaxRetryAttempts: 3,
			EnableHotLane:    true,
			HotQueueName:     hotQueue,
		},
	}
	q, inspector := newTestQueue(t, conf, txQueuePrefix+"_1", txQueuePrefix+"_2", hotQueue)

	txn := getTransactionMock(99, false)
	txn.Source = "bln_hot_source"
	txn.MetaData = map[string]interface{}{hotpairs.QueueLaneMetaKey: hotpairs.LaneHot}

	require.NoError(t, q.Enqueue(context.Background(), &txn))

	// The task must be on the hot queue, not the sharded normal queues.
	task, err := inspector.GetTaskInfo(hotQueue, txn.TransactionID)
	require.NoError(t, err, "hot-lane transaction must land on the configured hot queue")
	require.NotNil(t, task)
	assert.Equal(t, hotQueue, task.Type)

	for i := 1; i <= 2; i++ {
		normalTask, err := inspector.GetTaskInfo(fmt.Sprintf("%s_%d", txQueuePrefix, i), txn.TransactionID)
		if err == nil && normalTask != nil {
			t.Fatalf("hot transaction leaked into normal queue %s_%d", txQueuePrefix, i)
		}
	}

	// GetTransactionFromQueue must also check the hot lane.
	got, err := q.GetTransactionFromQueue(txn.TransactionID)
	require.NoError(t, err)
	require.NotNil(t, got, "GetTransactionFromQueue must search the hot queue when hot lane is enabled")
	assert.Equal(t, txn.TransactionID, got.TransactionID)
}

func TestEnqueue_ScheduledTransactionIsScheduledAtScheduledFor(t *testing.T) {
	txQueuePrefix := uniqueQueueName("tx")
	conf := &config.Configuration{
		Redis: config.RedisConfig{Dns: testRedisAddr},
		Queue: config.QueueConfig{
			TransactionQueue: txQueuePrefix,
			NumberOfQueues:   1,
			MaxRetryAttempts: 3,
		},
	}
	q, inspector := newTestQueue(t, conf, txQueuePrefix+"_1")

	txn := getTransactionMock(10, false)
	txn.Source = "bln_scheduled_source"
	scheduledFor := time.Now().Add(2 * time.Hour)
	txn.ScheduledFor = scheduledFor

	require.NoError(t, q.Enqueue(context.Background(), &txn))

	task, err := inspector.GetTaskInfo(txQueuePrefix+"_1", txn.TransactionID)
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, asynq.TaskStateScheduled, task.State)
	assert.WithinDuration(t, scheduledFor, task.NextProcessAt, 5*time.Second,
		"scheduled transaction must be processed at ScheduledFor, not immediately")
}
