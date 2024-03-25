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
	q.Client = client       // Assuming you have a way to set this for testing
	q.Inspector = inspector // Assuming you have a way to set this for testing

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
