package blnk

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/hibiken/asynq"
	"github.com/jerry-enebeli/blnk/model"
)

const TANSACTION_QUEUE = "new:transaction:test"

type Queue struct {
	client    *asynq.Client
	inspector *asynq.Inspector
}

type TransactionTypePayload struct {
	Data model.Transaction
}

func NewQueue(conf *config.Configuration) *Queue {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	return &Queue{
		client:    client,
		inspector: inspector,
	}
}

func (q *Queue) Enqueue(ctx context.Context, transaction *model.Transaction) error {
	payload, err := json.Marshal(transaction)
	if err != nil {
		log.Fatal(err)
	}
	info, err := q.client.Enqueue(q.geTask(transaction, payload), asynq.MaxRetry(5))
	if err != nil {
		log.Println(err, info)
		return err
	}
	log.Printf(" [*] Successfully enqueued task: %+v", transaction.TransactionID)

	return nil
}

func (q *Queue) geTask(transaction *model.Transaction, payload []byte) *asynq.Task {
	taskOptions := []asynq.Option{asynq.TaskID(transaction.TransactionID), asynq.Queue("transactions")}

	if !transaction.ScheduledFor.IsZero() {
		taskOptions = append(taskOptions, asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
	}
	return asynq.NewTask(TANSACTION_QUEUE, payload, taskOptions...)
}
