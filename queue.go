package blnk

import (
	"encoding/json"
	"log"
	"time"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/hibiken/asynq"
	"github.com/jerry-enebeli/blnk/model"
)

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

func (q *Queue) Enqueue(transaction model.Transaction) error {
	payload, err := json.Marshal(transaction)
	if err != nil {
		log.Fatal(err)
	}
	task := q.geTask(transaction, payload)
	if !transaction.ScheduledFor.IsZero() {
		task = q.getScheduledTasked(transaction, payload)
	}

	info, err := q.client.Enqueue(task, asynq.MaxRetry(5))
	if err != nil {
		log.Println("here", err, info)
	}
	log.Printf(" [*] Successfully enqueued task: %+v", transaction.TransactionID)

	return nil
}

func (q *Queue) geTask(transaction model.Transaction, payload []byte) *asynq.Task {
	if transaction.DRCR == "Credit" {
		//push to credit queue for higher priority and avoid grouping
		return asynq.NewTask("new:transaction:credit", payload, asynq.TaskID(transaction.TransactionID), asynq.Queue("credit-transactions"))
	}

	return asynq.NewTask("new:transaction", payload, asynq.TaskID(transaction.TransactionID), asynq.Queue("transactions"), asynq.Group(transaction.BalanceID))
}

func (q *Queue) getScheduledTasked(transaction model.Transaction, payload []byte) *asynq.Task {

	if transaction.DRCR == "Credit" {
		//push to credit queue for higher priority and avoid grouping
		return asynq.NewTask("new:transaction:credit", payload, asynq.TaskID(transaction.TransactionID), asynq.Queue("credit-transactions"), asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
	}

	return asynq.NewTask("new:transactions", payload, asynq.TaskID(transaction.TransactionID), asynq.Queue("transactions"), asynq.Group(transaction.BalanceID), asynq.ProcessIn(time.Until(transaction.ScheduledFor)))

}
