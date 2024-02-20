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

func (q *Queue) Enqueue(transaction model.Transaction, l *Blnk) error {
	payload, err := json.Marshal(transaction)
	if err != nil {
		log.Fatal(err)
	}
	task := q.geTask(transaction, payload)
	if !transaction.ScheduledFor.IsZero() {
		task = q.getScheduledTasked(transaction, payload)
	}

	info, err := q.client.Enqueue(task)
	if err != nil {
		log.Println("here", err, info)
	}
	log.Printf(" [*] Successfully enqueued task: %+v", transaction.TransactionID)

	return nil
}

func (q *Queue) geTask(transaction model.Transaction, payload []byte) *asynq.Task {
	return asynq.NewTask("new:transaction", payload, asynq.TaskID(transaction.TransactionID), asynq.Queue("transactions"), asynq.Group(transaction.BalanceID))
}

func (q *Queue) getScheduledTasked(transaction model.Transaction, payload []byte) *asynq.Task {
	return asynq.NewTask("new:transaction", payload, asynq.TaskID(transaction.TransactionID), asynq.Queue("transactions"), asynq.Group(transaction.BalanceID), asynq.ProcessIn(time.Until(transaction.ScheduledFor)))
}

// dequeueDB fetches queued transactions from db
// process each transaction
func dequeueDB(messageChan chan model.Transaction, l Blnk) error {
	for {
		transaction, err := l.datasource.GetNextQueuedTransaction()
		if err != nil {
			return err
		}
		if transaction != nil {
			messageChan <- *transaction
		}

	}
}

func Dequeue(messageChan chan model.Transaction, l Blnk) error {
	err := dequeueDB(messageChan, l)
	if err != nil {
		return err
	}
	return nil
}
