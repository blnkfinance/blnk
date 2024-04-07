package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/database"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/hibiken/asynq"
)

const NumberOfQueues = 5

func processTransaction(cxt context.Context, t *asynq.Task) error {
	var txn model.Transaction
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		logrus.Error(err)
		return err
	}
	cfg, err := config.Fetch()
	if err != nil {
		return err
	}
	db, err := database.NewDataSource(cfg)
	if err != nil {
		log.Fatalf("Error getting datasource: %v\n", err)
	}
	newBlnk, err := blnk.NewBlnk(db)
	if err != nil {
		log.Fatalf("Error creating blnk: %v\n", err)
	}

	logrus.Printf(" [*] Processing Transaction %s. source %s destination %s", txn.TransactionID, txn.Source, txn.Destination)
	_, err = newBlnk.RecordTransaction(cxt, &txn)
	if err != nil {
		return err
	}
	return nil
}

func aggregateDebit(_ string, tasks []*asynq.Task) *asynq.Task {
	var amount int64
	var source, destination string
	groupIds := make([]string, 0)
	for _, task := range tasks {
		var transaction model.Transaction
		if err := json.Unmarshal(task.Payload(), &transaction); err != nil {
			log.Printf("Failed to unmarshal task payload: %v", err)
			continue // Skip this task if unmarshalling fails
		}

		amount += transaction.Amount
		groupIds = append(groupIds, transaction.TransactionID)
		source = transaction.Source
		destination = transaction.Destination

	}
	aggregatedTransaction := model.Transaction{
		Amount:      amount,
		Source:      source,
		Destination: destination,
		GroupIds:    groupIds,
	}

	payload, err := json.Marshal(aggregatedTransaction)
	if err != nil {
		log.Fatalf(
			"Failed to marshal aggregated transaction: %v", err)
	}

	aggregatedTask := asynq.NewTask("new:transactions", payload)
	return aggregatedTask
}

func workerCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "start blnk workers",
		Run: func(cmd *cobra.Command, args []string) {
			conf, err := config.Fetch()
			if err != nil {
				log.Println("Error fetching config:", err)
				return
			}

			// Dynamically create the queues map based on the number of queues
			queues := make(map[string]int)
			queues[blnk.WEBHOOK_QUEUE] = 1
			for i := 1; i <= NumberOfQueues; i++ {
				queueName := fmt.Sprintf("%s_%d", blnk.TRANSACTION_QUEUE, i)
				queues[queueName] = 1
			}

			srv := asynq.NewServer(
				asynq.RedisClientOpt{Addr: conf.Redis.Dns},
				asynq.Config{
					Concurrency:     1,
					Queues:          queues,
					GroupMaxDelay:   time.Second,
					GroupAggregator: asynq.GroupAggregatorFunc(aggregateDebit),
				},
			)

			mux := asynq.NewServeMux()
			// Register handler for each queue
			for i := 1; i <= NumberOfQueues; i++ {
				queueName := fmt.Sprintf("%s_%d", blnk.TRANSACTION_QUEUE, i)
				mux.HandleFunc(queueName, processTransaction)

			}

			mux.HandleFunc(blnk.WEBHOOK_QUEUE, blnk.ProcessWebhook)
			if err := srv.Run(mux); err != nil {
				log.Fatal("Error running server:", err)
			}
		},
	}

	return cmd
}
