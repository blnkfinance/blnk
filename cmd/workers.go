package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jerry-enebeli/blnk/internal/notification"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/database"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/spf13/cobra"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/hibiken/asynq"
)

func processTransaction(cxt context.Context, t *asynq.Task) error {
	var txn model.Transaction
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		fmt.Println("error", err)
		return err
	}
	cfg, err := config.Fetch()
	if err != nil {
		fmt.Println("error", err)
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

	log.Printf(" [*] Processing Transaction %s. source %s destination %s", txn.TransactionID, txn.Source, txn.Destination)
	err = newBlnk.ApplyBalanceToQueuedTransaction(cxt, &txn)
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
				return
			}
			srv := asynq.NewServer(
				asynq.RedisClientOpt{Addr: conf.Redis.Dns},
				asynq.Config{Concurrency: 1, Queues: map[string]int{"transactions": 5, "credit-transactions": 5}, GroupMaxDelay: time.Second,
					GroupAggregator: asynq.GroupAggregatorFunc(aggregateDebit)},
			)
			mux := asynq.NewServeMux()
			mux.HandleFunc(blnk.TANSACTION_QUEUE, processTransaction)
			if err := srv.Run(mux); err != nil {
				notification.NotifyError(err)
				log.Fatal(err)
			}
		},
	}

	return cmd
}
