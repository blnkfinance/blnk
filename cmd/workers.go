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

func processTransaction(_ context.Context, t *asynq.Task) error {
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

	log.Printf(" [*] Processing Transaction %s on balance %s", txn.TransactionID, txn.BalanceID)
	err = newBlnk.ApplyBalanceToQueuedTransaction(txn)
	if err != nil {
		return err
	}
	return nil
}

func aggregateDebit(_ string, tasks []*asynq.Task) *asynq.Task {
	var totalDebit int64
	var balanceId string
	groupIds := make([]string, 0)
	for _, task := range tasks {
		var transaction model.Transaction
		if err := json.Unmarshal(task.Payload(), &transaction); err != nil {
			log.Printf("Failed to unmarshal task payload: %v", err)
			continue // Skip this task if unmarshalling fails
		}
		if transaction.DRCR == "Debit" {
			totalDebit += transaction.Amount
			groupIds = append(groupIds, transaction.TransactionID)
			balanceId = transaction.BalanceID
		}
	}

	aggregatedTransaction := model.Transaction{
		Amount:    totalDebit,
		BalanceID: balanceId,
		GroupIds:  groupIds,
		DRCR:      "Debit",
	}

	payload, err := json.Marshal(aggregatedTransaction)
	if err != nil {
		log.Fatalf("Failed to marshal aggregated transaction: %v", err)
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
				notification.NotifyError(err)
				return
			}
			srv := asynq.NewServer(
				asynq.RedisClientOpt{Addr: conf.Redis.Dns},
				asynq.Config{Concurrency: 10, Queues: map[string]int{"transactions": 5, "credit-transactions": 5}, GroupMaxDelay: time.Second,
					GroupAggregator: asynq.GroupAggregatorFunc(aggregateDebit)},
			)
			mux := asynq.NewServeMux()
			mux.HandleFunc("new:transaction:credit", processTransaction)
			mux.HandleFunc("new:transaction", processTransaction)
			if err := srv.Run(mux); err != nil {
				notification.NotifyError(err)
				log.Fatal(err)
			}
		},
	}

	return cmd
}
