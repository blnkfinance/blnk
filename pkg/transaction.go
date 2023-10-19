package pkg

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-co-op/gocron"

	config2 "github.com/jerry-enebeli/blnk/config"

	"github.com/jerry-enebeli/blnk/datasources"

	"github.com/jerry-enebeli/blnk"
)

const (
	STATUS_SCHEDULED  = "SCHEDULED"
	STATUS_QUEUED     = "QUEUED"
	STATUS_SUCCESSFUL = "SUCCESSFUL"
)

func (l Blnk) validateBlnCurrency(transaction *blnk.Transaction) (blnk.Balance, error) {
	balance, err := l.datasource.GetBalanceByID(transaction.BalanceID, nil)
	if err != nil {
		return blnk.Balance{}, err
	}
	if balance.Currency != transaction.Currency {
		//todo write flagged transactions table
		return blnk.Balance{}, errors.New("transaction currency does not match the balance currency. Please ensure they are consistent")
	}
	return *balance, nil
}

func getQueueName() (string, error) {
	conf, err := config2.Fetch()
	if err != nil {
		return "", err
	}
	return conf.ConfluentKafka.QueueName, nil
}

// ScheduleTransaction checks if the current transaction has a scheduled date. if it does it updates the tag to SCHEDULED and set the status to scheduled.
func (l Blnk) scheduleTransaction(transaction *blnk.Transaction) error {
	if transaction.ScheduledFor.Year() == 1 {
		return nil
	}
	transaction.Status = STATUS_SCHEDULED
	transaction.Tag = STATUS_SCHEDULED
	transaction.SkipBalanceUpdate = true
	return nil
}

// Helper function to get the opposite DRCR value
func inverseDRCR(drcr string) string {
	if drcr == "Debit" {
		return "Credit"
	}
	return "Debit"
}

func (l Blnk) applyBalanceToQueuedTransaction(transaction blnk.Transaction) error {
	//gets balance to apply transaction to
	balance, err := l.datasource.GetBalanceByID(transaction.BalanceID, nil)
	if err != nil {
		return err
	}
	//calculates new balances
	err = l.applyTransactionToBalance(balance, &transaction)
	if err != nil {
		return err
	}

	//updates the transaction status from QUEUED to SUCCESSFUL
	err = l.datasource.UpdateTransactionStatus(transaction.TransactionID, STATUS_SUCCESSFUL) //todo update the before and after for all balances
	if err != nil {
		return err

	}
	//updates balance in the db
	err = l.datasource.UpdateBalance(balance)
	if err != nil {
		return err
	}

	return nil
}

func (l Blnk) validateTxnAndReturnBalance(transaction blnk.Transaction) (blnk.Balance, error) {
	txn, err := l.GetTransactionByRef(transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return blnk.Balance{}, err
	}

	if errors.Is(err, nil) && txn.TransactionID != "" {
		return blnk.Balance{}, fmt.Errorf("this reference has already been used. Please use a unique reference")
	}

	balance, err := l.validateBlnCurrency(&transaction)
	if err != nil {
		return blnk.Balance{}, err
	}
	return balance, nil
}

func (l Blnk) recordTransaction(LedgerID string, transaction blnk.Transaction) (blnk.Transaction, error) {
	transaction, err := l.datasource.RecordTransaction(transaction)
	if err != nil {
		return transaction, err
	}
	return transaction, nil
}

func (l Blnk) applyTransactionToBalance(balance *blnk.Balance, transaction *blnk.Transaction) error {
	err := balance.UpdateBalances(transaction)
	if err != nil {
		return err
	}
	return nil
}

func (l Blnk) RecordTransaction(transaction blnk.Transaction) (blnk.Transaction, error) {
	balance, err := l.validateTxnAndReturnBalance(transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}

	transaction.LedgerID = balance.LedgerID
	err = l.applyTransactionToBalance(&balance, &transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}

	transaction.Status = STATUS_SUCCESSFUL
	err = l.scheduleTransaction(&transaction) //checks if it's a scheduled transaction and updates the status to scheduled
	if err != nil {
		return blnk.Transaction{}, err
	}
	transaction, err = l.recordTransaction(balance.LedgerID, transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}

	//if SkipBalanceUpdate is true it skips the db update leave the balance as it was before the transaction was processed.
	//This is useful for when we want to store a transaction record but don't compute the balance. it's used in places like scheduling transactions
	if !transaction.SkipBalanceUpdate {
		err = l.datasource.UpdateBalance(&balance)
		if err != nil {
			return blnk.Transaction{}, err
		}
	}

	return transaction, nil
}

func (l Blnk) QueueTransaction(transaction blnk.Transaction) (blnk.Transaction, error) {
	_, err := l.validateTxnAndReturnBalance(transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}
	transaction.Status = STATUS_QUEUED
	transaction.SkipBalanceUpdate = true                //does not apply transaction to the balance
	transaction, err = l.RecordTransaction(transaction) //saves transaction to db
	if err != nil {
		return blnk.Transaction{}, err
	}
	go func() {
		err = Enqueue(transaction) //send transaction to kafka
		if err != nil {
			log.Printf("Error: Error queuing transaction: %v", err)
		}
	}()

	return transaction, nil
}

func (l Blnk) ProcessTransactionFromQueue() {
	messageChan := make(chan blnk.Transaction)
	go func() {
		err := Dequeue(messageChan)
		if err != nil {
			log.Println("Message: Error fetching transactions from queue")
		}
	}()
	for {
		transaction, ok := <-messageChan
		if !ok {
			log.Println("Message: No transaction from queue")
		}

		err := l.applyBalanceToQueuedTransaction(transaction)
		if err != nil {
			err := Enqueue(transaction)
			if err != nil {
				log.Printf("Error: Error re-queuing scheduled transaction: %v", err)
			}
		}

	}

}

func (l Blnk) GetTransaction(TransactionID string) (blnk.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l Blnk) GetScheduledTransaction() {
	log.Println("Message: Fetching scheduled transactions...")
	s := gocron.NewScheduler(time.UTC)
	_, err := s.Every(5).Seconds().Do(func() {
		transactions, err := l.datasource.GetScheduledTransactions()
		if err != nil {
			return
		}
		//log.Printf("Message: Got %d scheduled transactions...", len(transactions))
		for _, transaction := range transactions {
			err = Enqueue(transaction) //send transaction to kafka
			if err != nil {
				log.Printf("Error: Error queuing scheduled transaction: %v", err)
			}
		}
	})
	if err != nil {
		log.Printf("Error: Error starting scheduled transaction job: %v", err)
	}
	s.StartAsync()
}

func (l Blnk) GetTransactionByRef(reference string) (blnk.Transaction, error) {
	return l.datasource.GetTransactionByRef(reference)
}

func (l Blnk) UpdateTransactionStatus(id string, status string) error {
	return l.datasource.UpdateTransactionStatus(id, status)
}

func (l Blnk) GroupTransactionsByCurrency() (map[string]struct {
	TotalAmount int64 `json:"total_amount"`
}, error) {
	return l.datasource.GroupTransactionsByCurrency()
}

func (l Blnk) RefundTransaction(transactionID string) (blnk.Transaction, error) {
	// Retrieve the original transaction by its ID
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		return blnk.Transaction{}, err
	}

	// Create a new inverse transaction with the opposite "drcr" value
	refundTxn := blnk.Transaction{
		Tag:       "Refund",
		Reference: datasources.GenerateUUIDWithSuffix("ref"),
		Amount:    originalTxn.Amount, // Inverse amount
		Currency:  originalTxn.Currency,
		DRCR:      inverseDRCR(originalTxn.DRCR), // Function to get the opposite DRCR
		LedgerID:  originalTxn.LedgerID,
		BalanceID: originalTxn.BalanceID,
		MetaData:  map[string]interface{}{"refunded_transaction_id": transactionID},
	}

	// Record the new inverse transaction
	refundTxn, err = l.RecordTransaction(refundTxn)
	if err != nil {
		return blnk.Transaction{}, err
	}

	return refundTxn, nil
}
