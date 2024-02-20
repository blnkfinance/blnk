package blnk

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/jerry-enebeli/blnk/internal/notification"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/database"
)

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
)

func (l Blnk) validateBlnCurrency(transaction *model.Transaction) (model.Balance, error) {
	balance, err := l.datasource.GetBalanceByID(transaction.BalanceID, nil)
	if err != nil {
		return model.Balance{}, err
	}
	if balance.Currency != transaction.Currency {
		return model.Balance{}, fmt.Errorf("transaction %s currency %s does not match the balance %s currency %s. Please ensure they are consistent", transaction.TransactionID, transaction.Currency, balance.BalanceID, balance.Currency)
	}
	return *balance, nil
}

// Helper function to get the opposite DRCR value
func inverseDRCR(drcr string) string {
	if drcr == "Debit" {
		return "Credit"
	}
	return "Debit"
}

func (l Blnk) updateBalance(balance model.Balance) error {
	err := l.datasource.UpdateBalance(&balance)
	if err != nil {
		return err
	}
	go func() {
		l.checkBalanceMonitors(&balance)
	}()

	return nil
}

func (l Blnk) ApplyBalanceToQueuedTransaction(transaction model.Transaction) error {
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

	if len(transaction.GroupIds) > 0 {
		for _, id := range transaction.GroupIds {
			//updates the transaction status from QUEUED to SUCCESSFUL
			err = l.datasource.UpdateTransactionStatus(id, StatusApplied)
			if err != nil {
				return err
			}
		}
	} else {
		err = l.datasource.UpdateTransactionStatus(transaction.TransactionID, StatusApplied)
		if err != nil {
			return err
		}
	}

	//updates balance in the db
	err = l.updateBalance(*balance)
	if err != nil {
		return err
	}

	return nil
}

func (l Blnk) validateTxnAndReturnBalance(transaction model.Transaction) (model.Balance, error) {
	txn, err := l.GetTransactionByRef(transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.Balance{}, err
	}

	if errors.Is(err, nil) && txn.TransactionID != "" {
		return model.Balance{}, fmt.Errorf("this reference has already been used. Please use a unique reference")
	}

	balance, err := l.validateBlnCurrency(&transaction)
	if err != nil {
		notification.NotifyError(err)
		return model.Balance{}, err
	}

	return balance, nil
}

func (l Blnk) recordTransaction(transaction model.Transaction) (model.Transaction, error) {
	transaction, err := l.datasource.RecordTransaction(transaction)
	if err != nil {
		return transaction, err
	}
	return transaction, nil
}

func (l Blnk) applyTransactionToBalance(balance *model.Balance, transaction *model.Transaction) error {
	err := balance.UpdateBalances(transaction)
	if err != nil {
		return err
	}
	return nil
}

func (l Blnk) RecordTransaction(transaction model.Transaction) (model.Transaction, error) {
	balance, err := l.validateTxnAndReturnBalance(transaction)
	if err != nil {
		return model.Transaction{}, err
	}

	riskScore := l.ApplyFraudScore(&balance, transaction.Amount)
	if riskScore >= transaction.RiskToleranceThreshold && transaction.RiskToleranceThreshold > 0 {
		return model.Transaction{}, fmt.Errorf("this transaction has been flagged as a high risk")
	}
	transaction.RiskScore = riskScore

	transaction.LedgerID = balance.LedgerID
	err = l.applyTransactionToBalance(&balance, &transaction)
	if err != nil {
		return model.Transaction{}, err
	}

	if transaction.Status == "" {
		transaction.Status = StatusApplied
	}
	transaction, err = l.recordTransaction(transaction)
	if err != nil {
		return model.Transaction{}, err
	}

	//if SkipBalanceUpdate is true it skips the db update leave the balance as it was before the transaction was processed.
	//This is useful for when we want to store a transaction record but don't compute the balance. it's used in places like scheduling transactions
	if !transaction.SkipBalanceUpdate {
		err = l.updateBalance(balance)
		if err != nil {
			return model.Transaction{}, err
		}
	}

	return transaction, nil
}

func (l Blnk) QueueTransaction(transaction model.Transaction) (model.Transaction, error) {
	transaction.Status = StatusQueued
	transaction.SkipBalanceUpdate = true
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	}
	//does not apply transaction to the balance
	transaction, err := l.RecordTransaction(transaction) //saves transaction to db
	if err != nil {
		return model.Transaction{}, err
	}
	go func() {
		err := l.queue.Enqueue(transaction)
		if err != nil {
			notification.NotifyError(err)
			log.Printf("Error: Error queuing transaction: %v", err)
		}
	}()

	return transaction, nil
}

func (l Blnk) GetTransaction(TransactionID string) (model.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l Blnk) GetAllTransactions() ([]model.Transaction, error) {
	return l.datasource.GetAllTransactions()
}

func (l Blnk) GetTransactionByRef(reference string) (model.Transaction, error) {
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

func (l Blnk) RefundTransaction(transactionID string) (model.Transaction, error) {
	// Retrieve the original transaction by its ID
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		return model.Transaction{}, err
	}

	// Create a new inverse transaction with the opposite "drcr" value
	refundTxn := model.Transaction{
		Tag:       "Refund",
		Reference: database.GenerateUUIDWithSuffix("ref"),
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
		return model.Transaction{}, err
	}

	return refundTxn, nil
}
