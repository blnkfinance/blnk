package pkg

import (
	"database/sql"
	"errors"

	"github.com/jerry-enebeli/blnk"
)

func (l Blnk) validateBlnCurrency(transaction *blnk.Transaction) (blnk.Balance, error) {
	balance, err := l.datasource.GetBalanceByID(transaction.BalanceID)
	if err != nil {
		return *balance, err
	}
	if balance.Currency != transaction.Currency {
		//todo write flagged transactions table
		return *balance, errors.New("currency mismatch")
	}
	return *balance, nil
}

func (l Blnk) validateTxnAndReturnBalance(transaction blnk.Transaction) (blnk.Balance, error) {
	txn, err := l.GetTransactionByRef(transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return blnk.Balance{}, err
	}

	if errors.Is(err, nil) && txn.ID != 0 {
		return blnk.Balance{}, errors.New("reference already used")
	}

	balance, err := l.validateBlnCurrency(&transaction)
	if err != nil {
		return blnk.Balance{}, err
	}
	_, err = l.datasource.GetLedgerByID(balance.LedgerID)
	if err != nil {
		return blnk.Balance{}, err
	}

	return balance, nil
}

func (l Blnk) recordTransaction(LedgerID int64, transaction blnk.Transaction) error {
	transaction.LedgerID = LedgerID
	transaction, err := l.datasource.RecordTransaction(transaction)
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
	balance.UpdateBalances(&transaction)
	balance.ModificationRef = transaction.ID //set the last balance modifier to the transaction id
	err = l.recordTransaction(balance.LedgerID, transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}
	err = l.datasource.UpdateBalance(&balance)
	if err != nil {
		return blnk.Transaction{}, err
	}
	return transaction, nil
}

func (l Blnk) GetTransaction(TransactionID string) (blnk.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
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
