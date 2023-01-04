package ledger

import (
	"errors"
	"log"
	"time"

	blnk "github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/datasources"
)

type ledger struct {
	datasource datasources.DataSource
	config     *config.Configuration
}

func NewLedger() *ledger {
	configuration, err := config.Fetch()
	if err != nil {
		panic(err)
		return nil
	}
	dataSource := datasources.NewDataSource(configuration)
	return &ledger{datasource: dataSource, config: configuration}
}

func (l ledger) writeToDisk(transaction blnk.Transaction) {
	//write to localfile
}

func (l ledger) validateBalance(transaction *blnk.Transaction) (blnk.Balance, error) {
	balance, err := l.datasource.GetBalance(transaction.BalanceID)
	if err != nil {
		return blnk.Balance{}, err
	}
	if balance.Currency != transaction.Currency {
		//todo write flagged transactions table
		return balance, errors.New("currency mismatch")
	}
	return balance, nil
}

func (l ledger) CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error) {
	ledger.Created = time.Now().UnixNano()
	currentLedger, err := l.datasource.GetLedger(ledger.ID)
	log.Println(currentLedger, err)
	if currentLedger.ID != "" {
		return blnk.Ledger{}, errors.New("ledger id already in use")
	}
	return l.datasource.CreateLedger(ledger)
}

func (l ledger) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	balance.Created = time.Now().UnixNano()
	//todo validateBalanceBefore creation. Prevent Duplicate
	return l.datasource.CreateBalance(balance)
}

func (l ledger) validateTransaction(transaction blnk.Transaction) (blnk.Balance, error) {
	transactionData, err := l.GetTransactionByRef(transaction.Reference)
	if transactionData.ID != "" {
		return blnk.Balance{}, errors.New("reference already used")
	} //ensures reference does not exist
	balance, err := l.validateBalance(&transaction)
	if err != nil {
		return blnk.Balance{}, err
	} //ensures balance exist
	_, err = l.datasource.GetLedger(balance.LedgerID)
	if err != nil {
		return blnk.Balance{}, err
	} //ensure ledger exist

	balance.ComputeNewBalances(&transaction)
	balance.ModificationRef = transaction.ID //set the last balance modifier to the transaction id

	return balance, nil
}

func (l ledger) recordTransaction(LedgerID string, transaction blnk.Transaction) error {
	transaction.LedgerID = LedgerID
	transaction, err := l.datasource.RecordTransaction(transaction)
	if err != nil {
		go l.writeToDisk(transaction)
		return err
	}
	return nil
}

func (l ledger) RecordTransaction(transaction blnk.Transaction) (blnk.Transaction, error) {
	transaction.Defaults()
	balance, err := l.validateTransaction(transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}
	err = l.recordTransaction(balance.LedgerID, transaction)
	if err != nil {
		return blnk.Transaction{}, err
	}
	_, err = l.datasource.UpdateBalance(transaction.BalanceID, balance)
	if err != nil {
		return blnk.Transaction{}, err
	}
	return transaction, nil
}

func (l ledger) GetLedger(LedgerID string) (blnk.Ledger, error) {
	return l.datasource.GetLedger(LedgerID)
}

func (l ledger) GetBalance(BalanceID string) (blnk.Balance, error) {
	return l.datasource.GetBalance(BalanceID)
}

func (l ledger) GetTransaction(TransactionID string) (blnk.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l ledger) GetTransactionByRef(reference string) (blnk.Transaction, error) {
	return l.datasource.GetTransactionByRef(reference)
}
