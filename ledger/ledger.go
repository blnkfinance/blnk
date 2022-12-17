package ledger

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/jerry-enebeli/saifu"
	"github.com/jerry-enebeli/saifu/config"
	"github.com/jerry-enebeli/saifu/datasources"
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

func (l ledger) verifyTransactionRef(reference string) error {
	return nil
}

func (l ledger) writeToDisk(transaction saifu.Transaction) {
	//write to localfile
}

func (l ledger) validateBalance(transaction *saifu.Transaction) (saifu.Balance, error) {

	balance, err := l.datasource.GetBalance(transaction.BalanceID)
	if err != nil {
		return saifu.Balance{}, err
	}

	if balance.Currency != transaction.Currency {
		//todo write flagged transactions table
		return balance, errors.New("currency mismatch")
	}

	return balance, nil
}

func (l ledger) CreateLedger(ledger saifu.Ledger) (saifu.Ledger, error) {
	ledger.Created = time.Now().UnixNano()

	currentLedger, err := l.datasource.GetLedger(ledger.ID)
	log.Println(currentLedger, err)
	if currentLedger.ID != "" {
		return saifu.Ledger{}, errors.New("ledger id already in use")
	}

	return l.datasource.CreateLedger(ledger)
}

func (l ledger) CreateBalance(balance saifu.Balance) (saifu.Balance, error) {
	balance.Created = time.Now().UnixNano()

	//todo validateBalanceBefore creation. Prevent Duplicate

	return l.datasource.CreateBalance(balance)
}

func (l ledger) RecordTransaction(transaction saifu.Transaction) (saifu.Transaction, error) {
	//TODO Roll back operations, create transaction, update balance. if update balance fails roll back create transaction
	transaction.Created = time.Now().UnixNano()
	transaction.ID = fmt.Sprintf("trans_%s", uuid.New().String())

	transactionData, err := l.GetTransactionByRef(transaction.Reference)
	if transactionData.ID != "" {
		return saifu.Transaction{}, errors.New("reference already used")
	}

	balance, err := l.validateBalance(&transaction)
	if err != nil {
		log.Println("validate balance error", err)
		return saifu.Transaction{}, err
	}

	_, err = l.datasource.GetLedger(balance.LedgerID)
	if err != nil {
		log.Println("get ledger error", err)
		return saifu.Transaction{}, err
	}

	transaction.LedgerID = balance.LedgerID
	transaction, err = l.datasource.RecordTransaction(transaction)
	if err != nil {
		go l.writeToDisk(transaction)
		log.Println("record transaction error", err)
		return saifu.Transaction{}, err
	}

	balanceUpdate := saifu.BalanceUpdate{ModificationRef: transaction.ID, DebitBalance: balance.DebitBalance, CreditBalance: balance.CreditBalance, Balance: balance.Balance}
	balanceUpdate.ComputeNewBalances(transaction.DRCR, transaction.Amount)

	_, err = l.datasource.UpdateBalance(transaction.BalanceID, balanceUpdate)
	if err != nil {
		log.Println("update balance error", err)
	}
	return transaction, nil
}

func (l ledger) GetLedger(LedgerID string) (saifu.Ledger, error) {
	return l.datasource.GetLedger(LedgerID)
}

func (l ledger) GetBalance(BalanceID string) (saifu.Balance, error) {
	return l.datasource.GetBalance(BalanceID)
}

func (l ledger) GetTransaction(TransactionID string) (saifu.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l ledger) GetTransactionByRef(reference string) (saifu.Transaction, error) {
	return l.datasource.GetTransactionByRef(reference)
}
