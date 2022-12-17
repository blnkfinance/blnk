package saifu

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/uptrace/bun"
)

type Service interface {
	CreateLedger(ledger Ledger) (Ledger, error)
	CreateBalance(balance Balance) (Balance, error)
	RecordTransaction(transaction Transaction) (Transaction, error)
	GetLedger(ledgerID string) (Ledger, error)
	GetBalance(balanceID string) (Balance, error)
	GetTransaction(transactionID string) (Transaction, error)
	GetTransactionByRef(reference string) (Transaction, error)
}

type Tools interface {
}

type Filter struct {
	Status string
}

type Transaction struct {
	bun.BaseModel `bun:"table:transactions,alias:t"`

	ID                  string                 `json:"id"`
	Tag                 string                 `json:"tag"`
	Reference           string                 `json:"reference"`
	Amount              int64                  `json:"amount"`
	Currency            string                 `json:"currency"`
	DRCR                string                 `json:"drcr"`
	Status              string                 `json:"status"` //successful, pending, encumbrance
	LedgerID            string                 `json:"ledger_id"`
	BalanceID           string                 `json:"balance_id"`
	CreditBalanceBefore int64                  `json:"credit_balance_before"`
	DebitBalanceBefore  int64                  `json:"debit_balance_before"`
	CreditBalanceAfter  int64                  `json:"credit_balance_after"`
	DebitBalanceAfter   int64                  `json:"debit_balance_after"`
	BalanceBefore       int64                  `json:"balance_before"`
	BalanceAfter        int64                  `json:"balance_after"`
	Created             int64                  `json:"created"`
	MetaData            map[string]interface{} `json:"meta_data"`
}

type TransactionFilter struct {
	ID                       string    `json:"id"`
	Tag                      string    `json:"tag"`
	DRCR                     string    `json:"drcr"`
	AmountRange              int64     `json:"amount_range"`
	CreditBalanceBeforeRange int64     `json:"credit_balance_before_range"`
	DebitBalanceBeforeRange  int64     `json:"debit_balance_before_range"`
	CreditBalanceAfterRange  int64     `json:"credit_balance_after_range"`
	DebitBalanceAfterRange   int64     `json:"debit_balance_after_range"`
	BalanceBeforeRange       int64     `json:"balance_before"`
	BalanceAfterRange        int64     `json:"balance_after"`
	From                     time.Time `json:"from"`
	To                       time.Time `json:"to"`
}

type Balance struct {
	bun.BaseModel `bun:"table:balances,alias:b"`

	ID                 string                 `json:"id"`
	Balance            int64                  `json:"balance"`
	CreditBalance      int64                  `json:"credit_balance"`
	DebitBalance       int64                  `json:"debit_balance"`
	Currency           string                 `json:"currency"`
	CurrencyMultiplier int64                  `json:"currency_multiplier"`
	LedgerID           string                 `json:"ledger_id"`
	Created            int64                  `json:"created"`
	ModificationRef    string                 `json:"modification_ref"`
	MetaData           map[string]interface{} `json:"meta_data"`
}

type BalanceFilter struct {
	ID                 string    `json:"id"`
	BalanceRange       string    `json:"balance_range"`
	CreditBalanceRange string    `json:"credit_balance_range"`
	DebitBalanceRange  string    `json:"debit_balance_range"`
	Currency           string    `json:"currency"`
	LedgerID           string    `json:"ledger_id"`
	From               time.Time `json:"from"`
	To                 time.Time `json:"to"`
}

type Ledger struct {
	bun.BaseModel `bun:"table:ledgers,alias:l"`

	ID       string                 `json:"id"`
	Created  int64                  `json:"created"`
	MetaData map[string]interface{} `json:"meta_data"`
}

type LedgerFilter struct {
	ID   string    `json:"id"`
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func (balance *Balance) computeCreditBalance(amount int64) {
	balance.CreditBalance = balance.CreditBalance + amount
}

func (balance *Balance) computeDebitBalance(amount int64) {
	balance.DebitBalance = balance.DebitBalance + amount
}

func (balance *Balance) computeBalance() {
	balance.Balance = balance.CreditBalance + -balance.DebitBalance
}

func (balance *Balance) attachBalanceBefore(transaction *Transaction) {
	transaction.DebitBalanceBefore = balance.DebitBalance
	transaction.CreditBalanceBefore = balance.CreditBalance
	transaction.BalanceBefore = balance.Balance

}

func (balance *Balance) attachBalanceAfter(transaction *Transaction) {
	transaction.DebitBalanceAfter = balance.DebitBalance
	transaction.CreditBalanceAfter = balance.CreditBalance
	transaction.BalanceAfter = balance.Balance
}

func (balance *Balance) ComputeNewBalances(transaction *Transaction) {
	balance.attachBalanceBefore(transaction)
	drcr := transaction.DRCR
	if drcr == "Credit" {
		balance.computeCreditBalance(transaction.Amount)
	} else {
		balance.computeDebitBalance(transaction.Amount)
	}
	balance.computeBalance()
	balance.attachBalanceAfter(transaction)
}

func (transaction *Transaction) Defaults() {
	transaction.Created = time.Now().UnixNano()
	transaction.ID = fmt.Sprintf("trans_%s", uuid.New().String())
}
