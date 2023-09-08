package blnk

import (
	"errors"
	"time"
)

type Filter struct {
	Status string
}

type Transaction struct {
	ID                     int64                  `json:"id"`
	Tag                    string                 `json:"tag"`
	Reference              string                 `json:"reference"`
	Amount                 int64                  `json:"amount"`
	Currency               string                 `json:"currency"`
	DRCR                   string                 `json:"drcr"`
	Status                 string                 `json:"status"`
	LedgerID               int64                  `json:"ledger_id"`
	BalanceID              int64                  `json:"balance_id"`
	CreditBalanceBefore    int64                  `json:"credit_balance_before"`
	DebitBalanceBefore     int64                  `json:"debit_balance_before"`
	CreditBalanceAfter     int64                  `json:"credit_balance_after"`
	DebitBalanceAfter      int64                  `json:"debit_balance_after"`
	BalanceBefore          int64                  `json:"balance_before"`
	BalanceAfter           int64                  `json:"balance_after"`
	Created                int64                  `json:"created"`
	ApplyBalanceMultiplier float64                `json:"apply_balance_multiplier"`
	MetaData               map[string]interface{} `json:"meta_data,omitempty"`
}

type TransactionFilter struct {
	ID                       int64     `json:"id"`
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
	ID                 int64                  `json:"id"`
	Balance            int64                  `json:"balance"`
	CreditBalance      int64                  `json:"credit_balance"`
	DebitBalance       int64                  `json:"debit_balance"`
	Currency           string                 `json:"currency"`
	CurrencyMultiplier int64                  `json:"currency_multiplier"`
	LedgerID           int64                  `json:"ledger_id"`
	Created            time.Time              `json:"created"`
	ModificationRef    int64                  `json:"modification_ref"`
	MetaData           map[string]interface{} `json:"meta_data"`
}

type BalanceFilter struct {
	ID                 int64     `json:"id"`
	BalanceRange       string    `json:"balance_range"`
	CreditBalanceRange string    `json:"credit_balance_range"`
	DebitBalanceRange  string    `json:"debit_balance_range"`
	Currency           string    `json:"currency"`
	LedgerID           string    `json:"ledger_id"`
	From               time.Time `json:"from"`
	To                 time.Time `json:"to"`
}

type Ledger struct {
	ID       int64                  `json:"id,omitempty"`
	Created  time.Time              `json:"created,omitempty"`
	MetaData map[string]interface{} `json:"meta_data,omitempty"`
}

type LedgerFilter struct {
	ID   int64     `json:"id"`
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func (balance *Balance) AddCredit(amount int64) {
	balance.CreditBalance += amount
}

func (balance *Balance) AddDebit(amount int64) {
	balance.DebitBalance += amount
}

func (balance *Balance) ComputeBalance() {
	balance.Balance = balance.CreditBalance - balance.DebitBalance
}

func (balance *Balance) AttachBalanceBefore(transaction *Transaction) {
	transaction.DebitBalanceBefore = balance.DebitBalance
	transaction.CreditBalanceBefore = balance.CreditBalance
	transaction.BalanceBefore = balance.Balance
}

func (balance *Balance) AttachBalanceAfter(transaction *Transaction) {
	transaction.DebitBalanceAfter = balance.DebitBalance
	transaction.CreditBalanceAfter = balance.CreditBalance
	transaction.BalanceAfter = balance.Balance
}

func (balance *Balance) UpdateBalances(transaction *Transaction) error {
	// Validate transaction
	err := transaction.validate()
	if err != nil {
		return err
	}
	balance.AttachBalanceBefore(transaction)
	if transaction.DRCR == "Credit" {
		balance.AddCredit(transaction.Amount)
	} else {
		balance.AddDebit(transaction.Amount)
	}
	balance.ComputeBalance()

	balance.AttachBalanceAfter(transaction)
	return nil
}

func (transaction *Transaction) validate() error {
	if transaction.Amount <= 0 {
		return errors.New("transaction amount must be positive")
	}
	if transaction.DRCR != "Credit" && transaction.DRCR != "Debit" {
		return errors.New("transaction DRCR must be 'Credit' or 'Debit'")
	}
	return nil
}
