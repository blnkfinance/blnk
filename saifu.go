package saifu

import "github.com/uptrace/bun"

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

	ID        string                 `json:"id"`
	Tag       string                 `json:"tag"`
	Reference string                 `json:"reference"`
	Amount    int64                  `json:"amount"`
	Currency  string                 `json:"currency"`
	DRCR      string                 `json:"drcr"`
	Status    string                 `json:"status"` //successful, pending, encumbrance
	LedgerID  string                 `json:"ledger_id"`
	BalanceID string                 `json:"balance_id"`
	Created   int64                  `json:"created"`
	MetaData  map[string]interface{} `json:"meta_data"`
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
type BalanceUpdate struct {
	bun.BaseModel `bun:"table:balances,alias:b"`

	Balance         int64  `json:"balance"`
	CreditBalance   int64  `json:"credit_balance"`
	DebitBalance    int64  `json:"debit_balance"`
	ModificationRef string `json:"modification_ref"`
}

type Ledger struct {
	bun.BaseModel `bun:"table:ledgers,alias:l"`

	ID       string                 `json:"id"`
	Created  int64                  `json:"created"`
	MetaData map[string]interface{} `json:"meta_data"`
}

func (balance *BalanceUpdate) computeCreditBalance(amount int64) {
	balance.CreditBalance = balance.CreditBalance + amount
}

func (balance *BalanceUpdate) computeDebitBalance(amount int64) {
	balance.DebitBalance = balance.DebitBalance + amount
}

func (balance *BalanceUpdate) computeBalance() {
	balance.Balance = balance.CreditBalance + -balance.DebitBalance
}

func (balance *BalanceUpdate) ComputeNewBalances(drcr string, amount int64) {
	if drcr == "Credit" {
		balance.computeCreditBalance(amount)
	} else {
		balance.computeDebitBalance(amount)
	}

	balance.computeBalance()
}
