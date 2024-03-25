package model

import (
	"sync"
	"time"
)

type Balance struct {
	ID                 int64                  `json:"-"`
	BalanceID          string                 `json:"balance_id"`
	Indicator          string                 `json:"indicator"`
	Balance            int64                  `json:"balance"`
	CreditBalance      int64                  `json:"credit_balance"`
	DebitBalance       int64                  `json:"debit_balance"`
	Currency           string                 `json:"currency"`
	CurrencyMultiplier int64                  `json:"currency_multiplier"`
	LedgerID           string                 `json:"ledger_id"`
	IdentityID         string                 `json:"identity_id"`
	Identity           *Identity              `json:"identity,omitempty"`
	Ledger             *Ledger                `json:"ledger,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	MetaData           map[string]interface{} `json:"meta_data"`
}

type BalanceMonitor struct {
	MonitorID   string         `json:"monitor_id"`
	BalanceID   string         `json:"balance_id"`
	Condition   AlertCondition `json:"condition"`
	Description string         `json:"description"`
	CallBackURL string         `json:"call_back_url"`
	CreatedAt   time.Time      `json:"created_at"`
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

type BalanceTracker struct {
	Balances    map[string]*Balance
	Frequencies map[string]int
	Mutex       sync.Mutex
}
type AlertCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    int64  `json:"value"`
}
