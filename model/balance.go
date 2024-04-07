package model

import (
	"sync"
	"time"
)

type Balance struct {
	ID                    int64                  `json:"-"`
	Balance               int64                  `json:"balance"`
	InflightBalance       int64                  `json:"inflight_balance"`
	CreditBalance         int64                  `json:"credit_balance"`
	InflightCreditBalance int64                  `json:"inflight_credit_balance"`
	DebitBalance          int64                  `json:"debit_balance"`
	InflightDebitBalance  int64                  `json:"inflight_debit_balance"`
	CurrencyMultiplier    int64                  `json:"preceision"`
	Version               int64                  `json:"version"`
	LedgerID              string                 `json:"ledger_id"`
	IdentityID            string                 `json:"identity_id"`
	BalanceID             string                 `json:"balance_id"`
	Indicator             string                 `json:"indicator"`
	Currency              string                 `json:"currency"`
	Identity              *Identity              `json:"identity,omitempty"`
	Ledger                *Ledger                `json:"ledger,omitempty"`
	CreatedAt             time.Time              `json:"created_at"`
	InflighExpiresAt      time.Time              `json:"inflight_expires_at"`
	MetaData              map[string]interface{} `json:"meta_data"`
}

type BalanceMonitor struct {
	MonitorID   string         `json:"monitor_id"`
	BalanceID   string         `json:"balance_id"`
	Description string         `json:"description"`
	CallBackURL string         `json:"call_back_url"`
	CreatedAt   time.Time      `json:"created_at"`
	Condition   AlertCondition `json:"condition"`
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
	Value    int64  `json:"value"`
	Field    string `json:"field"`
	Operator string `json:"operator"`
}
