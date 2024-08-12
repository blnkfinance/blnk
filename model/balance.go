package model

import (
	"math/big"
	"sync"
	"time"
)

type Balance struct {
	ID                    int64                  `json:"-"`
	Balance               *big.Int               `json:"balance"`
	Version               int64                  `json:"version"`
	InflightBalance       *big.Int               `json:"inflight_balance"`
	CreditBalance         *big.Int               `json:"credit_balance"`
	InflightCreditBalance *big.Int               `json:"inflight_credit_balance"`
	DebitBalance          *big.Int               `json:"debit_balance"`
	InflightDebitBalance  *big.Int               `json:"inflight_debit_balance"`
	CurrencyMultiplier    float64                `json:"precision"`
	LedgerID              string                 `json:"ledger_id"`
	IdentityID            string                 `json:"identity_id"`
	BalanceID             string                 `json:"balance_id"`
	Indicator             string                 `json:"indicator"`
	Currency              string                 `json:"currency"`
	Identity              *Identity              `json:"identity,omitempty"`
	Ledger                *Ledger                `json:"ledger,omitempty"`
	CreatedAt             time.Time              `json:"created_at"`
	InflightExpiresAt     time.Time              `json:"inflight_expires_at"`
	MetaData              map[string]interface{} `json:"meta_data"`
}

type BalanceMonitor struct {
	MonitorID   string         `json:"monitor_id"`
	BalanceID   string         `json:"balance_id"`
	Description string         `json:"description,omitempty"`
	CallBackURL string         `json:"-"`
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
	Value        float64  `json:"value"`
	Precision    float64  `json:"precision"`
	PreciseValue *big.Int `json:"precise_value"`
	Field        string   `json:"field"`
	Operator     string   `json:"operator"`
}
