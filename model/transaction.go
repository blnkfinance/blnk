package model

import (
	"encoding/json"
	"time"
)

type Transaction struct {
	ID                     int64                  `json:"-"`
	TransactionID          string                 `json:"id"`
	AllowOverdraft         bool                   `json:"allow_overdraft"`
	Source                 string                 `json:"source"`
	Destination            string                 `json:"destination"`
	Reference              string                 `json:"reference"`
	Amount                 int64                  `json:"amount"`
	Currency               string                 `json:"currency"`
	Description            string                 `json:"description"`
	Status                 string                 `json:"status"`
	CreatedAt              time.Time              `json:"created_at"`
	ScheduledFor           time.Time              `json:"scheduled_for,omitempty"`
	RiskToleranceThreshold float64                `json:"risk_tolerance_threshold"`
	RiskScore              float64                `json:"risk_score"`
	SkipBalanceUpdate      bool                   `json:"-"`
	Hash                   string                 `json:"hash"`
	MetaData               map[string]interface{} `json:"meta_data,omitempty"`
	GroupIds               []string               `json:"group_ids"`
}

func (transaction *Transaction) ToJSON() ([]byte, error) {
	return json.Marshal(transaction)
}
