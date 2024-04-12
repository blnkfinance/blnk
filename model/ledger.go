package model

import "time"

type Ledger struct {
	ID        int64                  `json:"-"`
	LedgerID  string                 `json:"ledger_id"`
	Name      string                 `json:"name"`
	MetaData  map[string]interface{} `json:"meta_data"`
	CreatedAt time.Time              `json:"created_at"`
}

type LedgerFilter struct {
	ID   int64     `json:"id"`
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}
