package ledger

import "time"

type Filter struct {
	Status string
}

type Transaction struct {
	ID        string    `json:"id"`
	Tag       string    `json:"tag"`
	Reference string    `json:"reference"`
	Amount    int64     `json:"amount"`
	Type      string    `json:"type"`
	Status    string    `json:"status"` //successful, pending, encumbrance
	LedgerID  string    `json:"ledger_id"`
	Created   time.Time `json:"created"`
	Mutated   time.Time `json:"mutated"`
}

type Balances struct {
	ID            string    `json:"id"`
	Balance       int64     `json:"balance"`
	CreditBalance int64     `json:"credit_balance"`
	DebitBalance  int64     `json:"debit_balance"`
	Currency      string    `json:"currency"`
	Multiplier    int64     `json:"multiplier"`
	LedgerID      string    `json:"ledger_id"`
	Created       time.Time `json:"created"`
	Mutated       time.Time `json:"mutated"`
}

type Ledger struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	DeletedAt time.Time `json:"deleted_at"`
	Created   time.Time `json:"created"`
	Mutated   time.Time `json:"mutated"`
}
