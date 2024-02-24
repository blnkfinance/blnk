package model

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type Transaction struct {
	ID                     int64                  `json:"-"`
	TransactionID          string                 `json:"id"`
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
	MetaData               map[string]interface{} `json:"meta_data,omitempty"`
	GroupIds               []string               `json:"group_ids"`
}

type Balance struct {
	ID                 int64                  `json:"-"`
	BalanceID          string                 `json:"balance_id"`
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

type AlertCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    int64  `json:"value"`
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

type Ledger struct {
	ID        int64                  `json:"-"`
	LedgerID  string                 `json:"id"`
	Name      string                 `json:"name"`
	CreatedAt time.Time              `json:"created_at"`
	MetaData  map[string]interface{} `json:"meta_data,omitempty"`
}

type LedgerFilter struct {
	ID   int64     `json:"id"`
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type Policy struct {
	ID        int64     `json:"id,omitempty"`
	Name      string    `json:"name,omitempty"`
	Operator  string    `json:"operator,omitempty"`
	Field     string    `json:"field,omitempty"`
	Value     string    `json:"value"`
	Action    string    `json:"action,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Identity struct {
	IdentityID       string                 `json:"identity_id" form:"identity_id"`
	IdentityType     string                 `json:"identity_type" form:"identity_type"` // "individual" or "organization"
	OrganizationName string                 `json:"organization_name" form:"organization_name"`
	Category         string                 `json:"category" form:"category"`
	FirstName        string                 `json:"first_name" form:"first_name"`
	LastName         string                 `json:"last_name" form:"last_name"`
	OtherNames       string                 `json:"other_names" form:"other_names"`
	Gender           string                 `json:"gender" form:"gender"`
	DOB              time.Time              `json:"dob" form:"dob"`
	EmailAddress     string                 `json:"email_address" form:"email_address"`
	PhoneNumber      string                 `json:"phone_number" form:"phone_number"`
	Nationality      string                 `json:"nationality" form:"nationality"`
	Street           string                 `json:"street" form:"street"`
	Country          string                 `json:"country" form:"country"`
	State            string                 `json:"state" form:"state"`
	PostCode         string                 `json:"post_code" form:"postCode"`
	City             string                 `json:"city" form:"city"`
	CreatedAt        time.Time              `json:"created_at" form:"createdAt"`
	MetaData         map[string]interface{} `json:"meta_data" form:"metaData"`
}

type Account struct {
	AccountID  string                 `json:"account_id"`
	Name       string                 `json:"name" form:"name"`
	Number     string                 `json:"number" form:"number"`
	BankName   string                 `json:"bank_name"`
	Currency   string                 `json:"currency"`
	CreatedAt  time.Time              `json:"created_at"`
	BalanceID  string                 `json:"balance_id" `
	IdentityID string                 `json:"identity_id" form:"identity_id"`
	LedgerID   string                 `json:"ledger_id"`
	MetaData   map[string]interface{} `json:"meta_data"`
	Ledger     *Ledger                `json:"ledger"`
	Balance    *Balance               `json:"balance"`
	Identity   *Identity              `json:"identity"`
}

type EventMapper struct {
	MapperID           string            `json:"mapper_id"`
	Name               string            `json:"name"`
	CreatedAt          time.Time         `json:"created_at"`
	MappingInstruction map[string]string `json:"mapping_instruction"`
}

type Event struct {
	MapperID  string                 `json:"mapper_id"`
	Drcr      string                 `json:"drcr"`
	BalanceID string                 `json:"balance_id"`
	Data      map[string]interface{} `json:"data"`
}

func compare(value int64, condition string, compareTo int64) bool {
	switch condition {
	case ">":
		return value > compareTo
	case "<":
		return value < compareTo
	case ">=":
		return value >= compareTo
	case "<=":
		return value <= compareTo
	case "==":
		return value == compareTo
	}
	return false
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

func (balance *Balance) applyMultiplier(transaction *Transaction) {
	if balance.CurrencyMultiplier == 0 {
		balance.CurrencyMultiplier = 1
	}
	transaction.Amount = transaction.Amount * balance.CurrencyMultiplier
}

func (balance *Balance) UpdateBalances(transaction *Transaction, position int) error {
	// Validate transaction
	err := transaction.validate()
	if err != nil {
		return err
	}

	balance.applyMultiplier(transaction)

	//position is first on the array of balances whihc is the source balance
	if position == 0 {
		balance.AddDebit(transaction.Amount)
	} else {
		balance.AddCredit(transaction.Amount)
	}

	balance.ComputeBalance()
	return nil
}

func (transaction *Transaction) validate() error {
	if transaction.Amount <= 0 {
		return errors.New("transaction amount must be positive")
	}
	return nil
}

func (transaction *Transaction) ToJSON() ([]byte, error) {
	return json.Marshal(transaction)
}

func (bm *BalanceMonitor) CheckCondition(b *Balance) bool {
	switch bm.Condition.Field {
	case "debit_balance":
		return compare(b.DebitBalance, bm.Condition.Operator, bm.Condition.Value)
	case "credit_balance":
		return compare(b.CreditBalance, bm.Condition.Operator, bm.Condition.Value)
	case "balance":
		return compare(b.Balance, bm.Condition.Operator, bm.Condition.Value)
	}
	return false
}
