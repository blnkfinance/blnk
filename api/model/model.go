package model

import (
	"errors"
	"time"

	"github.com/jerry-enebeli/blnk/model"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type CreateLedger struct {
	Name     string `json:"name"`
	MetaData struct {
		ProjectOwner string `json:"project owner"`
	} `json:"meta_data"`
}

type CreateIdentity struct {
	IdentityType     string    `json:"identity_type"`
	FirstName        string    `json:"first_name"`
	LastName         string    `json:"last_name"`
	OtherNames       string    `json:"other_names"`
	Gender           string    `json:"gender"`
	Dob              time.Time `json:"dob"`
	EmailAddress     string    `json:"email_address"`
	PhoneNumber      string    `json:"phone_number"`
	Nationality      string    `json:"nationality"`
	OrganizationName string    `json:"organization_name"`
	Category         string    `json:"category"`
	Street           string    `json:"street"`
	Country          string    `json:"country"`
	State            string    `json:"state"`
	PostCode         string    `json:"post_code"`
	City             string    `json:"city"`
	CreatedAt        time.Time `json:"created_at"`
	MetaData         struct {
		Verified  bool   `json:"verified"`
		Reference string `json:"reference"`
	} `json:"meta_data"`
}

type CreateBalance struct {
	LedgerId   string `json:"ledger_id"`
	IdentityId string `json:"identity_id"`
	Currency   string `json:"currency"`
}

type CreateBalanceMonitor struct {
	BalanceId   string           `json:"balance_id"`
	Condition   MonitorCondition `json:"condition"`
	CallBackURL string           `json:"call_back_url"`
}

type MonitorCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    int64  `json:"value"`
}

type CreateAccount struct {
	BankName   string `json:"bank_name"`
	Number     string `json:"number"`
	Currency   string `json:"currency"`
	IdentityId string `json:"identity_id"`
	LedgerId   string `json:"ledger_id"`
	BalanceId  string `json:"balance_id"`
}

type RecordTransaction struct {
	Amount                 float64   `json:"amount"`
	Source                 string    `json:"source"`
	Reference              string    `json:"reference"`
	Drcr                   string    `json:"drcr"`
	Destination            string    `json:"destination"`
	Description            string    `json:"description"`
	Currency               string    `json:"currency"`
	BalanceId              string    `json:"balance_id"`
	RiskToleranceThreshold float64   `json:"risk_tolerance_threshold"`
	ScheduledFor           time.Time `json:"scheduled_for"`
}

type CreateEventMapper struct {
	Name               string            `json:"name"`
	MappingInstruction map[string]string `json:"mapping_instruction"`
}

type CreateEvent struct {
	MapperId  string                 `json:"mapper_id"`
	Drcr      string                 `json:"drcr"`
	BalanceId string                 `json:"balance_id"`
	Data      map[string]interface{} `json:"data"`
}

func (l *CreateLedger) ValidateCreateLedger() error {
	return validation.ValidateStruct(l,
		validation.Field(&l.Name, validation.Required),
	)
}

func (l *CreateLedger) ToLedger() model.Ledger {
	return model.Ledger{Name: l.Name}
}

func (b *CreateBalance) ValidateCreateBalance() error {
	return validation.ValidateStruct(b,
		validation.Field(&b.LedgerId, validation.Required),
		validation.Field(&b.Currency, validation.Required),
	)
}

func (b *CreateBalance) ToBalance() model.Balance {
	return model.Balance{LedgerID: b.LedgerId, IdentityID: b.IdentityId, Currency: b.Currency}
}

func (b *CreateBalanceMonitor) ValidateCreateBalanceMonitor() error {
	return validation.ValidateStruct(b,
		validation.Field(&b.BalanceId, validation.Required),
		validation.Field(&b.Condition, validation.Required, validation.By(func(value interface{}) error {
			// Convert the interface{} to MonitorCondition type
			condition, ok := value.(MonitorCondition)
			if !ok {
				// Handle the case where the value cannot be converted
				return errors.New("invalid condition type")
			}
			// Call the ValidateMonitorCondition method
			return condition.ValidateMonitorCondition()
		})),
	)
}

func (c *MonitorCondition) ValidateMonitorCondition() error {
	return validation.ValidateStruct(c,
		validation.Field(&c.Field, validation.Required),
		validation.Field(&c.Operator, validation.Required),
		validation.Field(&c.Value, validation.Required),
	)
}

func (b *CreateBalanceMonitor) ToBalanceMonitor() model.BalanceMonitor {
	return model.BalanceMonitor{BalanceID: b.BalanceId, Condition: model.AlertCondition{
		Field:    b.Condition.Field,
		Operator: b.Condition.Operator,
		Value:    b.Condition.Value,
	}, CallBackURL: b.CallBackURL}
}

func (a *CreateAccount) ValidateCreateAccount() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.LedgerId, validation.When(a.BalanceId == "", validation.Required.Error("Ledger ID is required when Balance ID is not provided"))),
		validation.Field(&a.IdentityId, validation.When(a.BalanceId == "", validation.Required.Error("Identity ID is required when Balance ID is not provided"))),
		validation.Field(&a.Currency, validation.When(a.BalanceId == "", validation.Required.Error("currency is required when Balance ID is not provided"))),
		validation.Field(&a.LedgerId, validation.By(func(value interface{}) error {
			if a.BalanceId != "" && a.LedgerId != "" {
				return errors.New("either LedgerId or BalanceId must be provided, not both")
			}
			return nil
		})),
		validation.Field(&a.Currency, validation.By(func(value interface{}) error {
			if a.BalanceId != "" && a.Currency != "" {
				return errors.New("either Currency or BalanceId must be provided, not both")
			}
			return nil
		})),
	)
}

func (a *CreateAccount) ToAccount() model.Account {
	return model.Account{BalanceID: a.BalanceId, LedgerID: a.LedgerId, IdentityID: a.IdentityId, Currency: a.Currency, Number: a.Number, BankName: a.BankName}
}

func (t *RecordTransaction) ValidateRecordTransaction() error {
	return validation.ValidateStruct(t,
		validation.Field(&t.Amount, validation.Required),
		validation.Field(&t.Currency, validation.Required),
		validation.Field(&t.Reference, validation.Required),
		validation.Field(&t.Source, validation.Required),
		validation.Field(&t.Destination, validation.Required),
	)
}

func (t *RecordTransaction) ToTransaction() *model.Transaction {
	return &model.Transaction{Currency: t.Currency, Source: t.Source, Description: t.Description, Reference: t.Reference, RiskToleranceThreshold: t.RiskToleranceThreshold, ScheduledFor: t.ScheduledFor, Destination: t.Destination, Amount: int64(t.Amount)}
}

func (t *CreateEventMapper) ValidateCreateEventMapper() error {
	return validation.ValidateStruct(t,
		validation.Field(&t.Name, validation.Required),
		validation.Field(&t.MappingInstruction, validation.Map(validation.Key("amount", validation.Required), validation.Key("currency", validation.Required), validation.Key("reference", validation.Required))),
	)
}

func (t *CreateEventMapper) ToEventMapper() model.EventMapper {
	return model.EventMapper{Name: t.Name, MappingInstruction: t.MappingInstruction}
}

func (e *CreateEvent) ValidateCreateEvent() error {
	return validation.ValidateStruct(e,
		validation.Field(&e.MapperId, validation.Required),
		validation.Field(&e.BalanceId, validation.Required),
		validation.Field(&e.Data, validation.Required),
		validation.Field(&e.Drcr, validation.Required, validation.In("Credit", "Debit")),
	)
}

func (e *CreateEvent) ToEvent() model.Event {
	return model.Event{MapperID: e.MapperId, Drcr: e.Drcr, BalanceID: e.BalanceId, Data: e.Data}
}
