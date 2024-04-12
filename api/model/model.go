package model

import (
	"errors"

	"github.com/jerry-enebeli/blnk/model"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func sourceOrSourcesValidation(t *RecordTransaction) validation.RuleFunc {
	return func(value interface{}) error {
		if (t.Source == "" && len(t.Sources) == 0) || (t.Source != "" && len(t.Sources) > 0) {
			return errors.New("either source or sources is required, not both")
		}
		return nil
	}
}

func destinationOrDestinationsValidation(t *RecordTransaction) validation.RuleFunc {
	return func(value interface{}) error {
		if (t.Destination == "" && len(t.Destinations) == 0) || (t.Destination != "" && len(t.Destinations) > 0) {
			return errors.New("either destination or destinations is required, not both")
		}
		return nil
	}
}

func (l *CreateLedger) ValidateCreateLedger() error {
	return validation.ValidateStruct(l,
		validation.Field(&l.Name, validation.Required),
	)
}

func (b *CreateBalance) ValidateCreateBalance() error {
	return validation.ValidateStruct(b,
		validation.Field(&b.LedgerId, validation.Required),
		validation.Field(&b.Currency, validation.Required),
	)
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

func (t *RecordTransaction) ValidateRecordTransaction() error {
	return validation.ValidateStruct(t,
		validation.Field(&t.Amount, validation.Required),
		validation.Field(&t.Currency, validation.Required),
		validation.Field(&t.Reference, validation.Required),
		validation.Field(&t.Description, validation.Required),
		validation.Field(&t.Source, validation.By(sourceOrSourcesValidation(t))),
		validation.Field(&t.Destination, validation.By(destinationOrDestinationsValidation(t))),
	)
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

func (l *CreateLedger) ToLedger() model.Ledger {
	return model.Ledger{Name: l.Name, MetaData: l.MetaData}
}

func (b *CreateBalance) ToBalance() model.Balance {
	return model.Balance{LedgerID: b.LedgerId, IdentityID: b.IdentityId, Currency: b.Currency, MetaData: b.MetaData, CurrencyMultiplier: b.Precision}
}

func (b *CreateBalanceMonitor) ToBalanceMonitor() model.BalanceMonitor {
	return model.BalanceMonitor{BalanceID: b.BalanceId, Condition: model.AlertCondition{
		Field:    b.Condition.Field,
		Operator: b.Condition.Operator,
		Value:    b.Condition.Value,
	}, CallBackURL: b.CallBackURL}
}

func (a *CreateAccount) ToAccount() model.Account {
	return model.Account{BalanceID: a.BalanceId, LedgerID: a.LedgerId, IdentityID: a.IdentityId, Currency: a.Currency, Number: a.Number, BankName: a.BankName, MetaData: a.MetaData}
}

func (t *RecordTransaction) ToTransaction() *model.Transaction {
	return &model.Transaction{Currency: t.Currency, Source: t.Source, Description: t.Description, Reference: t.Reference, ScheduledFor: t.ScheduledFor, Destination: t.Destination, Amount: t.Amount, AllowOverdraft: t.AllowOverDraft, MetaData: t.MetaData, Sources: t.Sources, Destinations: t.Destinations, Inflight: t.Inflight, Precision: t.Precision}
}
