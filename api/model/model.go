/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package model

import (
	"errors"
	"time"

	"github.com/sirupsen/logrus"

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

func validateDateFormat(format, value string) error {
	_, err := time.Parse(format, value)
	if err != nil {
		return errors.New("please format the scheduled date as 'YYYY-MM-DDTHH:MM:SS+00:00' (e.g., 2024-04-22T15:28:03+00:00)")
	}
	return nil
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
		validation.Field(&c.Field, validation.Required, validation.In("debit_balance", "credit_balance", "balance", "inflight_debit_balance", "inflight_credit_balance", "inflight_balance")),
		validation.Field(&c.Operator, validation.Required),
		validation.Field(&c.Precision, validation.Required),
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
		validation.Field(&t.ScheduledFor, validation.When(t.ScheduledFor != "", validation.By(func(value interface{}) error {
			dateStr, ok := value.(string)
			if !ok {
				return errors.New("invalid type for scheduled date")
			}
			return validateDateFormat("2006-01-02T15:04:05Z07:00", dateStr)
		})),
		),
		validation.Field(&t.InflightExpiryDate, validation.When(t.InflightExpiryDate != "", validation.By(func(value interface{}) error {
			dateStr, ok := value.(string)
			if !ok {
				return errors.New("invalid type for scheduled date")
			}
			return validateDateFormat("2006-01-02T15:04:05Z07:00", dateStr)
		})),
		),
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
		Field:     b.Condition.Field,
		Operator:  b.Condition.Operator,
		Value:     b.Condition.Value,
		Precision: b.Condition.Precision,
	}, CallBackURL: b.CallBackURL}
}

func (a *CreateAccount) ToAccount() model.Account {
	return model.Account{BalanceID: a.BalanceId, LedgerID: a.LedgerId, IdentityID: a.IdentityId, Currency: a.Currency, Number: a.Number, BankName: a.BankName, MetaData: a.MetaData}
}

func (t *RecordTransaction) ToTransaction() *model.Transaction {
	var scheduledFor time.Time
	var inflightExpiryDate time.Time

	if t.ScheduledFor != "" {
		scheduledTime, err := time.Parse("2006-01-02T15:04:05Z07:00", t.ScheduledFor)
		if err != nil {
			logrus.Error(err)
		}

		scheduledFor = scheduledTime

	}

	if t.InflightExpiryDate != "" {
		inflightExpiry, err := time.Parse("2006-01-02T15:04:05Z07:00", t.InflightExpiryDate)
		if err != nil {
			logrus.Error(err)
		}

		inflightExpiryDate = inflightExpiry

	}

	return &model.Transaction{Currency: t.Currency, Source: t.Source, Description: t.Description, Reference: t.Reference, ScheduledFor: scheduledFor, Destination: t.Destination, Amount: t.Amount, AllowOverdraft: t.AllowOverDraft, MetaData: t.MetaData, Sources: t.Sources, Destinations: t.Destinations, Inflight: t.Inflight, Precision: t.Precision, InflightExpiryDate: inflightExpiryDate, Rate: t.Rate}
}
