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
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"
)

// jsonFieldSpecified is true when the key is present and not JSON null.
// Null is the same as omit: optional SDK fields often serialize as null.
func jsonFieldSpecified(raw map[string]json.RawMessage, key string) bool {
	v, ok := raw[key]
	if !ok {
		return false
	}
	return !bytes.Equal(bytes.TrimSpace(v), []byte("null"))
}

type Balance struct {
	ID                    int64                  `json:"-"`
	Balance               *big.Int               `json:"balance"`
	Version               int64                  `json:"version"`
	InflightBalance       *big.Int               `json:"inflight_balance"`
	CreditBalance         *big.Int               `json:"credit_balance"`
	InflightCreditBalance *big.Int               `json:"inflight_credit_balance"`
	DebitBalance          *big.Int               `json:"debit_balance"`
	InflightDebitBalance  *big.Int               `json:"inflight_debit_balance"`
	QueuedDebitBalance    *big.Int               `json:"queued_debit_balance,omitempty"`
	QueuedCreditBalance   *big.Int               `json:"queued_credit_balance,omitempty"`
	LedgerID              string                 `json:"ledger_id"`
	IdentityID            string                 `json:"identity_id"`
	BalanceID             string                 `json:"balance_id"`
	Indicator             string                 `json:"indicator,omitempty"`
	Currency              string                 `json:"currency"`
	Identity              *Identity              `json:"identity,omitempty"`
	Ledger                *Ledger                `json:"ledger,omitempty"`
	CreatedAt             time.Time              `json:"created_at"`
	InflightExpiresAt     time.Time              `json:"inflight_expires_at"`
	MetaData              map[string]interface{} `json:"meta_data"`
	TrackFundLineage      bool                   `json:"track_fund_lineage"`
	AllocationStrategy    string                 `json:"allocation_strategy,omitempty"`
}

// Trigger modes for a BalanceMonitor.
//
// TriggerEdge fires once when the condition goes from false to true and stays
// silent until it has evaluated false again. TriggerLevel fires on every
// committed balance update while the condition holds.
const (
	TriggerEdge  = "edge"
	TriggerLevel = "level"
)

// NormalizeTrigger resolves a caller-supplied trigger mode, filling in the
// default for an unset one and rejecting anything else. It is the only place
// that knows what the default is, so the API, the datasource and the read path
// cannot drift apart on it.
func NormalizeTrigger(trigger string) (string, error) {
	switch trigger {
	case "":
		return TriggerEdge, nil
	case TriggerEdge, TriggerLevel:
		return trigger, nil
	default:
		return "", fmt.Errorf("trigger must be either %q or %q", TriggerEdge, TriggerLevel)
	}
}

type BalanceMonitor struct {
	MonitorID   string         `json:"monitor_id"`
	BalanceID   string         `json:"balance_id"`
	Description string         `json:"description,omitempty"`
	CallBackURL string         `json:"-"`
	CreatedAt   time.Time      `json:"created_at"`
	Condition   AlertCondition `json:"condition"`
	Trigger     string         `json:"trigger,omitempty"`
	// ConditionState is the last observed truth value of the condition. It is
	// owned by the evaluation path and is never authoritative on a cached copy;
	// read it from a TransitionMonitorState result, not from here.
	ConditionState *bool `json:"condition_state,omitempty"`
	// descriptionSpecified is true when JSON included "description", including
	// an explicit empty string that means "clear the name".
	descriptionSpecified bool
}

// UnmarshalJSON records whether the caller sent "description", so an update
// can keep the stored name when the key is missing or null, and still clear
// it when the body sends "".
func (bm *BalanceMonitor) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	type plain BalanceMonitor
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*bm = BalanceMonitor(decoded)
	bm.descriptionSpecified = jsonFieldSpecified(raw, "description")
	return nil
}

// DescriptionSpecified reports whether JSON included "description".
func (bm BalanceMonitor) DescriptionSpecified() bool {
	return bm.descriptionSpecified
}

// SetDescription marks the name as intentionally set, including to empty.
func (bm *BalanceMonitor) SetDescription(description string) {
	bm.Description = description
	bm.descriptionSpecified = true
}

// TriggerMode is the read side of NormalizeTrigger. A monitor with no trigger
// predates the field, in a cache entry written before the upgrade, and those
// monitors are level: reading them as edge would drop their repeat alerts
// until the entry expired.
func (bm *BalanceMonitor) TriggerMode() string {
	if bm.Trigger == TriggerEdge {
		return TriggerEdge
	}
	return TriggerLevel
}

type LineageMapping struct {
	ID                 int64     `json:"id"`
	BalanceID          string    `json:"balance_id"`
	Provider           string    `json:"provider"`
	ShadowBalanceID    string    `json:"shadow_balance_id"`
	AggregateBalanceID string    `json:"aggregate_balance_id"`
	IdentityID         string    `json:"identity_id"`
	CreatedAt          time.Time `json:"created_at"`
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
	// valueSpecified is true when JSON included "value". Zero is a valid
	// threshold, so an omitted value cannot be told from an explicit 0 by the
	// number alone.
	valueSpecified bool
}

// UnmarshalJSON records whether the caller sent "value", so an update can keep
// the stored line when the key is missing or null and still accept an explicit 0.
func (c *AlertCondition) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	type plain AlertCondition
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = AlertCondition(decoded)
	c.valueSpecified = jsonFieldSpecified(raw, "value")
	return nil
}

// ValueSpecified reports whether JSON included "value" or SetValue was used.
// Go-constructed conditions that never marked the value return false.
func (c AlertCondition) ValueSpecified() bool {
	return c.valueSpecified
}

// SetValue marks the threshold as intentionally set, including to 0, so
// UpdateMonitor does not treat a programmatic zero as an omitted field.
func (c *AlertCondition) SetValue(value float64) {
	c.Value = value
	c.valueSpecified = true
}

// Validate reports whether field and operator are ones the ledger can evaluate.
// The table's check constraint stores the same set; rejecting here keeps a PUT
// from turning a bad operator into a 500.
func (c AlertCondition) Validate() error {
	switch c.Field {
	case "debit_balance", "credit_balance", "balance", "inflight_debit_balance", "inflight_credit_balance", "inflight_balance":
	default:
		return fmt.Errorf("field must be one of debit_balance, credit_balance, balance, inflight_debit_balance, inflight_credit_balance, inflight_balance")
	}
	switch c.Operator {
	case ">", "<", ">=", "<=", "=", "!=":
	default:
		return fmt.Errorf("operator must be one of >, <, >=, <=, =, !=")
	}
	return nil
}
