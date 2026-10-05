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
	"fmt"
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
	ConditionState bool `json:"condition_state"`
}

// TriggerMode is the read side of NormalizeTrigger: it never fails, because a
// stored row or a cache entry written before the field existed must still
// resolve to something, and the default is the safe reading.
func (bm *BalanceMonitor) TriggerMode() string {
	trigger, err := NormalizeTrigger(bm.Trigger)
	if err != nil {
		return TriggerEdge
	}
	return trigger
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
}
