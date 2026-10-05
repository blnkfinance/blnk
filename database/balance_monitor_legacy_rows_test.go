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

package database

import (
	"math/big"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

func legacyTestBalance(t *testing.T, ds Datasource) model.Balance {
	t.Helper()
	ledger, err := ds.CreateLedger(model.Ledger{Name: gofakeit.UUID()})
	require.NoError(t, err)
	balance, err := ds.CreateBalance(model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	return balance
}

// precision and precise_value were added as nullable columns, so every monitor
// created before that migration carries NULLs. Scanning them into plain Go
// numbers failed the whole read, and because checkBalanceMonitors gives up on
// that error, one such row silenced every monitor on its balance.
func TestGetMonitors_ToleratesNullPrecision_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	balance := legacyTestBalance(t, ds)

	legacyID := model.GenerateUUIDWithSuffix("mon")
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, description, call_back_url, created_at)
		VALUES ($1, $2, 'balance', '<', 100, 'written before the precision migration', '', NOW())
	`, legacyID, balance.BalanceID)
	require.NoError(t, err)

	one, err := ds.GetMonitorByID(legacyID)
	require.NoError(t, err, "a legacy row must still be readable")
	assert.Equal(t, float64(0), one.Condition.Precision)
	assert.Equal(t, big.NewInt(0), one.Condition.PreciseValue)

	monitors, err := ds.GetBalanceMonitors(balance.BalanceID)
	require.NoError(t, err, "one legacy row must not take down the balance's whole monitor list")
	require.Len(t, monitors, 1)
	assert.Equal(t, legacyID, monitors[0].MonitorID)
	assert.NotNil(t, monitors[0].Condition.PreciseValue, "a nil threshold would panic the comparison")

	all, err := ds.GetAllMonitors()
	require.NoError(t, err)
	assert.NotEmpty(t, all)
}

// GetAllMonitors omitted precision and precise_value, so the list endpoint
// reported a different threshold from the single-monitor endpoint.
func TestGetAllMonitors_CarriesPrecision_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	balance := legacyTestBalance(t, ds)

	created, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID: balance.BalanceID,
		Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 100, Precision: 100, PreciseValue: big.NewInt(10000)},
	})
	require.NoError(t, err)

	one, err := ds.GetMonitorByID(created.MonitorID)
	require.NoError(t, err)

	all, err := ds.GetAllMonitors()
	require.NoError(t, err)

	var listed *model.BalanceMonitor
	for i := range all {
		if all[i].MonitorID == created.MonitorID {
			listed = &all[i]
		}
	}
	require.NotNil(t, listed, "the monitor must appear in the list")
	assert.Equal(t, one.Condition.Precision, listed.Condition.Precision)
	assert.Equal(t, one.Condition.PreciseValue, listed.Condition.PreciseValue)
}

// BalanceMonitor.CallBackURL is tagged json:"-", so an update can never carry
// one. Writing it anyway meant every update cleared the URL the monitor was
// created with.
func TestUpdateMonitor_PreservesCallbackURL_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	balance := legacyTestBalance(t, ds)

	created, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID:   balance.BalanceID,
		CallBackURL: "https://example.com/hook",
		Condition:   model.AlertCondition{Field: "balance", Operator: "<", Value: 100, Precision: 1, PreciseValue: big.NewInt(100)},
	})
	require.NoError(t, err)

	update := model.BalanceMonitor{
		MonitorID: created.MonitorID,
		BalanceID: balance.BalanceID,
		Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 200},
	}
	require.NoError(t, ds.UpdateMonitor(&update))

	var stored string
	require.NoError(t, ds.Conn.QueryRow(
		`SELECT call_back_url FROM blnk.balance_monitors WHERE monitor_id = $1`, created.MonitorID).Scan(&stored))
	assert.Equal(t, "https://example.com/hook", stored, "an update that cannot carry a callback URL must not clear it")
}

// value is the human-readable half of the threshold and is a float64 in Go, but
// the column was a BIGINT, so a fractional threshold failed at the database.
func TestCreateMonitor_FractionalThreshold_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	balance := legacyTestBalance(t, ds)

	created, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID: balance.BalanceID,
		Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 100.5, Precision: 100, PreciseValue: big.NewInt(10050)},
	})
	require.NoError(t, err, "a fractional threshold is a normal thing to want")

	stored, err := ds.GetMonitorByID(created.MonitorID)
	require.NoError(t, err)
	assert.Equal(t, 100.5, stored.Condition.Value)
	assert.Equal(t, big.NewInt(10050), stored.Condition.PreciseValue)
}

// The check constraint stores equality as '=', but compare() only knew '==',
// which the same constraint rejects. Equality monitors were unusable.
func TestCheckCondition_EqualityOperator_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	balance := legacyTestBalance(t, ds)

	created, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID: balance.BalanceID,
		Condition: model.AlertCondition{Field: "balance", Operator: "=", Value: 100, Precision: 1, PreciseValue: big.NewInt(100)},
	})
	require.NoError(t, err)

	stored, err := ds.GetMonitorByID(created.MonitorID)
	require.NoError(t, err)

	exactly := &model.Balance{Balance: big.NewInt(100)}
	exactly.InitializeBalanceFields()
	other := &model.Balance{Balance: big.NewInt(101)}
	other.InitializeBalanceFields()

	assert.True(t, stored.CheckCondition(exactly), "an equality monitor must fire on an equal balance")
	assert.False(t, stored.CheckCondition(other))
}
