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
	"context"
	"database/sql"
	"errors"
	"math/big"
	"os"
	"regexp"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

const transitionQuery = `
		UPDATE blnk.balance_monitors
		SET condition_state = $2, state_version = $3, state_changed_at = NOW()
		WHERE monitor_id = $1
		  AND balance_id = $4
		  AND condition_state IS DISTINCT FROM $2
		  AND state_version < $3
	`

const releaseQuery = `
		UPDATE blnk.balance_monitors
		SET condition_state = FALSE, state_changed_at = NOW()
		WHERE monitor_id = $1
		  AND balance_id = $3
		  AND state_version = $2
		  AND condition_state = TRUE
	`

func TestTransitionMonitorState_OwnsTheTransition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectExec(regexp.QuoteMeta(transitionQuery)).
		WithArgs("mon_123", true, int64(7), "bln_123").
		WillReturnResult(sqlmock.NewResult(0, 1))

	owned, err := Datasource{Conn: db}.TransitionMonitorState(context.Background(), "mon_123", "bln_123", true, 7)
	assert.NoError(t, err)
	assert.True(t, owned)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransitionMonitorState_NoRowMeansSomebodyElseOwnsIt(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectExec(regexp.QuoteMeta(transitionQuery)).
		WithArgs("mon_123", true, int64(7), "bln_123").
		WillReturnResult(sqlmock.NewResult(0, 0))

	owned, err := Datasource{Conn: db}.TransitionMonitorState(context.Background(), "mon_123", "bln_123", true, 7)
	assert.NoError(t, err)
	assert.False(t, owned)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransitionMonitorState_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectExec(regexp.QuoteMeta(transitionQuery)).
		WithArgs("mon_123", false, int64(7), "bln_123").
		WillReturnError(errors.New("connection reset"))

	owned, err := Datasource{Conn: db}.TransitionMonitorState(context.Background(), "mon_123", "bln_123", false, 7)
	assert.Error(t, err)
	assert.False(t, owned)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReleaseMonitorState(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectExec(regexp.QuoteMeta(releaseQuery)).
		WithArgs("mon_123", int64(7), "bln_123").
		WillReturnResult(sqlmock.NewResult(0, 1))

	assert.NoError(t, Datasource{Conn: db}.ReleaseMonitorState(context.Background(), "mon_123", "bln_123", 7))
	assert.NoError(t, mock.ExpectationsWereMet())
}

// seedMonitor creates the ledger, balance and monitor a state test needs and
// returns the monitor ID.
func seedMonitor(t *testing.T, ds Datasource) (string, string) {
	t.Helper()

	ledger, err := ds.CreateLedger(model.Ledger{Name: gofakeit.UUID()})
	require.NoError(t, err)

	balance, err := ds.CreateBalance(model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	monitor, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID:   balance.BalanceID,
		Description: "edge state test",
		Condition: model.AlertCondition{
			Field:        "balance",
			Operator:     "<",
			Value:        100,
			Precision:    1,
			PreciseValue: big.NewInt(100),
		},
	})
	require.NoError(t, err)

	return monitor.MonitorID, balance.BalanceID
}

func readMonitorState(t *testing.T, ds Datasource, monitorID string) (bool, int64) {
	t.Helper()
	var state bool
	var version int64
	err := ds.Conn.QueryRow(`SELECT condition_state, state_version FROM blnk.balance_monitors WHERE monitor_id = $1`, monitorID).
		Scan(&state, &version)
	require.NoError(t, err)
	return state, version
}

// TestTransitionMonitorState_ConcurrentEvaluations_RealDB is the test the edge
// design rests on. Post-commit monitor checks run in detached goroutines, so
// several evaluations of the same monitor can race; exactly one of them may be
// told it owns the crossing, or the consumer gets the duplicate alerts the
// feature exists to remove.
func TestTransitionMonitorState_ConcurrentEvaluations_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	monitorID, balanceID := seedMonitor(t, ds)

	const racers = 24

	var wg sync.WaitGroup
	var mu sync.Mutex
	owners := 0
	start := make(chan struct{})

	for i := 1; i <= racers; i++ {
		wg.Add(1)
		go func(version int64) {
			defer wg.Done()
			<-start
			owned, err := ds.TransitionMonitorState(ctx, monitorID, balanceID, true, version)
			if err != nil {
				t.Errorf("transition failed: %v", err)
				return
			}
			if owned {
				mu.Lock()
				owners++
				mu.Unlock()
			}
		}(int64(i))
	}

	close(start)
	wg.Wait()

	assert.Equal(t, 1, owners, "exactly one concurrent evaluation may own the false -> true crossing")

	state, version := readMonitorState(t, ds, monitorID)
	assert.True(t, state, "the monitor must be left triggered")
	assert.LessOrEqual(t, version, int64(racers))
}

// TestTransitionMonitorState_VersionFence_RealDB pins that an evaluation running
// on an older balance cannot flip the state back. Without the fence it would
// alert on a balance that has already recovered, and then swallow the next
// genuine crossing.
func TestTransitionMonitorState_VersionFence_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	monitorID, balanceID := seedMonitor(t, ds)

	owned, err := ds.TransitionMonitorState(ctx, monitorID, balanceID, true, 10)
	require.NoError(t, err)
	require.True(t, owned)

	// A goroutine that evaluated balance version 6 finally gets scheduled.
	owned, err = ds.TransitionMonitorState(ctx, monitorID, balanceID, false, 6)
	require.NoError(t, err)
	assert.False(t, owned, "a stale evaluation must not own a transition")

	state, version := readMonitorState(t, ds, monitorID)
	assert.True(t, state, "a stale evaluation must not move the state")
	assert.Equal(t, int64(10), version)

	// The genuine recovery, on a newer version, still re-arms.
	owned, err = ds.TransitionMonitorState(ctx, monitorID, balanceID, false, 11)
	require.NoError(t, err)
	assert.True(t, owned)

	state, version = readMonitorState(t, ds, monitorID)
	assert.False(t, state)
	assert.Equal(t, int64(11), version)
}

func TestReleaseMonitorState_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	monitorID, balanceID := seedMonitor(t, ds)

	owned, err := ds.TransitionMonitorState(ctx, monitorID, balanceID, true, 4)
	require.NoError(t, err)
	require.True(t, owned)

	// A release for a version we no longer hold must not disarm anything.
	require.NoError(t, ds.ReleaseMonitorState(ctx, monitorID, balanceID, 3))
	state, _ := readMonitorState(t, ds, monitorID)
	assert.True(t, state, "a release fenced to another version must do nothing")

	require.NoError(t, ds.ReleaseMonitorState(ctx, monitorID, balanceID, 4))
	state, version := readMonitorState(t, ds, monitorID)
	assert.False(t, state, "releasing our own transition re-arms the monitor")
	assert.Equal(t, int64(4), version, "the version stays put so the next evaluation still fences correctly")
}

// TestUpdateMonitor_RearmsState_RealDB pins the documented lifecycle rule: the
// condition has been replaced, so state carried against the old one is dropped.
func TestUpdateMonitor_RearmsState_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	monitorID, balanceID := seedMonitor(t, ds)

	owned, err := ds.TransitionMonitorState(ctx, monitorID, balanceID, true, 5)
	require.NoError(t, err)
	require.True(t, owned)

	monitor, err := ds.GetMonitorByID(monitorID)
	require.NoError(t, err)
	require.True(t, monitor.ConditionState, "GetMonitorByID must report the stored state")

	monitor.Condition.Operator = ">"
	monitor.Trigger = model.TriggerLevel
	require.NoError(t, ds.UpdateMonitor(monitor))

	state, version := readMonitorState(t, ds, monitorID)
	assert.False(t, state, "updating a monitor re-arms it")
	assert.Equal(t, int64(0), version)

	reread, err := ds.GetMonitorByID(monitorID)
	require.NoError(t, err)
	assert.Equal(t, model.TriggerLevel, reread.Trigger)
}

// TestCreateMonitor_DefaultsToEdge_RealDB pins the new default at the storage
// layer, where the migration's column default and the insert have to agree.
func TestCreateMonitor_DefaultsToEdge_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	monitorID, _ := seedMonitor(t, ds)

	monitor, err := ds.GetMonitorByID(monitorID)
	require.NoError(t, err)
	assert.Equal(t, model.TriggerEdge, monitor.Trigger)
	assert.False(t, monitor.ConditionState)
}

// TestMonitorTriggerColumnDefault_RealDB pins the promise the migration makes to
// monitors that already exist: they adopt edge and start armed, so one whose
// condition already holds alerts once on its balance's next transaction.
func TestMonitorTriggerColumnDefault_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	_, balanceID := seedMonitor(t, ds)

	// An insert that names none of the new columns, the way a row written before
	// this migration looks once the migration has run.
	legacyID := model.GenerateUUIDWithSuffix("mon")
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, description, call_back_url, created_at)
		VALUES ($1, $2, 'balance', '<', 100, 'legacy row', '', NOW())
	`, legacyID, balanceID)
	require.NoError(t, err)

	var trigger string
	var state bool
	var version int64
	require.NoError(t, ds.Conn.QueryRow(
		`SELECT trigger_type, condition_state, state_version FROM blnk.balance_monitors WHERE monitor_id = $1`, legacyID,
	).Scan(&trigger, &state, &version))

	assert.Equal(t, model.TriggerEdge, trigger)
	assert.False(t, state, "an existing monitor starts armed")
	assert.Equal(t, int64(0), version)
}

// TestMonitorTriggerCheckConstraint_RealDB keeps the table as the last word on
// what is storable, whatever slips past the API.
func TestMonitorTriggerCheckConstraint_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	monitorID, _ := seedMonitor(t, ds)

	_, err := ds.Conn.Exec(`UPDATE blnk.balance_monitors SET trigger_type = 'sometimes' WHERE monitor_id = $1`, monitorID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "balance_monitors_trigger_type_check")

	for _, valid := range []string{model.TriggerEdge, model.TriggerLevel} {
		_, err := ds.Conn.Exec(`UPDATE blnk.balance_monitors SET trigger_type = $2 WHERE monitor_id = $1`, monitorID, valid)
		assert.NoError(t, err, "trigger %q must be storable", valid)
	}
}

// TestCreateMonitor_KeepsAnExplicitTrigger_RealDB guards the normalisation from
// coercing a real choice into the default.
func TestCreateMonitor_KeepsAnExplicitTrigger_RealDB(t *testing.T) {
	ds := openRealTestDB(t)

	ledger, err := ds.CreateLedger(model.Ledger{Name: gofakeit.UUID()})
	require.NoError(t, err)
	balance, err := ds.CreateBalance(model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	created, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID: balance.BalanceID,
		Trigger:   model.TriggerLevel,
		Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 100, Precision: 1, PreciseValue: big.NewInt(100)},
	})
	require.NoError(t, err)

	stored, err := ds.GetMonitorByID(created.MonitorID)
	require.NoError(t, err)
	assert.Equal(t, model.TriggerLevel, stored.Trigger)
}

// TestGetBalanceMonitors_CarriesTheTrigger_RealDB covers the read the evaluation
// path actually uses, which is a different query from GetMonitorByID.
func TestGetBalanceMonitors_CarriesTheTrigger_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	monitorID, balanceID := seedMonitor(t, ds)

	owned, err := ds.TransitionMonitorState(ctx, monitorID, balanceID, true, 3)
	require.NoError(t, err)
	require.True(t, owned)

	monitors, err := ds.GetBalanceMonitors(balanceID)
	require.NoError(t, err)
	require.Len(t, monitors, 1)
	assert.Equal(t, model.TriggerEdge, monitors[0].Trigger)
	assert.True(t, monitors[0].ConditionState)

	all, err := ds.GetAllMonitors()
	require.NoError(t, err)
	found := false
	for _, m := range all {
		if m.MonitorID == monitorID {
			assert.Equal(t, model.TriggerEdge, m.Trigger)
			assert.True(t, m.ConditionState)
			found = true
		}
	}
	assert.True(t, found, "GetAllMonitors must return the monitor it just stored")
}

// BenchmarkTransitionMonitorState measures what an edge monitor adds to a
// committed balance update. The steady-state case is the one that matters: a
// monitor whose condition has not changed matches no rows and writes nothing,
// and that is what almost every transaction sees.
func BenchmarkTransitionMonitorState(b *testing.B) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultRealTestDSN
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		b.Skipf("real database unavailable: %v", err)
	}
	if err := db.Ping(); err != nil {
		b.Skipf("real database unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	ds := Datasource{Conn: db}

	ledger, err := ds.CreateLedger(model.Ledger{Name: gofakeit.UUID()})
	if err != nil {
		b.Fatal(err)
	}
	balance, err := ds.CreateBalance(model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	if err != nil {
		b.Fatal(err)
	}
	monitor, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID: balance.BalanceID,
		Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 100, Precision: 1, PreciseValue: big.NewInt(100)},
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.Run("steady state (no transition)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := ds.TransitionMonitorState(ctx, monitor.MonitorID, balance.BalanceID, false, int64(i+1)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("alternating (every call transitions)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := ds.TransitionMonitorState(ctx, monitor.MonitorID, balance.BalanceID, i%2 == 0, int64(i+1)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
