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
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// openTestDBInTimeZone opens the real test database on a connection pinned to
// one session TimeZone.
//
// MaxOpenConns(1) is what makes the SET stick: the setting is per-session, and
// a pool that opens a second connection would run half the test on an
// unconfigured one. GetBalanceAtTime opens its own transaction, but on this
// pool that is still the same connection.
func openTestDBInTimeZone(t *testing.T, tz string) Datasource {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultRealTestDSN
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Skipf("real database unavailable at %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// SET does not take bind parameters; set_config is its parameterised form.
	// false = session-scoped, not transaction-scoped.
	_, err = db.Exec("SELECT set_config('TimeZone', $1, false)", tz)
	require.NoError(t, err)

	var applied string
	require.NoError(t, db.QueryRow("SHOW TimeZone").Scan(&applied))
	require.Equal(t, tz, applied)

	return Datasource{Conn: db}
}

// TestGetBalanceAtTime_NonUTCSession_RealDB pins historical balances to the
// same answer regardless of the database session's TimeZone.
//
// Two clocks meet in GetBalanceAtTime's
//
//	WHERE COALESCE(effective_date, created_at) > $snapshot_time
//
// created_at is written by Go as txn.CreatedAt.UTC(), so it is always a UTC
// wall clock. snapshot_time is written by
// blnk.take_daily_balance_snapshots_batched. If that side records the session's
// wall clock instead — or if the column's type leaves Postgres to reinterpret
// it in the session zone at comparison time — the bound sits at the session's
// UTC offset away from the transaction timeline, and the endpoint answers with
// a wrong number and no error:
//
//	east of UTC   the range narrows, post-snapshot transactions are dropped
//	west of UTC   the range widens, pre-snapshot transactions are applied on
//	              top of the snapshot that already accounts for them
//
// Both offsets are exercised, and the snapshot comes from the real function
// rather than a hand-written row, so the writer and the column type are both
// covered.
func TestGetBalanceAtTime_NonUTCSession_RealDB(t *testing.T) {
	for _, tz := range []string{"UTC", "Asia/Kolkata", "America/New_York"} {
		t.Run(tz, func(t *testing.T) {
			ds := openTestDBInTimeZone(t, tz)
			ctx := context.Background()

			marker := gofakeit.UUID()
			ledger, err := ds.CreateLedger(model.Ledger{Name: "bat-tz-" + marker})
			require.NoError(t, err)
			src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
			require.NoError(t, err)
			dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
			require.NoError(t, err)

			now := time.Now().UTC()

			// Before the snapshot. The balances row is left untouched, so the
			// snapshot taken below records zeros — which makes this transaction
			// a sentinel: it must not appear in the answer, and if the range is
			// widened it shows up as an extra 1000 of credit.
			insertUTCTransaction(t, ds, src.BalanceID, dst.BalanceID,
				"bat-tz-before-"+marker, 1000, now.Add(-3*time.Hour))

			// The snapshot itself, written by the production writer on whatever
			// clock it chooses.
			var snapshotted int
			require.NoError(t,
				ds.Conn.QueryRowContext(ctx, "SELECT blnk.take_daily_balance_snapshots_batched($1)", 1000).
					Scan(&snapshotted))
			require.NotZero(t, snapshotted, "the snapshot function recorded nothing to test against")

			var snapshots int
			require.NoError(t, ds.Conn.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM blnk.balance_snapshots WHERE balance_id = $1", dst.BalanceID).
				Scan(&snapshots))
			require.Equal(t, 1, snapshots, "expected exactly one snapshot for the balance under test")

			// After the snapshot, and dated forward so it is unambiguously on
			// the far side of it. This one must be applied on top.
			insertUTCTransaction(t, ds, src.BalanceID, dst.BalanceID,
				"bat-tz-after-"+marker, 400, now.Add(2*time.Hour))

			bal, err := ds.GetBalanceAtTime(ctx, dst.BalanceID, now.Add(3*time.Hour), false)
			require.NoError(t, err)

			assert.Equal(t, 0, bal.CreditBalance.Cmp(big.NewInt(400)),
				"credit must be the snapshot (0) plus only the post-snapshot 400, got %s", bal.CreditBalance)
			assert.Equal(t, 0, bal.DebitBalance.Cmp(big.NewInt(0)),
				"no debits were recorded against this balance, got %s", bal.DebitBalance)
		})
	}
}

// TestGetBalanceAtTime_TargetTimeOffset_RealDB pins the other end of the same
// comparison: the caller's own timestamp.
//
// GET /balances/:id/at parses ?timestamp= with time.Parse(time.RFC3339), which
// keeps the offset the client wrote. The cutoff is then applied against plain
// TIMESTAMP columns holding UTC wall clocks, and a TIMESTAMP comparison
// discards the offset rather than applying it — so the same instant written
// three different ways has to come back with the same balance, or the endpoint
// is answering a question nobody asked.
func TestGetBalanceAtTime_TargetTimeOffset_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "bat-offset-" + marker})
	require.NoError(t, err)
	src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
	require.NoError(t, err)
	dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
	require.NoError(t, err)

	now := time.Now().UTC()
	// Inside the cutoff.
	insertUTCTransaction(t, ds, src.BalanceID, dst.BalanceID,
		"bat-offset-in-"+marker, 1000, now.Add(-4*time.Hour))
	// Outside it — and inside the window a dropped +05:30 offset would open up.
	insertUTCTransaction(t, ds, src.BalanceID, dst.BalanceID,
		"bat-offset-out-"+marker, 500, now.Add(-1*time.Hour))

	cutoff := now.Add(-2 * time.Hour)
	for _, zone := range []*time.Location{
		time.UTC,
		time.FixedZone("+0530", 5*3600+30*60),
		time.FixedZone("-0400", -4*3600),
	} {
		t.Run(zone.String(), func(t *testing.T) {
			bal, err := ds.GetBalanceAtTime(ctx, dst.BalanceID, cutoff.In(zone), true)
			require.NoError(t, err)
			assert.Equal(t, 0, bal.CreditBalance.Cmp(big.NewInt(1000)),
				"the cutoff is an instant, not a wall-clock reading: expected 1000, got %s", bal.CreditBalance)
		})
	}
}

// insertUTCTransaction writes a transaction the way the production insert in
// RecordTransaction does: created_at normalised to UTC before it reaches the
// plain TIMESTAMP column.
func insertUTCTransaction(t *testing.T, ds Datasource, source, destination, reference string, precise int64, createdAt time.Time) {
	t.Helper()
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.transactions
			(transaction_id, source, destination, reference, amount, precise_amount, precision,
			 currency, status, description, hash, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, 100, 'USD', 'APPLIED', 'timezone fixture', 'bat-tz-hash', $7, '{}'::jsonb)
	`, model.GenerateUUIDWithSuffix("txn"), source, destination, reference,
		float64(precise)/100, precise, createdAt.UTC())
	require.NoError(t, err)
}
