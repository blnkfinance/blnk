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
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// Snapshots hold what had been applied when they were taken, regardless of
// effective_date (#398). These tests insert transactions in apply order, so
// their ids are the order the snapshot saw them in, and check that the
// snapshot-based answer always matches the from-source one.

// insertReplayTransaction writes an APPLIED transaction with its own
// created_at and effective_date and returns its transactions.id.
func insertReplayTransaction(t *testing.T, ds Datasource, source, destination string, precise int64, createdAt, effectiveDate time.Time) int64 {
	t.Helper()
	var id int64
	err := ds.Conn.QueryRow(`
		INSERT INTO blnk.transactions
			(transaction_id, source, destination, reference, amount, precise_amount, precision,
			 currency, status, description, hash, created_at, effective_date, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, 100, 'USD', 'APPLIED', 'snapshot seq fixture', 'seq-hash', $7, $8, '{}'::jsonb)
		RETURNING id
	`, model.GenerateUUIDWithSuffix("txn"), source, destination, "seq-"+gofakeit.UUID(),
		float64(precise)/100, precise, createdAt.UTC(), effectiveDate.UTC()).Scan(&id)
	require.NoError(t, err)
	return id
}

// insertSnapshotWithSeq writes a snapshot that contains every transaction on
// the balance up to lastTxSeq.
func insertSnapshotWithSeq(t *testing.T, ds Datasource, balanceID, ledgerID string, credit, debit int64, snapshotTime time.Time, lastTxSeq int64) {
	t.Helper()
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.balance_snapshots
			(balance_id, ledger_id, balance, credit_balance, debit_balance,
			 inflight_balance, inflight_credit_balance, inflight_debit_balance,
			 currency, snapshot_time, created_at, last_tx_seq)
		VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 'USD', $6, $7, $8)
	`, balanceID, ledgerID, credit-debit, credit, debit, snapshotTime.UTC(), snapshotTime.UTC(), lastTxSeq)
	require.NoError(t, err)
}

// insertLegacySnapshot writes a snapshot taken before last_tx_seq existed.
func insertLegacySnapshot(t *testing.T, ds Datasource, balanceID, ledgerID string, credit, debit int64, snapshotTime time.Time) {
	t.Helper()
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.balance_snapshots
			(balance_id, ledger_id, balance, credit_balance, debit_balance,
			 inflight_balance, inflight_credit_balance, inflight_debit_balance,
			 currency, snapshot_time, created_at, last_tx_seq)
		VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 'USD', $6, $7, NULL)
	`, balanceID, ledgerID, credit-debit, credit, debit, snapshotTime.UTC(), snapshotTime.UTC())
	require.NoError(t, err)
}

func newReplayPair(t *testing.T, ds Datasource, name string) (ledgerID, src, dst string) {
	t.Helper()
	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: name + "-" + marker})
	require.NoError(t, err)
	s, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
	require.NoError(t, err)
	d, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
	require.NoError(t, err)
	return ledger.LedgerID, s.BalanceID, d.BalanceID
}

func assertBalanceAt(t *testing.T, ds Datasource, balanceID string, at time.Time, credit, debit int64) {
	t.Helper()
	ctx := context.Background()
	for _, fromSource := range []bool{false, true} {
		bal, err := ds.GetBalanceAtTime(ctx, balanceID, at, fromSource)
		require.NoError(t, err)
		assert.Equal(t, 0, bal.CreditBalance.Cmp(big.NewInt(credit)),
			"from_source=%v: credit want %d, got %s", fromSource, credit, bal.CreditBalance)
		assert.Equal(t, 0, bal.DebitBalance.Cmp(big.NewInt(debit)),
			"from_source=%v: debit want %d, got %s", fromSource, debit, bal.DebitBalance)
	}
}

func TestGetBalanceAtTime_SnapshotSeq_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	now := time.Now().UTC()
	snapTime := now.Add(-5 * time.Minute)

	t.Run("backdated transaction applied after the snapshot is counted", func(t *testing.T) {
		ledgerID, src, dst := newReplayPair(t, ds, "seq-backdated")

		first := insertReplayTransaction(t, ds, src, dst, 1000, now.Add(-10*time.Minute), now.Add(-10*time.Minute))
		insertSnapshotWithSeq(t, ds, dst, ledgerID, 1000, 0, snapTime, first)
		insertSnapshotWithSeq(t, ds, src, ledgerID, 0, 1000, snapTime, first)

		// Created after the snapshot, effective before it: the reproduction in #398.
		insertReplayTransaction(t, ds, src, dst, 300, now.Add(-1*time.Minute), now.Add(-8*time.Minute))

		assertBalanceAt(t, ds, dst, now, 1300, 0)
		assertBalanceAt(t, ds, src, now, 0, 1300)
	})

	t.Run("future-dated transaction in the snapshot is not counted twice", func(t *testing.T) {
		ledgerID, src, dst := newReplayPair(t, ds, "seq-future")

		insertReplayTransaction(t, ds, src, dst, 1000, now.Add(-10*time.Minute), now.Add(-10*time.Minute))
		future := insertReplayTransaction(t, ds, src, dst, 200, now.Add(-7*time.Minute), now.Add(time.Hour))
		insertSnapshotWithSeq(t, ds, dst, ledgerID, 1200, 0, snapTime, future)

		assertBalanceAt(t, ds, dst, now, 1000, 0)
		assertBalanceAt(t, ds, dst, now.Add(2*time.Hour), 1200, 0)
	})

	t.Run("transaction queued before the snapshot and applied after it is counted", func(t *testing.T) {
		ledgerID, src, dst := newReplayPair(t, ds, "seq-queued")

		first := insertReplayTransaction(t, ds, src, dst, 1000, now.Add(-10*time.Minute), now.Add(-10*time.Minute))
		insertSnapshotWithSeq(t, ds, dst, ledgerID, 1000, 0, snapTime, first)

		// created_at is the enqueue time, so it is older than the snapshot even
		// though the balance only moved after it.
		insertReplayTransaction(t, ds, src, dst, 50, now.Add(-6*time.Minute), now.Add(-6*time.Minute))

		assertBalanceAt(t, ds, dst, now, 1050, 0)
	})

	t.Run("snapshot without a seq is skipped for a backdated transaction", func(t *testing.T) {
		ledgerID, src, dst := newReplayPair(t, ds, "seq-legacy")

		insertReplayTransaction(t, ds, src, dst, 1000, now.Add(-10*time.Minute), now.Add(-10*time.Minute))
		insertLegacySnapshot(t, ds, dst, ledgerID, 1000, 0, snapTime)
		insertReplayTransaction(t, ds, src, dst, 300, now.Add(-1*time.Minute), now.Add(-8*time.Minute))

		assertBalanceAt(t, ds, dst, now, 1300, 0)
	})

	t.Run("snapshot without a seq is skipped for a transaction queued before it and applied after it", func(t *testing.T) {
		ledgerID, src, dst := newReplayPair(t, ds, "seq-legacy-queued")

		insertReplayTransaction(t, ds, src, dst, 1000, now.Add(-10*time.Minute), now.Add(-10*time.Minute))
		insertLegacySnapshot(t, ds, dst, ledgerID, 1000, 0, snapTime)
		// Queued at -6m, before the snapshot, but applied after it, so the
		// snapshot does not hold it even though created_at is older.
		insertReplayTransaction(t, ds, src, dst, 50, now.Add(-6*time.Minute), now.Add(-6*time.Minute))

		assertBalanceAt(t, ds, dst, now, 1050, 0)
	})
}

// TestGetBalanceAtTime_SnapshotSeqMatchesFromSource_RealDB applies random
// transactions, with created_at and effective_date scattered on both sides of
// the snapshot, and checks the snapshot-based balance against the from-source
// balance at many target times.
func TestGetBalanceAtTime_SnapshotSeqMatchesFromSource_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(398))

	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round %d", round), func(t *testing.T) {
			ledgerID, src, dst := newReplayPair(t, ds, "seq-parity")
			now := time.Now().UTC()
			snapTime := now.Add(-time.Hour)
			jitter := func() time.Time { return snapTime.Add(time.Duration(rng.Intn(240)-120) * time.Minute) }

			var credit, debit, lastSeq int64
			snapshotAt := 1 + rng.Intn(10)
			for i := 0; i < 12; i++ {
				amount := int64(1 + rng.Intn(1000))
				from, to := src, dst
				if rng.Intn(2) == 0 {
					from, to = dst, src
				}
				lastSeq = insertReplayTransaction(t, ds, from, to, amount, jitter(), jitter())
				if to == dst {
					credit += amount
				} else {
					debit += amount
				}
				if i+1 == snapshotAt {
					insertSnapshotWithSeq(t, ds, dst, ledgerID, credit, debit, snapTime, lastSeq)
				}
			}

			for minutes := 0; minutes <= 180; minutes += 15 {
				at := snapTime.Add(time.Duration(minutes) * time.Minute)
				want, err := ds.GetBalanceAtTime(ctx, dst, at, true)
				require.NoError(t, err)
				got, err := ds.GetBalanceAtTime(ctx, dst, at, false)
				require.NoError(t, err)
				assert.Equal(t, want.CreditBalance.String(), got.CreditBalance.String(), "credit at %s", at)
				assert.Equal(t, want.DebitBalance.String(), got.DebitBalance.String(), "debit at %s", at)
			}
		})
	}
}

func TestTakeBalanceSnapshots_RecordsLastAppliedSeq_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	_, src, dst := newReplayPair(t, ds, "seq-writer")
	now := time.Now().UTC()

	applied := insertReplayTransaction(t, ds, src, dst, 100, now, now)
	// A later queued row has a higher id but has not moved the balance.
	insertTestTransaction(t, ds, model.GenerateUUIDWithSuffix("txn"), src, dst,
		"seq-writer-queued-"+gofakeit.UUID(), 1, 100, "USD", "QUEUED", now, `{}`)

	var snapshotted int
	require.NoError(t, ds.Conn.QueryRowContext(ctx,
		"SELECT blnk.take_daily_balance_snapshots_batched($1)", 1000).Scan(&snapshotted))

	var seq int64
	require.NoError(t, ds.Conn.QueryRowContext(ctx,
		"SELECT last_tx_seq FROM blnk.balance_snapshots WHERE balance_id = $1", dst).Scan(&seq))
	assert.Equal(t, applied, seq)

	_, err := ds.Conn.ExecContext(ctx,
		"UPDATE blnk.balance_snapshots SET last_tx_seq = last_tx_seq + 1 WHERE balance_id = $1", dst)
	assert.Error(t, err, "last_tx_seq must be immutable like the rest of the snapshot")
}
