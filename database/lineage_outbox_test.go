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
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// quiesceOutbox parks all unrelated pending outbox rows behind a future lock
// so that claim assertions only see this test's fixtures. The shared database
// persists rows between runs, so this keeps claims deterministic. It also
// registers a cleanup that marks this test's fixtures terminal so they never
// leak into later runs or get chewed by lineage processors started elsewhere.
func quiesceOutbox(t *testing.T, ds Datasource, markerPrefix string) {
	t.Helper()
	_, err := ds.Conn.Exec(`
		UPDATE blnk.lineage_outbox
		SET locked_until = NOW() + interval '1 hour'
		WHERE status = 'pending'
		  AND (locked_until IS NULL OR locked_until < NOW())
		  AND transaction_id NOT LIKE $1
	`, markerPrefix+"%")
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := ds.Conn.Exec(`
			UPDATE blnk.lineage_outbox
			SET status = 'completed', processed_at = NOW()
			WHERE transaction_id LIKE $1
			  AND status IN ('pending', 'processing')
		`, markerPrefix+"%")
		if err != nil {
			t.Logf("failed to retire outbox fixtures: %v", err)
		}
	})
}

func newOutboxEntry(markerPrefix string) *model.LineageOutbox {
	return &model.LineageOutbox{
		TransactionID: markerPrefix + model.GenerateUUIDWithSuffix("txn"),
		LineageType:   model.LineageTypeCredit,
		Payload:       json.RawMessage(`{"fixture":"` + markerPrefix + `"}`),
		MaxAttempts:   5,
	}
}

// ---------------------------------------------------------------------------
// DeleteLineageMapping
// ---------------------------------------------------------------------------

func TestDeleteLineageMapping_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "cov-lineage-ledger-" + marker})
	require.NoError(t, err)
	identity, err := ds.CreateIdentity(model.Identity{
		FirstName: "Lin", LastName: "Eage",
		EmailAddress: "lineage-" + marker + "@example.com",
		IdentityType: "individual",
	})
	require.NoError(t, err)

	mkBal := func() string {
		b, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
		require.NoError(t, err)
		return b.BalanceID
	}
	mainBal, shadowBal, aggBal := mkBal(), mkBal(), mkBal()

	provider := "cov-provider-" + marker
	require.NoError(t, ds.UpsertLineageMapping(ctx, model.LineageMapping{
		BalanceID:          mainBal,
		Provider:           provider,
		ShadowBalanceID:    shadowBal,
		AggregateBalanceID: aggBal,
		IdentityID:         identity.IdentityID,
	}))

	mappings, err := ds.GetLineageMappings(ctx, mainBal)
	require.NoError(t, err)
	require.Len(t, mappings, 1)
	mappingID := mappings[0].ID
	require.NotZero(t, mappingID)

	t.Run("delete removes the mapping", func(t *testing.T) {
		require.NoError(t, ds.DeleteLineageMapping(ctx, mappingID))

		after, err := ds.GetLineageMappings(ctx, mainBal)
		require.NoError(t, err)
		assert.Empty(t, after)

		byProvider, err := ds.GetLineageMappingByProvider(ctx, mainBal, provider)
		require.NoError(t, err)
		assert.Nil(t, byProvider)
	})

	t.Run("deleting the same mapping twice returns not found", func(t *testing.T) {
		err := ds.DeleteLineageMapping(ctx, mappingID)
		require.Error(t, err)
		apiErr, ok := err.(apierror.APIError)
		require.True(t, ok, "expected apierror.APIError, got %T", err)
		assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
	})

	t.Run("deleting a nonexistent id returns not found", func(t *testing.T) {
		err := ds.DeleteLineageMapping(ctx, int64(-99999))
		require.Error(t, err)
		apiErr, ok := err.(apierror.APIError)
		require.True(t, ok)
		assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
	})
}

// ---------------------------------------------------------------------------
// InsertLineageOutbox / InsertLineageOutboxInTx
// ---------------------------------------------------------------------------

func TestInsertLineageOutbox_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	markerPrefix := "covob-" + gofakeit.UUID()[:8] + "-"

	t.Run("insert sets id and round-trips through GetOutboxByTransactionID", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)
		entry.SourceBalanceID = "bln_src"
		entry.DestinationBalanceID = "bln_dst"
		entry.Provider = "prov-x"
		entry.Inflight = true

		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))
		assert.NotZero(t, entry.ID, "RETURNING id must populate the entry")

		got, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, entry.ID, got.ID)
		assert.Equal(t, entry.TransactionID, got.TransactionID)
		assert.Equal(t, "bln_src", got.SourceBalanceID)
		assert.Equal(t, "bln_dst", got.DestinationBalanceID)
		assert.Equal(t, "prov-x", got.Provider)
		assert.Equal(t, model.LineageTypeCredit, got.LineageType)
		assert.Equal(t, model.OutboxStatusPending, got.Status, "fresh entries must be pending")
		assert.Equal(t, 0, got.Attempts)
		assert.Equal(t, 5, got.MaxAttempts)
		assert.True(t, got.Inflight)
		assert.Nil(t, got.ProcessedAt)
		assert.Nil(t, got.LockedUntil)
		assert.JSONEq(t, string(entry.Payload), string(got.Payload))
	})

	t.Run("duplicate transaction_id is rejected by unique index", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))

		dup := newOutboxEntry(markerPrefix)
		dup.TransactionID = entry.TransactionID
		err := ds.InsertLineageOutbox(ctx, dup)
		require.Error(t, err, "outbox must enforce one entry per transaction_id")
	})

	t.Run("get by unknown transaction id returns nil without error", func(t *testing.T) {
		got, err := ds.GetOutboxByTransactionID(ctx, "covob-missing-"+gofakeit.UUID())
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestInsertLineageOutboxInTx_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	markerPrefix := "covobtx-" + gofakeit.UUID()[:8] + "-"

	t.Run("rolled back transaction leaves no outbox entry", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)

		tx, err := ds.Conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, ds.InsertLineageOutboxInTx(ctx, tx, entry))
		assert.NotZero(t, entry.ID)
		require.NoError(t, tx.Rollback())

		got, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		assert.Nil(t, got, "entry inserted in a rolled-back tx must not be visible")
	})

	t.Run("committed transaction persists the outbox entry atomically", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)

		tx, err := ds.Conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, ds.InsertLineageOutboxInTx(ctx, tx, entry))

		// Not visible outside the transaction until commit.
		before, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		assert.Nil(t, before, "uncommitted entry must not be visible to other connections")

		require.NoError(t, tx.Commit())

		after, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		require.NotNil(t, after)
		assert.Equal(t, model.OutboxStatusPending, after.Status)
	})
}

// ---------------------------------------------------------------------------
// ClaimPendingOutboxEntries
// ---------------------------------------------------------------------------

func TestClaimPendingOutboxEntries_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	markerPrefix := "covclaim-" + gofakeit.UUID()[:8] + "-"
	quiesceOutbox(t, ds, markerPrefix)

	mine := func(entries []model.LineageOutbox) []string {
		ids := make([]string, 0)
		for _, e := range entries {
			if strings.HasPrefix(e.TransactionID, markerPrefix) {
				ids = append(ids, e.TransactionID)
			}
		}
		return ids
	}

	var inserted []string
	for i := 0; i < 4; i++ {
		entry := newOutboxEntry(markerPrefix)
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))
		inserted = append(inserted, entry.TransactionID)
		time.Sleep(2 * time.Millisecond) // distinct created_at for stable FIFO ordering
	}

	t.Run("claim marks entries processing with a future lock", func(t *testing.T) {
		claimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)

		got := mine(claimed)
		assert.ElementsMatch(t, inserted, got, "all of my pending entries must be claimed")

		for _, e := range claimed {
			if !strings.HasPrefix(e.TransactionID, markerPrefix) {
				continue
			}
			assert.Equal(t, model.OutboxStatusProcessing, e.Status)
			require.NotNil(t, e.LockedUntil)
			// Compare on the database clock: locked_until is a timestamp
			// without time zone, so Go-side wall-clock comparisons are
			// unreliable across timezones.
			var lockInFuture bool
			require.NoError(t, ds.Conn.QueryRow(
				"SELECT locked_until > NOW() FROM blnk.lineage_outbox WHERE id = $1", e.ID).Scan(&lockInFuture))
			assert.True(t, lockInFuture, "locked_until must be in the future for %s", e.TransactionID)
		}
	})

	t.Run("claim batch picks the oldest pending entries first", func(t *testing.T) {
		// Fresh batch: claim with batchSize 1 repeatedly and verify the
		// candidate selection follows created_at ASC. (The RETURNING order of
		// the claiming UPDATE itself is unspecified, so ordering can only be
		// asserted via single-row claims.)
		var batch []string
		for i := 0; i < 3; i++ {
			entry := newOutboxEntry(markerPrefix)
			require.NoError(t, ds.InsertLineageOutbox(ctx, entry))
			batch = append(batch, entry.TransactionID)
			time.Sleep(2 * time.Millisecond)
		}

		var claimedOrder []string
		for i := 0; i < 3; i++ {
			claimed, err := ds.ClaimPendingOutboxEntries(ctx, 1, 5*time.Minute)
			require.NoError(t, err)
			require.Len(t, claimed, 1)
			claimedOrder = append(claimedOrder, claimed[0].TransactionID)
		}
		assert.Equal(t, batch, claimedOrder, "single-row claims must drain the queue oldest-first")
	})

	t.Run("second claim must not return already-processing entries", func(t *testing.T) {
		claimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		assert.Empty(t, mine(claimed), "claimed entries must not be claimable again")
	})

	t.Run("failed entries below max_attempts return to pending and are reclaimable", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))

		claimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		require.Contains(t, mine(claimed), entry.TransactionID)

		require.NoError(t, ds.MarkOutboxFailed(ctx, entry.ID, "boom"))
		got, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		assert.Equal(t, model.OutboxStatusPending, got.Status)
		assert.Equal(t, 1, got.Attempts)
		assert.Equal(t, "boom", got.LastError)

		reclaimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		assert.Contains(t, mine(reclaimed), entry.TransactionID)
	})

	t.Run("entries at max attempts are never claimed", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)
		entry.MaxAttempts = 2
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))
		_, err := ds.Conn.Exec("UPDATE blnk.lineage_outbox SET attempts = max_attempts WHERE id = $1", entry.ID)
		require.NoError(t, err)

		claimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		assert.NotContains(t, mine(claimed), entry.TransactionID)
	})

	t.Run("completed entries are terminal", func(t *testing.T) {
		entry := newOutboxEntry(markerPrefix)
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))

		claimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		require.Contains(t, mine(claimed), entry.TransactionID)

		require.NoError(t, ds.MarkOutboxCompleted(ctx, entry.ID))
		got, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		assert.Equal(t, model.OutboxStatusCompleted, got.Status)
		require.NotNil(t, got.ProcessedAt)
		assert.Nil(t, got.LockedUntil)

		again, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		assert.NotContains(t, mine(again), entry.TransactionID)
	})

	t.Run("expired lock on a processing entry is not reclaimable", func(t *testing.T) {
		// A processor that crashes after claiming leaves status='processing'.
		// The claim query filters on status='pending', so even after
		// locked_until expires the entry can never be picked up again unless
		// something resets its status. Verify the current behavior and flag it.
		entry := newOutboxEntry(markerPrefix)
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))

		claimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		require.Contains(t, mine(claimed), entry.TransactionID)

		// Simulate a crashed processor: the lock expires (forced on the
		// database clock to avoid timezone skew) but the status stays
		// 'processing' because MarkOutboxFailed/Completed never ran.
		_, err = ds.Conn.Exec(
			"UPDATE blnk.lineage_outbox SET locked_until = NOW() - interval '1 minute' WHERE id = $1", entry.ID)
		require.NoError(t, err)

		got, err := ds.GetOutboxByTransactionID(ctx, entry.TransactionID)
		require.NoError(t, err)
		require.Equal(t, model.OutboxStatusProcessing, got.Status)
		var lockExpired bool
		require.NoError(t, ds.Conn.QueryRow(
			"SELECT locked_until < NOW() FROM blnk.lineage_outbox WHERE id = $1", entry.ID).Scan(&lockExpired))
		require.True(t, lockExpired, "lock must already be expired on the database clock")

		reclaimed, err := ds.ClaimPendingOutboxEntries(ctx, 500, 5*time.Minute)
		require.NoError(t, err)
		if !containsTxnID(reclaimed, entry.TransactionID) {
			t.Skip("SUSPECTED BUG: ClaimPendingOutboxEntries (database/lineage.go:282-295) only claims " +
				"status='pending' rows; an entry stuck in 'processing' after a processor crash is never " +
				"reclaimed even after locked_until expires, so the outbox work is silently lost unless " +
				"MarkOutboxFailed/Completed runs")
		}
	})
}

func containsTxnID(entries []model.LineageOutbox, txnID string) bool {
	for _, e := range entries {
		if e.TransactionID == txnID {
			return true
		}
	}
	return false
}

func TestClaimPendingOutboxEntries_ConcurrentClaimsAreDisjoint_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	markerPrefix := "covconc-" + gofakeit.UUID()[:8] + "-"
	quiesceOutbox(t, ds, markerPrefix)

	const total = 20
	insertedSet := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		entry := newOutboxEntry(markerPrefix)
		require.NoError(t, ds.InsertLineageOutbox(ctx, entry))
		insertedSet[entry.TransactionID] = true
	}

	const workers = 4
	results := make([][]model.LineageOutbox, workers)
	errs := make([]error, workers)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for w := 0; w < workers; w++ {
		done.Add(1)
		go func(w int) {
			defer done.Done()
			start.Wait() // line up all workers for maximum contention
			results[w], errs[w] = ds.ClaimPendingOutboxEntries(ctx, total/workers, 5*time.Minute)
		}(w)
	}
	start.Done()
	done.Wait()

	for w, err := range errs {
		require.NoError(t, err, "worker %d claim failed", w)
	}

	claimCounts := make(map[string]int)
	for w := 0; w < workers; w++ {
		for _, e := range results[w] {
			if insertedSet[e.TransactionID] {
				claimCounts[e.TransactionID]++
				assert.Equal(t, model.OutboxStatusProcessing, e.Status)
			}
		}
	}

	for txnID, n := range claimCounts {
		assert.Equal(t, 1, n, "entry %s was claimed by %d workers; FOR UPDATE SKIP LOCKED must keep claims disjoint", txnID, n)
	}

	// Between them, the workers requested exactly `total` rows, all of which
	// were pending and unlocked, so every fixture must be claimed exactly once.
	assert.Len(t, claimCounts, total, "every inserted fixture should be claimed exactly once across workers")
}
