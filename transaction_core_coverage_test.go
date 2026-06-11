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

package blnk

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCoreTestBlnk builds a Blnk instance against the real local Postgres and
// Redis, mirroring the integration setup used in transaction_test.go.
func newCoreTestBlnk(t *testing.T) (*Blnk, database.IDataSource) {
	t.Helper()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_core_cov",
			IndexQueue:       "index_queue_core_cov",
			TransactionQueue: "transaction_queue_core_cov",
			NumberOfQueues:   1,
		},
		Transaction: config.TransactionConfig{
			BatchSize:    100,
			MaxQueueSize: 1000,
			MaxWorkers:   2,
			LockDuration: 30 * time.Second,
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "failed to create datasource")
	b, err := NewBlnk(ds)
	require.NoError(t, err, "failed to create blnk instance")
	return b, ds
}

// fundedPair creates two USD balances and returns them. allowOverdraft on the
// recorded transactions funds the source implicitly where needed.
func newBalancePair(t *testing.T, ds database.IDataSource) (*model.Balance, *model.Balance) {
	t.Helper()
	src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	return &src, &dst
}

func recordAppliedTxn(t *testing.T, b *Blnk, src, dst string, amount float64) *model.Transaction {
	t.Helper()
	draft := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      "ref_" + gofakeit.UUID(),
		Source:         src,
		Destination:    dst,
		Amount:         amount,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: true,
		SkipQueue:      true,
		Status:         StatusApplied,
	}
	// Direct RecordTransaction (unlike the queue flow) neither assigns a
	// TransactionID nor applies precision twice: the ID must be preset, and
	// priming PreciseAmount makes the in-flow ApplyPrecision take the
	// precise->decimal branch that fills AmountString.
	model.ApplyPrecision(draft)
	txn, err := b.RecordTransaction(context.Background(), draft)
	require.NoError(t, err)
	require.Equal(t, StatusApplied, txn.Status)
	return txn
}

// --- refunds -----------------------------------------------------------------

func TestRefundTransaction_RestoresSourceBalance(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	txn := recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, 100)

	refund, err := b.RefundTransaction(context.Background(), txn.TransactionID, true)
	require.NoError(t, err)
	assert.Equal(t, txn.TransactionID, refund.ParentTransaction, "refund must reference the original")
	assert.Equal(t, StatusApplied, refund.Status)

	// Money conservation: after refund the source must be back to zero and
	// the destination back to zero — verify actual numbers, not statuses.
	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	dstAfter, err := ds.GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.Balance.String(), "source balance must be restored after refund")
	assert.Equal(t, "0", dstAfter.Balance.String(), "destination balance must be drained after refund")
}

func TestRefundTransaction_NonexistentFails(t *testing.T) {
	b, _ := newCoreTestBlnk(t)
	_, err := b.RefundTransaction(context.Background(), "txn_does_not_exist_"+gofakeit.UUID(), true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRefundTransaction_DoubleRefund(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	txn := recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, 50)

	_, err := b.RefundTransaction(context.Background(), txn.TransactionID, true)
	require.NoError(t, err)

	// A second refund of the same transaction must not move money again.
	_, err = b.RefundTransaction(context.Background(), txn.TransactionID, true)
	require.Error(t, err, "double refund must be rejected")

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.Balance.String(),
		"source must hold exactly the original amount after one refund; a double refund must not double-credit")
}

func TestRefundTransaction_RejectedTransactionFails(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	rejected, err := b.RecordTransaction(context.Background(), &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "ref_" + gofakeit.UUID(),
		Source:        src.BalanceID,
		Destination:   dst.BalanceID,
		Amount:        10,
		Precision:     100,
		Currency:      "USD",
		SkipQueue:     true,
	})
	// Without overdraft and with a zero source balance this is rejected.
	if err == nil {
		t.Skipf("expected insufficient-funds rejection, got status %s", rejected.Status)
	}

	// Record a rejection row explicitly and try to refund it.
	queued := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "ref_" + gofakeit.UUID(),
		Source:        src.BalanceID,
		Destination:   dst.BalanceID,
		Amount:        10,
		Precision:     100,
		Currency:      "USD",
		CreatedAt:     time.Now(),
	}
	// Two passes: the first fills PreciseAmount, the second takes the
	// precise->decimal branch that fills AmountString (RejectTransaction
	// persists directly without the queue flow's metadata preparation).
	model.ApplyPrecision(queued)
	model.ApplyPrecision(queued)
	rej, err := b.RejectTransaction(context.Background(), queued, "insufficient funds")
	require.NoError(t, err)

	_, err = b.RefundTransaction(context.Background(), rej.TransactionID, true)
	require.Error(t, err, "refunding a REJECTED transaction must fail")
}

// TestRefundTransaction_DeterministicReference pins the refund reference to a
// stable "<original>_refund" value so the DB unique-reference constraint can act
// as the idempotency backstop against double refunds.
func TestRefundTransaction_DeterministicReference(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	txn := recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, 75)

	refund, err := b.RefundTransaction(context.Background(), txn.TransactionID, true)
	require.NoError(t, err)
	assert.Equal(t, txn.TransactionID+"_refund", refund.Reference,
		"refund reference must be deterministic for idempotency")
}

// TestRefundTransaction_ConcurrentSingleWinner fires several concurrent refunds
// of the same applied transaction. The redlock + already-refunded check must let
// exactly one succeed; the rest must fail without double-crediting the source.
func TestRefundTransaction_ConcurrentSingleWinner(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	txn := recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, 120)

	const racers = 5
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successes int
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			if _, err := b.RefundTransaction(context.Background(), txn.TransactionID, true); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, successes, "exactly one concurrent refund may succeed")

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.Balance.String(),
		"a single refund must restore the source exactly; concurrent refunds must not double-credit")
}

// TestRefundTransaction_RejectsNonRefundableStatus asserts the status gate blocks
// refunds of transactions that never settled (QUEUED/INFLIGHT/SCHEDULED).
func TestRefundTransaction_RejectsNonRefundableStatus(t *testing.T) {
	for _, status := range []string{StatusQueued, StatusInflight, StatusScheduled} {
		t.Run(status, func(t *testing.T) {
			b, ds := newCoreTestBlnk(t)
			src, dst := newBalancePair(t, ds)

			draft := &model.Transaction{
				TransactionID: model.GenerateUUIDWithSuffix("txn"),
				Reference:     "ref_" + gofakeit.UUID(),
				Source:        src.BalanceID,
				Destination:   dst.BalanceID,
				Amount:        25,
				Precision:     100,
				Currency:      "USD",
				Status:        status,
				CreatedAt:     time.Now(),
			}
			model.ApplyPrecision(draft)
			model.ApplyPrecision(draft)
			persisted, err := ds.RecordTransaction(context.Background(), draft)
			require.NoError(t, err)

			_, err = b.RefundTransaction(context.Background(), persisted.TransactionID, true)
			require.Error(t, err, "refunding a %s transaction must fail", status)
			assert.Contains(t, err.Error(), "cannot be refunded")
		})
	}
}

// --- inflight void / commit balance integrity ---------------------------------

// TestCommitInflight_CreditEqualsDebitNoResidue locks in the removal of the
// per-transaction FX rate: on an inflight hold the destination's inflight
// credit equals the source's inflight debit, and committing moves exactly
// that amount to the real balance with nothing stranded in inflight credit.
// Before rate was removed, the hold credited the rated amount but the commit
// moved the un-rated amount, leaving a residue forever.
func TestCommitInflight_CreditEqualsDebitNoResidue(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	draft := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      "ref_" + gofakeit.UUID(),
		Source:         src.BalanceID,
		Destination:    dst.BalanceID,
		Amount:         100,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: true,
		SkipQueue:      true,
		Inflight:       true,
		Status:         StatusInflight,
	}
	model.ApplyPrecision(draft)
	inflight, err := b.RecordTransaction(context.Background(), draft)
	require.NoError(t, err)
	require.Equal(t, StatusInflight, inflight.Status)

	// On hold: destination inflight credit must equal source inflight debit.
	srcHold, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	dstHold, err := ds.GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	require.Equal(t, "10000", srcHold.InflightDebitBalance.String())
	require.Equal(t, srcHold.InflightDebitBalance.String(), dstHold.InflightCreditBalance.String(),
		"inflight credit must equal inflight debit (no rate)")

	// Commit the full amount.
	_, err = b.CommitInflightTransaction(context.Background(), inflight.TransactionID, big.NewInt(0))
	require.NoError(t, err)

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	dstAfter, err := ds.GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)

	// Destination credited exactly the debited amount; zero residue anywhere.
	assert.Equal(t, srcAfter.DebitBalance.String(), dstAfter.CreditBalance.String(),
		"destination credit must equal source debit on commit")
	assert.Equal(t, "10000", dstAfter.CreditBalance.String())
	assert.Equal(t, "0", dstAfter.InflightCreditBalance.String(),
		"no amount may be stranded in inflight credit after commit")
	assert.Equal(t, "0", srcAfter.InflightDebitBalance.String(),
		"no amount may be stranded in inflight debit after commit")
}

func TestVoidInflight_ReleasesInflightBalance(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	inflightDraft := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      "ref_" + gofakeit.UUID(),
		Source:         src.BalanceID,
		Destination:    dst.BalanceID,
		Amount:         75,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: true,
		SkipQueue:      true,
		Inflight:       true,
		Status:         StatusInflight,
	}
	model.ApplyPrecision(inflightDraft)
	inflight, err := b.RecordTransaction(context.Background(), inflightDraft)
	require.NoError(t, err)
	require.Equal(t, StatusInflight, inflight.Status)

	voided, err := b.VoidInflightTransaction(context.Background(), inflight.TransactionID)
	require.NoError(t, err)
	assert.Equal(t, StatusVoid, voided.Status)

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.InflightDebitBalance.String(), "void must release the inflight debit")
	assert.Equal(t, "0", srcAfter.Balance.String(), "void must not touch the committed balance")
}

func TestCommitInflight_PartialOverCommitRejected(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	commitDraft := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      "ref_" + gofakeit.UUID(),
		Source:         src.BalanceID,
		Destination:    dst.BalanceID,
		Amount:         100,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: true,
		SkipQueue:      true,
		Inflight:       true,
		Status:         StatusInflight,
	}
	model.ApplyPrecision(commitDraft)
	inflight, err := b.RecordTransaction(context.Background(), commitDraft)
	require.NoError(t, err)

	// Committing more than the inflight amount must fail.
	_, err = b.CommitInflightTransaction(context.Background(), inflight.TransactionID, big.NewInt(20000))
	require.Error(t, err, "over-commit beyond the original amount must be rejected")

	// Commit exactly half, then the rest; a further commit must fail.
	_, err = b.CommitInflightTransaction(context.Background(), inflight.TransactionID, big.NewInt(5000))
	require.NoError(t, err)
	_, err = b.CommitInflightTransaction(context.Background(), inflight.TransactionID, big.NewInt(5000))
	require.NoError(t, err)
	_, err = b.CommitInflightTransaction(context.Background(), inflight.TransactionID, big.NewInt(1))
	require.Error(t, err, "commit after the full amount is settled must be rejected")
}

// --- bulk rollback paths -------------------------------------------------------

func TestRollbackBatch_RefundsNonInflightBatch(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	batchID := model.GenerateUUIDWithSuffix("bulk")

	txns := []*model.Transaction{
		{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: 40, Precision: 100, Currency: "USD", AllowOverdraft: true},
		{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: 60, Precision: 100, Currency: "USD", AllowOverdraft: true},
	}
	require.NoError(t, b.processBulkTransactions(context.Background(), txns, batchID, false, true))

	action, err := b.rollbackBatchTransactions(context.Background(), batchID, false)
	require.NoError(t, err)
	assert.Equal(t, "refunded", action)

	// Batch refunds are enqueued (SkipQueue is not persisted, so the worker
	// path settles them). Every original transaction must gain a queued
	// refund child with source/destination swapped; the queued row is
	// persisted asynchronously, so poll briefly.
	for _, original := range txns {
		var children []*model.Transaction
		deadline := time.Now().Add(10 * time.Second)
		for {
			var err error
			children, err = ds.GetTransactionsByParent(context.Background(), original.TransactionID, 10, 0)
			require.NoError(t, err)
			if len(children) > 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		require.Len(t, children, 1, "each batch transaction must get exactly one refund child")
		refund := children[0]
		assert.Equal(t, StatusQueued, refund.Status, "rollback refunds are queued for the worker")
		assert.Equal(t, original.Destination, refund.Source, "refund must swap source and destination")
		assert.Equal(t, original.Source, refund.Destination, "refund must swap source and destination")
	}
}

func TestRollbackBatch_VoidsInflightBatch(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	batchID := model.GenerateUUIDWithSuffix("bulk")

	txns := []*model.Transaction{
		{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: 25, Precision: 100, Currency: "USD", AllowOverdraft: true},
		{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: 35, Precision: 100, Currency: "USD", AllowOverdraft: true},
	}
	require.NoError(t, b.processBulkTransactions(context.Background(), txns, batchID, true, true))

	action, err := b.rollbackBatchTransactions(context.Background(), batchID, true)
	require.NoError(t, err)
	assert.Equal(t, "voided", action)

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.InflightDebitBalance.String(), "all inflight debits must be released by the void rollback")
	assert.Equal(t, "0", srcAfter.Balance.String(), "committed balance must be untouched by an inflight batch rollback")
}

// --- queued execution path -----------------------------------------------------

func TestProcessQueuedTransaction_AppliesAndIsIdempotent(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	ref := "ref_" + gofakeit.UUID()
	queueCopy := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      ref,
		Source:         src.BalanceID,
		Destination:    dst.BalanceID,
		Amount:         80,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: true,
		Status:         StatusQueued,
		CreatedAt:      time.Now(),
	}
	model.ApplyPrecision(queueCopy)

	applied, err := b.ProcessQueuedTransaction(context.Background(), queueCopy, false)
	require.NoError(t, err)
	assert.Equal(t, StatusApplied, applied.Status)

	dstAfter, err := ds.GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "8000", dstAfter.Balance.String())

	// Replaying the same queue copy (same reference) must not double-apply.
	replay := *queueCopy
	replay.TransactionID = model.GenerateUUIDWithSuffix("txn")
	_, err = b.ProcessQueuedTransaction(context.Background(), &replay, false)
	require.Error(t, err, "replaying the same reference must be rejected")
	assert.True(t, IsDuplicateReferenceError(err), "replay rejection must be a duplicate-reference error, got: %v", err)

	dstFinal, err := ds.GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "8000", dstFinal.Balance.String(), "balance must be unchanged after a duplicate replay")
}
