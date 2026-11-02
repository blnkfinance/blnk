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
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/redis/go-redis/v9"
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
	// TransactionID nor applies precision: the ID must be preset. One
	// ApplyPrecision pass fills PreciseAmount and AmountString.
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
	// ApplyPrecision fills PreciseAmount and AmountString in one pass.
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
	assert.Equal(t, model.RefundReference(txn.TransactionID), refund.Reference,
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

	action, count, err := b.rollbackBatchTransactions(context.Background(), batchID, false)
	require.NoError(t, err)
	assert.Equal(t, "refunded", action)
	assert.Equal(t, 2, count, "both batch transactions must be counted as refunded")

	// Batch refunds are applied before the rollback returns, so the caller
	// is never told about a refund that has not happened yet. Every original
	// transaction must already have one applied refund child with
	// source/destination swapped.
	for _, original := range txns {
		children, err := ds.GetTransactionsByParent(context.Background(), original.TransactionID, 10, 0)
		require.NoError(t, err)
		require.Len(t, children, 1, "each batch transaction must get exactly one refund child")
		refund := children[0]
		assert.Equal(t, StatusApplied, refund.Status, "rollback refunds are applied before the rollback returns")
		assert.Equal(t, model.RefundReference(original.TransactionID), refund.Reference)
		assert.Equal(t, original.Destination, refund.Source, "refund must swap source and destination")
		assert.Equal(t, original.Source, refund.Destination, "refund must swap source and destination")
	}

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	dstAfter, err := ds.GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.Balance.String(), "the source must be back where it started")
	assert.Equal(t, "0", dstAfter.Balance.String(), "the destination must be back where it started")

	// A second pass finds both transactions refunded and refunds nothing,
	// the refunds included.
	_, count, err = b.rollbackBatchTransactions(context.Background(), batchID, false)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	srcAfter, err = ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.Balance.String(), "a second rollback pass must not move money")
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

	action, count, err := b.rollbackBatchTransactions(context.Background(), batchID, true)
	require.NoError(t, err)
	assert.Equal(t, "voided", action)
	assert.Equal(t, 2, count, "both inflight transactions must be counted as voided")

	srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", srcAfter.InflightDebitBalance.String(), "all inflight debits must be released by the void rollback")
	assert.Equal(t, "0", srcAfter.Balance.String(), "committed balance must be untouched by an inflight batch rollback")
}

// An atomic skip_queue batch that fails must report its refunds only once they
// are applied: when CreateBulkTransactions returns, the balances are back and
// the message says how many transactions were refunded, or that nothing had
// been applied.
func TestCreateBulkTransactions_SkipQueueAtomicReportsAppliedRefunds(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		amounts []float64
		want    string
		applied int
	}{
		{name: "50 then 500", amounts: []float64{50, 500}, want: "1 applied transaction(s) from this batch have been refunded", applied: 1},
		{name: "500 then 50", amounts: []float64{500, 50}, want: "Nothing from this batch had been applied, so nothing was refunded", applied: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			funding, src := newBalancePair(t, ds)
			recordAppliedTxn(t, b, funding.BalanceID, src.BalanceID, 100)

			var items []*model.Transaction
			var dsts []string
			for _, amount := range tc.amounts {
				dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
				require.NoError(t, err)
				dsts = append(dsts, dst.BalanceID)
				items = append(items, &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: src.BalanceID, Destination: dst.BalanceID, Amount: amount, Precision: 100, Currency: "USD"})
			}

			result, err := b.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, SkipQueue: true, Transactions: items})
			require.Error(t, err)
			assert.Equal(t, "failed", result.Status)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, result.Error, tc.want)

			// No waiting: the refund has been applied by the time the caller
			// is told about it.
			srcAfter, err := ds.GetBalanceByIDLite(src.BalanceID)
			require.NoError(t, err)
			assert.Equal(t, "10000", srcAfter.Balance.String(), "the source must be back at 100 when the call returns")
			for i, dst := range dsts {
				dstAfter, err := ds.GetBalanceByIDLite(dst)
				require.NoError(t, err)
				assert.Equal(t, "0", dstAfter.Balance.String(), "item %d must not stay applied", i)
			}

			applied, err := ds.GetTransactionsByParent(ctx, result.BatchID, 10, 0)
			require.NoError(t, err)
			require.Len(t, applied, tc.applied)
			for _, item := range applied {
				refunds, err := ds.GetTransactionsByParent(ctx, item.TransactionID, 10, 0)
				require.NoError(t, err)
				require.Len(t, refunds, 1, "the applied item must have one refund")
				assert.Equal(t, StatusApplied, refunds[0].Status, "the refund must be applied, not queued")
				assert.Equal(t, model.RefundReference(item.TransactionID), refunds[0].Reference)
			}
		})
	}
}

// failingMarkHook makes the Redis write of the failed-batch mark fail while it
// is on.
type failingMarkHook struct{ on atomic.Bool }

func (h *failingMarkHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failingMarkHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if h.on.Load() && len(args) > 1 && strings.HasPrefix(fmt.Sprint(args[1]), "atomic-batch-failed:") && cmd.Name() == "set" {
			err := errors.New("injected: redis unavailable")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *failingMarkHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// When the mark that fails a queued atomic batch cannot be saved, the refused
// item must not be recorded and nothing may be rolled back yet: the worker
// retries the item, and the retry fails the batch properly.
func TestRejectTransaction_AtomicBatchItemWaitsForTheMark(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	ctx := context.Background()

	funding, src := newBalancePair(t, ds)
	recordAppliedTxn(t, b, funding.BalanceID, src.BalanceID, 100)
	dst, refusedDst := newBalancePair(t, ds)
	batchID := model.GenerateUUIDWithSuffix("bulk")
	balanceOf := func(id string) string {
		balance, err := ds.GetBalanceByIDLite(id)
		require.NoError(t, err)
		return balance.Balance.String()
	}

	// The batch's 50, as the worker applied it.
	paid := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "ref_" + gofakeit.UUID() + "_q",
		Source:        src.BalanceID,
		Destination:   dst.BalanceID,
		Amount:        50,
		Precision:     100,
		Currency:      "USD",
		SkipQueue:     true,
		Status:        StatusApplied,
		MetaData:      map[string]interface{}{"QUEUED_PARENT_TRANSACTION": batchID, "atomic": true},
	}
	model.ApplyPrecision(paid)
	_, err := b.RecordTransaction(ctx, paid)
	require.NoError(t, err)
	require.Equal(t, "5000", balanceOf(src.BalanceID), "the 50 must have left the source")

	// The batch's 500, as the worker hands it over after the refusal. Each
	// attempt gets a fresh copy, the way each retry unmarshals the task.
	refusedID, refusedRef := model.GenerateUUIDWithSuffix("txn"), "ref_"+gofakeit.UUID()+"_q"
	refused := func() *model.Transaction {
		txn := &model.Transaction{
			TransactionID: refusedID,
			Reference:     refusedRef,
			Source:        src.BalanceID,
			Destination:   refusedDst.BalanceID,
			Amount:        500,
			Precision:     100,
			Currency:      "USD",
			Atomic:        true,
			Status:        StatusQueued,
			CreatedAt:     time.Now(),
			MetaData:      map[string]interface{}{"QUEUED_PARENT_TRANSACTION": batchID, "atomic": true},
		}
		model.ApplyPrecision(txn)
		return txn
	}

	hook := &failingMarkHook{}
	hook.on.Store(true)
	b.redis.AddHook(hook)
	t.Cleanup(func() { hook.on.Store(false) })

	_, err = b.RejectTransaction(ctx, refused(), "insufficient funds")
	require.Error(t, err, "the item must not be rejected while its batch cannot be marked as failed")
	recorded, err := ds.TransactionExistsByRef(ctx, refusedRef)
	require.NoError(t, err)
	assert.False(t, recorded, "nothing may be recorded for the item, so the worker can retry it")
	assert.Equal(t, "5000", balanceOf(src.BalanceID), "nothing may be rolled back before the batch is marked")

	// Redis is back; the retry marks the batch and rolls it back.
	hook.on.Store(false)
	rejected, err := b.RejectTransaction(ctx, refused(), "insufficient funds")
	require.NoError(t, err)
	assert.Equal(t, StatusRejected, rejected.Status)
	assert.Equal(t, "10000", balanceOf(src.BalanceID), "the source must be back at 100")
	assert.Equal(t, "0", balanceOf(dst.BalanceID), "the 50 must be refunded")
	assert.Equal(t, "0", balanceOf(refusedDst.BalanceID))
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

// --- zero-amount viaQueue semantics ------------------------------------------

// TestRecordTransaction_DirectZeroAmountIsDiscardedNotRejected locks in the issue #142/#334
// semantics: a zero-amount transaction reaching RecordTransaction directly (e.g. a split leg
// whose share rounds down to zero) must still be discarded silently, not persisted as a
// REJECTED record. Only queue-originated processing (see the sibling test below) needs to
// persist a REJECTED child, because only a QUEUED row can get stuck in recovery.
func TestRecordTransaction_DirectZeroAmountIsDiscardedNotRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	ctx := context.Background()

	leg := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "split_leg_zero_" + model.GenerateUUIDWithSuffix("ref"),
		Source:        src.BalanceID,
		Destination:   dst.BalanceID,
		Amount:        0,
		PreciseAmount: big.NewInt(0),
		Precision:     100,
		Currency:      "USD",
		Status:        StatusApplied,
		SkipQueue:     true,
	}

	result, err := b.RecordTransaction(ctx, leg)
	require.NoError(t, err, "a zero-amount direct leg must be discarded without error, not rejected")
	assert.Equal(t, StatusApplied, result.Status, "the discarded transaction keeps its original status, it is not flipped to REJECTED")

	children, err := ds.GetTransactionsByParent(ctx, leg.TransactionID, 10, 0)
	require.NoError(t, err)
	assert.Empty(t, children, "a discarded zero-amount leg must not persist a REJECTED child")
}

// TestProcessQueuedTransaction_ZeroAmountIsRejectedAndPersisted verifies the queue-originated
// counterpart: a zero-amount transaction processed via the queue worker must persist a
// REJECTED child so the parent gets a terminal child and never becomes a stuck-queue
// recovery candidate.
func TestProcessQueuedTransaction_ZeroAmountIsRejectedAndPersisted(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	ctx := context.Background()

	parent := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "queued_zero_" + model.GenerateUUIDWithSuffix("ref"),
		Source:        src.BalanceID,
		Destination:   dst.BalanceID,
		Amount:        0,
		AmountString:  "0",
		PreciseAmount: big.NewInt(0),
		Precision:     100,
		Currency:      "USD",
		Status:        StatusQueued,
		MetaData:      map[string]interface{}{},
	}

	_, err := ds.RecordTransaction(ctx, parent)
	require.NoError(t, err, "parent QUEUED row must persist with amount 0")

	queueCopy := createQueueCopy(parent, parent.Reference)
	result, err := b.ProcessQueuedTransaction(ctx, queueCopy, false)
	require.NoError(t, err)
	assert.Equal(t, StatusRejected, result.Status)

	children, err := ds.GetTransactionsByParent(ctx, parent.TransactionID, 10, 0)
	require.NoError(t, err)
	require.Len(t, children, 1, "queue processing of a zero-amount row must write a REJECTED child")
	assert.Equal(t, StatusRejected, children[0].Status)
}
