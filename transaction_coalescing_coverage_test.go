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
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// eligibleQueuedTxn returns a transaction that passes every coalescing
// eligibility gate; tests mutate single fields off this baseline.
func eligibleQueuedTxn() *model.Transaction {
	return &model.Transaction{
		TransactionID:     gofakeit.UUID(),
		ParentTransaction: gofakeit.UUID(),
		Source:            "bln_source",
		Destination:       "bln_destination",
		Currency:          "USD",
		Status:            StatusQueued,
	}
}

func TestCanCoalesceQueuedTransaction_Coverage(t *testing.T) {
	l := &Blnk{}

	tests := []struct {
		name       string
		mutate     func(txn *model.Transaction) *model.Transaction
		wantOK     bool
		wantReason string
	}{
		{
			name:   "fully eligible queued transaction",
			mutate: func(txn *model.Transaction) *model.Transaction { return txn },
			wantOK: true,
		},
		{
			name:       "nil transaction",
			mutate:     func(txn *model.Transaction) *model.Transaction { return nil },
			wantReason: "nil_transaction",
		},
		{
			name: "missing parent transaction",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.ParentTransaction = ""
				return txn
			},
			wantReason: "missing_parent_transaction",
		},
		{
			name: "applied transaction not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Status = StatusApplied
				return txn
			},
			wantReason: "status_not_queued",
		},
		{
			name: "inflight status not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Status = StatusInflight
				return txn
			},
			wantReason: "status_not_queued",
		},
		{
			name: "atomic transaction not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Atomic = true
				return txn
			},
			wantReason: "atomic_transaction",
		},
		{
			name: "skip queue transaction not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.SkipQueue = true
				return txn
			},
			wantReason: "skip_queue_enabled",
		},
		{
			name: "atomic wins over skip queue in reason",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Atomic = true
				txn.SkipQueue = true
				return txn
			},
			wantReason: "atomic_transaction",
		},
		{
			name: "multi-source split transaction not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Sources = []model.Distribution{{Identifier: "a", Distribution: "left"}}
				return txn
			},
			wantReason: "split_transaction",
		},
		{
			name: "multi-destination split transaction not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Destinations = []model.Distribution{{Identifier: "a", Distribution: "left"}}
				return txn
			},
			wantReason: "split_transaction",
		},
		{
			name: "scheduled transaction not coalescable",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.ScheduledFor = time.Now().Add(time.Hour)
				return txn
			},
			wantReason: "scheduled_transaction",
		},
		{
			name: "missing source",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Source = ""
				return txn
			},
			wantReason: "missing_pair_or_currency",
		},
		{
			name: "missing destination",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Destination = ""
				return txn
			},
			wantReason: "missing_pair_or_currency",
		},
		{
			name: "missing currency",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Currency = ""
				return txn
			},
			wantReason: "missing_pair_or_currency",
		},
		{
			name: "inflight flag alone does not block coalescing",
			mutate: func(txn *model.Transaction) *model.Transaction {
				txn.Inflight = true
				return txn
			},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, reason := l.canCoalesceQueuedTransaction(tt.mutate(eligibleQueuedTxn()))
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

func TestCanCoalesceQueuedOriginal_Coverage(t *testing.T) {
	l := &Blnk{}

	t.Run("originals do not require a parent transaction", func(t *testing.T) {
		txn := eligibleQueuedTxn()
		txn.ParentTransaction = ""
		ok, reason := l.canCoalesceQueuedOriginal(txn)
		assert.True(t, ok)
		assert.Empty(t, reason)
	})

	t.Run("nil transaction", func(t *testing.T) {
		ok, reason := l.canCoalesceQueuedOriginal(nil)
		assert.False(t, ok)
		assert.Equal(t, "nil_transaction", reason)
	})

	t.Run("status gate matches leader gate", func(t *testing.T) {
		txn := eligibleQueuedTxn()
		txn.Status = StatusApplied
		ok, reason := l.canCoalesceQueuedOriginal(txn)
		assert.False(t, ok)
		assert.Equal(t, "status_not_queued", reason)
	})

	t.Run("flag restoration from metadata flips eligibility", func(t *testing.T) {
		// Siblings come from the DB where Atomic/SkipQueue live in metadata;
		// after restoreTransactionFlagsFromMetadata an atomic sibling must be
		// rejected even though the column-level flag was false.
		txn := eligibleQueuedTxn()
		txn.MetaData = map[string]interface{}{"atomic": true}
		restoreTransactionFlagsFromMetadata(txn)
		ok, reason := l.canCoalesceQueuedOriginal(txn)
		assert.False(t, ok)
		assert.Equal(t, "atomic_transaction", reason)
	})

	t.Run("scheduled sibling rejected", func(t *testing.T) {
		txn := eligibleQueuedTxn()
		txn.ScheduledFor = time.Now().Add(time.Minute)
		ok, reason := l.canCoalesceQueuedOriginal(txn)
		assert.False(t, ok)
		assert.Equal(t, "scheduled_transaction", reason)
	})

	t.Run("split sibling rejected", func(t *testing.T) {
		txn := eligibleQueuedTxn()
		txn.Sources = []model.Distribution{{Identifier: "x", Distribution: "100%"}}
		ok, reason := l.canCoalesceQueuedOriginal(txn)
		assert.False(t, ok)
		assert.Equal(t, "split_transaction", reason)
	})
}

func TestValidateQueuedBatchCurrencies_Coverage(t *testing.T) {
	mk := func(currencies ...string) []*model.Transaction {
		txns := make([]*model.Transaction, 0, len(currencies))
		for _, c := range currencies {
			txns = append(txns, &model.Transaction{Currency: c})
		}
		return txns
	}

	t.Run("empty batch valid", func(t *testing.T) {
		assert.NoError(t, validateQueuedBatchCurrencies(nil))
		assert.NoError(t, validateQueuedBatchCurrencies([]*model.Transaction{}))
	})

	t.Run("single transaction valid", func(t *testing.T) {
		assert.NoError(t, validateQueuedBatchCurrencies(mk("USD")))
	})

	t.Run("homogeneous batch valid", func(t *testing.T) {
		assert.NoError(t, validateQueuedBatchCurrencies(mk("USD", "USD", "USD")))
	})

	t.Run("currency mismatch rejected", func(t *testing.T) {
		err := validateQueuedBatchCurrencies(mk("USD", "USD", "EUR"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multiple currencies")
	})

	t.Run("mismatch in second position rejected", func(t *testing.T) {
		err := validateQueuedBatchCurrencies(mk("NGN", "USD"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multiple currencies")
	})

	t.Run("case-sensitive currency comparison", func(t *testing.T) {
		// "usd" vs "USD" are treated as different currencies; mixing them in
		// one batch must be rejected (no silent case folding of money types).
		err := validateQueuedBatchCurrencies(mk("USD", "usd"))
		assert.Error(t, err)
	})
}

func TestQueuedBatchReferenceSets_Coverage(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled batch reference check skips DB entirely", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		l := &Blnk{
			datasource: ds,
			config: &config.Configuration{
				Transaction: config.TransactionConfig{DisableBatchReferenceCheck: true},
			},
		}
		prefetched, existing, err := l.queuedBatchReferenceSets(ctx, []*model.Transaction{
			{Reference: "ref_a"},
		})
		require.NoError(t, err)
		assert.Empty(t, prefetched)
		assert.Empty(t, existing)
		ds.AssertNotCalled(t, "GetExistingTransactionReferences", mock.Anything, mock.Anything)
	})

	t.Run("dedupes references and skips nil and empty entries", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		ds.On("GetExistingTransactionReferences", mock.Anything, []string{"ref_a", "ref_b"}).
			Return(map[string]struct{}{"ref_b": {}}, nil).Once()

		l := &Blnk{
			datasource: ds,
			config:     &config.Configuration{},
		}
		prefetched, existing, err := l.queuedBatchReferenceSets(ctx, []*model.Transaction{
			{Reference: "ref_a"},
			nil,
			{Reference: ""},
			{Reference: "ref_a"}, // duplicate must be sent to the DB only once
			{Reference: "ref_b"},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"ref_a": {}, "ref_b": {}}, prefetched)
		assert.Equal(t, map[string]struct{}{"ref_b": {}}, existing)
		ds.AssertExpectations(t)
	})

	t.Run("propagates lookup errors", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		ds.On("GetExistingTransactionReferences", mock.Anything, mock.Anything).
			Return(nil, assert.AnError).Once()

		l := &Blnk{datasource: ds, config: &config.Configuration{}}
		_, _, err := l.queuedBatchReferenceSets(ctx, []*model.Transaction{{Reference: "ref_x"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to validate coalesced transaction references")
	})
}

func TestCoalescingBalancesForTransaction_Coverage(t *testing.T) {
	src := &model.Balance{BalanceID: "bln_src"}
	dst := &model.Balance{BalanceID: "bln_dst"}
	balances := map[string]*model.Balance{"bln_src": src, "bln_dst": dst}

	t.Run("resolves both balances", func(t *testing.T) {
		s, d, err := coalescingBalancesForTransaction(balances, &model.Transaction{Source: "bln_src", Destination: "bln_dst"})
		require.NoError(t, err)
		assert.Same(t, src, s)
		assert.Same(t, dst, d)
	})

	t.Run("missing source balance", func(t *testing.T) {
		_, _, err := coalescingBalancesForTransaction(balances, &model.Transaction{Source: "bln_other", Destination: "bln_dst"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing source balance bln_other")
	})

	t.Run("missing destination balance", func(t *testing.T) {
		_, _, err := coalescingBalancesForTransaction(balances, &model.Transaction{Source: "bln_src", Destination: "bln_other"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing destination balance bln_other")
	})

	t.Run("nil balance entry treated as missing", func(t *testing.T) {
		withNil := map[string]*model.Balance{"bln_src": nil, "bln_dst": dst}
		_, _, err := coalescingBalancesForTransaction(withNil, &model.Transaction{Source: "bln_src", Destination: "bln_dst"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing source balance")
	})
}

func TestQueuedCoalescingBatchSize_Coverage(t *testing.T) {
	mkBlnk := func(enable bool, size int) *Blnk {
		return &Blnk{config: &config.Configuration{
			Transaction: config.TransactionConfig{EnableCoalescing: enable, BatchSize: size},
		}}
	}

	t.Run("disabled and not forced yields zero", func(t *testing.T) {
		assert.Equal(t, 0, mkBlnk(false, 100).queuedCoalescingBatchSize(false))
	})

	t.Run("disabled but forced (hot lane) uses configured size", func(t *testing.T) {
		assert.Equal(t, 100, mkBlnk(false, 100).queuedCoalescingBatchSize(true))
	})

	t.Run("size capped at max", func(t *testing.T) {
		assert.Equal(t, maxQueuedCoalescingBatchSize, mkBlnk(true, maxQueuedCoalescingBatchSize+1).queuedCoalescingBatchSize(false))
	})

	t.Run("size of one passes through (caller skips batching)", func(t *testing.T) {
		assert.Equal(t, 1, mkBlnk(true, 1).queuedCoalescingBatchSize(false))
	})

	t.Run("zero size passes through", func(t *testing.T) {
		assert.Equal(t, 0, mkBlnk(true, 0).queuedCoalescingBatchSize(false))
	})
}

func TestCollectQueuedCoalescingBalanceIDs_Coverage(t *testing.T) {
	ids := collectQueuedCoalescingBalanceIDs([]*model.Transaction{
		{Source: "bln_b", Destination: "bln_a"},
		nil,
		{Source: "bln_a", Destination: "bln_c"},
		{Source: "", Destination: "bln_b"},
	})
	// Deduplicated and lexicographically sorted for deterministic lock order.
	assert.Equal(t, []string{"bln_a", "bln_b", "bln_c"}, ids)
}

func TestPrepareQueuedBatchTransaction_Coverage(t *testing.T) {
	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")
	defer span.End()

	newBalance := func(id string) *model.Balance {
		b := &model.Balance{BalanceID: id, Currency: "USD"}
		b.InitializeBalanceFields()
		return b
	}

	mkBlnk := func(ds *mocks.MockDataSource) *Blnk {
		return &Blnk{
			datasource: ds,
			config:     &config.Configuration{},
		}
	}

	t.Run("missing balance fails before any balance mutation", func(t *testing.T) {
		l := mkBlnk(&mocks.MockDataSource{})
		txn := eligibleQueuedTxn()
		txn.Reference = gofakeit.UUID()
		_, err := l.prepareQueuedBatchTransaction(ctx, span, txn,
			map[string]*model.Balance{}, nil,
			map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing source balance")
	})

	t.Run("zero amount transaction rejected in batch mode", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		ds.On("TransactionExistsByRef", mock.Anything, mock.Anything).Return(false, nil)
		l := mkBlnk(ds)

		txn := eligibleQueuedTxn()
		txn.Reference = gofakeit.UUID()
		txn.Amount = 0
		txn.Precision = 100
		txn.AllowOverdraft = true

		src := newBalance(txn.Source)
		dst := newBalance(txn.Destination)
		_, err := l.prepareQueuedBatchTransaction(ctx, span, txn,
			map[string]*model.Balance{txn.Source: src, txn.Destination: dst}, nil,
			map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be positive")
	})

	t.Run("insufficient funds without overdraft fails the batch item", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		ds.On("TransactionExistsByRef", mock.Anything, mock.Anything).Return(false, nil)
		l := mkBlnk(ds)

		txn := eligibleQueuedTxn()
		txn.Reference = gofakeit.UUID()
		txn.Amount = 10
		txn.Precision = 100
		txn.AllowOverdraft = false

		src := newBalance(txn.Source)
		dst := newBalance(txn.Destination)
		_, err := l.prepareQueuedBatchTransaction(ctx, span, txn,
			map[string]*model.Balance{txn.Source: src, txn.Destination: dst}, nil,
			map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient funds")
		// The shared in-memory balances must not retain a partial application.
		assert.Equal(t, "0", src.Balance.String())
		assert.Equal(t, "0", dst.Balance.String())
	})

	t.Run("duplicate reference inside one batch rejected on second occurrence", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		ds.On("TransactionExistsByRef", mock.Anything, mock.Anything).Return(false, nil)
		l := mkBlnk(ds)

		sharedRef := gofakeit.UUID()
		batchRefs := map[string]struct{}{}

		first := eligibleQueuedTxn()
		first.Reference = sharedRef
		first.Amount = 5
		first.Precision = 100
		first.AllowOverdraft = true

		src := newBalance(first.Source)
		dst := newBalance(first.Destination)
		balances := map[string]*model.Balance{first.Source: src, first.Destination: dst}

		work, err := l.prepareQueuedBatchTransaction(ctx, span, first, balances, nil,
			map[string]struct{}{}, map[string]struct{}{}, batchRefs)
		require.NoError(t, err)
		require.NotNil(t, work.transaction)
		assert.Equal(t, StatusApplied, work.transaction.Status)
		assert.Equal(t, "-500", src.Balance.String())
		assert.Equal(t, "500", dst.Balance.String())

		second := eligibleQueuedTxn()
		second.Source = first.Source
		second.Destination = first.Destination
		second.Reference = sharedRef
		second.Amount = 5
		second.Precision = 100
		second.AllowOverdraft = true

		_, err = l.prepareQueuedBatchTransaction(ctx, span, second, balances, nil,
			map[string]struct{}{}, map[string]struct{}{}, batchRefs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already been used")
		// Money from the rejected duplicate must not have been applied.
		assert.Equal(t, "-500", src.Balance.String())
		assert.Equal(t, "500", dst.Balance.String())
	})

	t.Run("inflight batch item holds funds in inflight balances", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		ds.On("TransactionExistsByRef", mock.Anything, mock.Anything).Return(false, nil)
		l := mkBlnk(ds)

		txn := eligibleQueuedTxn()
		txn.Reference = gofakeit.UUID()
		txn.Amount = 7.5
		txn.Precision = 100
		txn.AllowOverdraft = true
		txn.Inflight = true

		src := newBalance(txn.Source)
		dst := newBalance(txn.Destination)
		work, err := l.prepareQueuedBatchTransaction(ctx, span, txn,
			map[string]*model.Balance{txn.Source: src, txn.Destination: dst}, nil,
			map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{})
		require.NoError(t, err)
		assert.Equal(t, StatusInflight, work.transaction.Status)
		assert.Equal(t, "750", src.InflightDebitBalance.String())
		assert.Equal(t, "750", dst.InflightCreditBalance.String())
		assert.Equal(t, "0", src.Balance.String())
		assert.Equal(t, "0", dst.Balance.String())
	})
}

func TestTryRecordQueuedTransactionBatch_GateBehavior(t *testing.T) {
	config.MockConfig(&config.Configuration{Queue: config.QueueConfig{NumberOfQueues: 1}})

	t.Run("coalescing disabled skips sibling lookup entirely", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		l := &Blnk{
			datasource: ds,
			config: &config.Configuration{
				Transaction: config.TransactionConfig{EnableCoalescing: false, BatchSize: 10},
			},
		}
		handled, err := l.TryRecordQueuedTransactionBatch(context.Background(), eligibleQueuedTxn())
		require.NoError(t, err)
		assert.False(t, handled)
		ds.AssertNotCalled(t, "GetQueuedTransactionsForCoalescing",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("hot lane forces batching even when coalescing disabled", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		txn := eligibleQueuedTxn()
		ds.On("GetQueuedTransactionsForCoalescing", mock.Anything, txn.Source, txn.Destination,
			txn.Currency, txn.ParentTransaction, mock.Anything, 9).
			Return([]*model.Transaction{}, nil).Once()
		ds.On("GetQueuedTransactionsForSourceCoalescing", mock.Anything, txn.Source,
			txn.Currency, txn.ParentTransaction, mock.Anything, 9).
			Return([]*model.Transaction{}, nil).Once()
		ds.On("GetQueuedTransactionsForDestinationCoalescing", mock.Anything, txn.Destination,
			txn.Currency, txn.ParentTransaction, mock.Anything, 9).
			Return([]*model.Transaction{}, nil).Once()

		l := &Blnk{
			datasource: ds,
			config: &config.Configuration{
				Transaction: config.TransactionConfig{EnableCoalescing: false, BatchSize: 10},
			},
		}
		handled, err := l.TryRecordQueuedTransactionBatchForHotLane(context.Background(), txn)
		require.NoError(t, err)
		assert.False(t, handled, "no siblings -> not handled, caller falls back to single path")
		ds.AssertExpectations(t)
	})

	t.Run("batch size below two short-circuits even for hot lane", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		l := &Blnk{
			datasource: ds,
			config: &config.Configuration{
				Transaction: config.TransactionConfig{EnableCoalescing: true, BatchSize: 1},
			},
		}
		handled, err := l.TryRecordQueuedTransactionBatchForHotLane(context.Background(), eligibleQueuedTxn())
		require.NoError(t, err)
		assert.False(t, handled)
		ds.AssertNotCalled(t, "GetQueuedTransactionsForCoalescing",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("ineligible transaction skips lookup", func(t *testing.T) {
		ds := &mocks.MockDataSource{}
		l := &Blnk{
			datasource: ds,
			config: &config.Configuration{
				Transaction: config.TransactionConfig{EnableCoalescing: true, BatchSize: 10},
			},
		}
		txn := eligibleQueuedTxn()
		txn.SkipQueue = true
		handled, err := l.TryRecordQueuedTransactionBatchForHotLane(context.Background(), txn)
		require.NoError(t, err)
		assert.False(t, handled)
		ds.AssertNotCalled(t, "GetQueuedTransactionsForCoalescing",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}

// TestHotLaneCoalescing_EndToEnd exercises the full coalescing persistence
// path against real Postgres and Redis: a leader queue-copy plus one queued
// sibling are batched, applied atomically, and the shared balances must move
// by the exact sum of both precise amounts.
func TestHotLaneCoalescing_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coalescing integration test in short mode")
	}

	ctx := context.Background()
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{SecretKey: "test-secret"},
		Transaction: config.TransactionConfig{
			BatchSize:        10,
			MaxQueueSize:     1000,
			LockDuration:     30 * time.Second,
			IndexQueuePrefix: "test_index",
			EnableCoalescing: false, // hot lane must force batching anyway
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err)
	blnk, err := NewBlnk(ds)
	require.NoError(t, err)

	source, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: GeneralLedgerID})
	require.NoError(t, err)
	dest, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: GeneralLedgerID})
	require.NoError(t, err)

	baseTime := time.Now().Add(-time.Minute)

	// Sibling: an original QUEUED row sitting in the DB waiting for a worker.
	siblingRef := gofakeit.UUID()
	sibling := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      siblingRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		PreciseAmount:  big.NewInt(333), // 3.33 USD
		Precision:      100,
		Currency:       "USD",
		Status:         StatusQueued,
		AllowOverdraft: true,
		CreatedAt:      baseTime.Add(time.Second),
		MetaData:       map[string]interface{}{"allow_overdraft": true},
	}
	model.ApplyPrecision(sibling) // populates Amount/AmountString from PreciseAmount
	sibling.Hash = sibling.HashTxn()
	_, err = ds.RecordTransaction(ctx, sibling)
	require.NoError(t, err)

	// Leader: the queue copy a worker would be processing (parent set, QUEUED).
	leaderRef := gofakeit.UUID()
	leader := &model.Transaction{
		TransactionID:     model.GenerateUUIDWithSuffix("txn"),
		ParentTransaction: model.GenerateUUIDWithSuffix("txn"),
		Reference:         leaderRef,
		Source:            source.BalanceID,
		Destination:       dest.BalanceID,
		PreciseAmount:     big.NewInt(555), // 5.55 USD
		Precision:         100,
		Currency:          "USD",
		Status:            StatusQueued,
		AllowOverdraft:    true,
		CreatedAt:         baseTime,
	}
	model.ApplyPrecision(leader)

	handled, err := blnk.TryRecordQueuedTransactionBatchForHotLane(ctx, leader)
	require.NoError(t, err)
	require.True(t, handled, "leader + queued sibling must be coalesced")

	// Leader applied under its own reference.
	leaderApplied, err := ds.GetTransactionByRef(ctx, leaderRef)
	require.NoError(t, err)
	assert.Equal(t, StatusApplied, leaderApplied.Status)

	// Sibling applied under its queue-copy reference "<ref>_q".
	siblingApplied, err := ds.GetTransactionByRef(ctx, siblingRef+"_q")
	require.NoError(t, err)
	assert.Equal(t, StatusApplied, siblingApplied.Status)
	assert.Equal(t, sibling.TransactionID, siblingApplied.ParentTransaction,
		"sibling queue copy must reference the original queued row")

	// Balance integrity: exactly leader + sibling, nothing more.
	expected := big.NewInt(555 + 333)
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err)
	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, 0, updatedSource.Balance.Cmp(new(big.Int).Neg(expected)),
		"source balance must equal -(leader+sibling); got %s", updatedSource.Balance)
	assert.Equal(t, 0, updatedDest.Balance.Cmp(expected),
		"destination balance must equal leader+sibling; got %s", updatedDest.Balance)
}

// TestHotLaneCoalescing_CurrencyIsolation verifies a queued sibling in a
// different currency on the same pair is never folded into the batch.
func TestHotLaneCoalescing_CurrencyIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coalescing integration test in short mode")
	}

	ctx := context.Background()
	cnf := &config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{SecretKey: "test-secret"},
		Transaction: config.TransactionConfig{
			BatchSize:        10,
			LockDuration:     30 * time.Second,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err)
	blnk, err := NewBlnk(ds)
	require.NoError(t, err)

	source, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: GeneralLedgerID})
	require.NoError(t, err)
	dest, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: GeneralLedgerID})
	require.NoError(t, err)

	baseTime := time.Now().Add(-time.Minute)

	// EUR sibling on the same balance pair: must never coalesce with USD leader.
	eurRef := gofakeit.UUID()
	eurSibling := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      eurRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		PreciseAmount:  big.NewInt(10000), // 100.00 EUR
		Precision:      100,
		Currency:       "EUR",
		Status:         StatusQueued,
		AllowOverdraft: true,
		CreatedAt:      baseTime.Add(time.Second),
		MetaData:       map[string]interface{}{"allow_overdraft": true},
	}
	model.ApplyPrecision(eurSibling)
	_, err = ds.RecordTransaction(ctx, eurSibling)
	require.NoError(t, err)

	leader := &model.Transaction{
		TransactionID:     model.GenerateUUIDWithSuffix("txn"),
		ParentTransaction: model.GenerateUUIDWithSuffix("txn"),
		Reference:         gofakeit.UUID(),
		Source:            source.BalanceID,
		Destination:       dest.BalanceID,
		PreciseAmount:     big.NewInt(1000), // 10.00 USD
		Precision:         100,
		Currency:          "USD",
		Status:            StatusQueued,
		AllowOverdraft:    true,
		CreatedAt:         baseTime,
	}
	leader.PreciseAmount = model.ApplyPrecision(leader)

	handled, err := blnk.TryRecordQueuedTransactionBatchForHotLane(ctx, leader)
	require.NoError(t, err)
	assert.False(t, handled, "USD leader must not coalesce with an EUR sibling")

	// The EUR sibling must still be queued and untouched.
	eurRow, err := ds.GetTransactionByRef(ctx, eurRef)
	require.NoError(t, err)
	assert.Equal(t, StatusQueued, eurRow.Status)

	// No money may have moved.
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, 0, updatedSource.Balance.Cmp(big.NewInt(0)))
}
