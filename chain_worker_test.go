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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// chainAll seals every transaction created so far (cutoff in the future).
func chainAll(t *testing.T, ds database.IDataSource) {
	t.Helper()
	ctx := context.Background()
	for {
		n, err := ds.ChainPendingTransactions(ctx, time.Now().Add(time.Hour), 500)
		require.NoError(t, err)
		if n < 500 {
			return
		}
	}
}

// assertSealed verifies one transaction is chained and its stored chain_hash
// recomputes from its stored chain_prev_hash — i.e. the chainer sealed it
// correctly. It checks only this row, so it is robust to the globally shared
// test DB (which other tests' trigger-bypassing helpers can mutate).
func assertSealed(t *testing.T, ds database.IDataSource, txnID string) {
	t.Helper()
	conn := ds.(*database.Datasource).Conn
	var seq int64
	var prev, stored string
	var r model.ChainRow
	err := conn.QueryRowContext(context.Background(), `
		SELECT chain_seq, COALESCE(chain_prev_hash, ''), COALESCE(chain_hash, ''),
		       transaction_id, COALESCE(source, ''), COALESCE(destination, ''),
		       COALESCE(amount::text, ''), COALESCE(precise_amount::text, ''),
		       COALESCE(currency, ''), COALESCE(status, ''), COALESCE(reference, ''),
		       created_at
		FROM blnk.transactions WHERE transaction_id = $1`, txnID).
		Scan(&seq, &prev, &stored, &r.TransactionID, &r.Source, &r.Destination,
			&r.Amount, &r.PreciseAmount, &r.Currency, &r.Status, &r.Reference, &r.CreatedAt)
	require.NoError(t, err)
	require.Greater(t, seq, int64(0), "txn %s must be chained", txnID)
	require.NotEmpty(t, stored)
	assert.Equal(t, stored, model.ComputeChainHash(prev, r), "stored chain_hash must recompute for %s", txnID)
}

func TestChain_SealsTransactions(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, float64(10+i)).TransactionID)
	}

	chainAll(t, ds)
	for _, id := range ids {
		assertSealed(t, ds, id)
	}

	// Idempotent: re-running seals nothing new.
	n, err := ds.ChainPendingTransactions(context.Background(), time.Now().Add(time.Hour), 500)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "a second pass must seal nothing new")
}

func TestChainProcessor_ChainsAcrossTicks(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)
	id := recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, 7).TransactionID

	ctx := context.Background()
	p := NewChainProcessor(b)
	p.pollInterval = 100 * time.Millisecond
	p.trailingDelay = 0

	p.Start(ctx)
	defer p.Stop()
	assert.True(t, p.IsRunning())

	require.Eventually(t, func() bool {
		n, err := ds.CountUnchainedTransactions(ctx, time.Now())
		return err == nil && n == 0
	}, 15*time.Second, 200*time.Millisecond, "processor should drain the backlog")

	assertSealed(t, ds, id)
}

// TestChain_ConcurrentChainersSerialize fires several chainers at the shared
// chain at once. The chain_state FOR UPDATE lock serializes them and the unique
// chain_seq index forbids duplicate positions; every transaction is sealed
// correctly exactly once. Run under -race.
func TestChain_ConcurrentChainersSerialize(t *testing.T) {
	b, ds := newCoreTestBlnk(t)
	src, dst := newBalancePair(t, ds)

	var ids []string
	for i := 0; i < 8; i++ {
		ids = append(ids, recordAppliedTxn(t, b, src.BalanceID, dst.BalanceID, float64(5+i)).TransactionID)
	}

	ctx := context.Background()
	cutoff := time.Now().Add(time.Hour)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := ds.ChainPendingTransactions(ctx, cutoff, 100)
				if err != nil {
					t.Errorf("concurrent chainer failed: %v", err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, id := range ids {
		assertSealed(t, ds, id)
	}
}

// TestVerifyChain exercises the verifier logic in isolation with a mock
// datasource, so it does not depend on the globally shared test DB.
func TestVerifyChain(t *testing.T) {
	genesis := model.ChainGenesisHash
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []model.ChainRow{
		{TransactionID: "t1", Source: "a", Destination: "b", Amount: "10", PreciseAmount: "1000", Currency: "USD", Status: "APPLIED", Reference: "r1", CreatedAt: at},
		{TransactionID: "t2", Source: "b", Destination: "c", Amount: "20", PreciseAmount: "2000", Currency: "USD", Status: "APPLIED", Reference: "r2", CreatedAt: at},
		{TransactionID: "t3", Source: "c", Destination: "d", Amount: "30", PreciseAmount: "3000", Currency: "USD", Status: "APPLIED", Reference: "r3", CreatedAt: at},
	}
	head := genesis
	var chain []model.ChainedTransaction
	for i, r := range rows {
		h := model.ComputeChainHash(head, r)
		chain = append(chain, model.ChainedTransaction{Row: r, ChainSeq: int64(i + 1), ChainPrevHash: head, ChainHash: h})
		head = h
	}

	newMock := func(rows []model.ChainedTransaction) *Blnk {
		ds := new(mocks.MockDataSource)
		ds.On("GetChainState", mock.Anything).Return(
			&model.ChainState{ChainKey: "global", GenesisHash: genesis, HeadHash: head, LastSeq: int64(len(rows))}, nil)
		ds.On("GetChainedTransactionsAfter", mock.Anything, int64(0), mock.Anything).Return(rows, nil)
		ds.On("GetChainedTransactionsAfter", mock.Anything, int64(len(rows)), mock.Anything).
			Return([]model.ChainedTransaction{}, nil)
		return &Blnk{datasource: ds}
	}

	t.Run("intact chain verifies", func(t *testing.T) {
		res, err := newMock(chain).VerifyChain(context.Background(), nil)
		require.NoError(t, err)
		assert.True(t, res.Verified)
		assert.Equal(t, int64(3), res.LastSeq)
		assert.Equal(t, head, res.HeadHash)
	})

	t.Run("tampered hash is caught", func(t *testing.T) {
		bad := append([]model.ChainedTransaction(nil), chain...)
		bad[1].ChainHash = strings.Repeat("f", 64)
		res, err := newMock(bad).VerifyChain(context.Background(), nil)
		require.NoError(t, err)
		assert.False(t, res.Verified)
		assert.Equal(t, int64(2), res.BrokenSeq)
		assert.Equal(t, "t2", res.BrokenTxnID)
	})

	t.Run("non-contiguous seq is caught", func(t *testing.T) {
		bad := append([]model.ChainedTransaction(nil), chain...)
		bad[1].ChainSeq = 99
		res, err := newMock(bad).VerifyChain(context.Background(), nil)
		require.NoError(t, err)
		assert.False(t, res.Verified)
		assert.Contains(t, res.Reason, "non-contiguous")
	})
}
