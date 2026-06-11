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
	"fmt"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

// maxBatchesPerTick bounds how much a single tick chains so a large backfill
// never starves the ticker; the next tick simply continues.
const maxBatchesPerTick = 50

// ChainProcessor seals transactions into the tamper-evident hash chain off the
// hot path. It polls for unchained rows and links them in a single DB
// transaction per batch — modeled on LineageOutboxProcessor.
type ChainProcessor struct {
	blnk          *Blnk
	pollInterval  time.Duration
	batchSize     int
	trailingDelay time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
	running       bool
	mu            sync.Mutex
}

// NewChainProcessor builds a ChainProcessor from the HashChain config (with safe
// fallbacks).
func NewChainProcessor(blnk *Blnk) *ChainProcessor {
	hc := blnk.Config().Transaction.HashChain
	pollInterval := hc.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	batchSize := hc.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	trailingDelay := hc.TrailingDelay
	if trailingDelay <= 0 {
		trailingDelay = 30 * time.Second
	}
	return &ChainProcessor{
		blnk:          blnk,
		pollInterval:  pollInterval,
		batchSize:     batchSize,
		trailingDelay: trailingDelay,
		stopCh:        make(chan struct{}),
	}
}

// Start launches the background chainer loop.
func (p *ChainProcessor) Start(ctx context.Context) {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(ctx)
	}()
	logrus.Info("Hash-chain processor started")
}

// Stop signals the loop to exit and waits for it.
func (p *ChainProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	logrus.Info("Hash-chain processor stopped")
}

// IsRunning reports whether the loop is active.
func (p *ChainProcessor) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

func (p *ChainProcessor) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.processTick(ctx)
		}
	}
}

// processTick chains as many batches as are ready (bounded), then records metrics.
func (p *ChainProcessor) processTick(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-p.trailingDelay)
	for i := 0; i < maxBatchesPerTick; i++ {
		n, err := p.blnk.datasource.ChainPendingTransactions(ctx, cutoff, p.batchSize)
		if err != nil {
			logrus.WithError(err).Error("hash-chain: failed to seal batch")
			break
		}
		if n < p.batchSize {
			break // caught up
		}
	}
	p.recordMetrics(ctx, cutoff)
}

func (p *ChainProcessor) recordMetrics(ctx context.Context, cutoff time.Time) {
	if state, err := p.blnk.datasource.GetChainState(ctx); err == nil {
		metrics.ChainHeadSeq.Record(ctx, state.LastSeq)
		metrics.ChainLagSeconds.Record(ctx, time.Since(state.UpdatedAt).Seconds())
	}
	if backlog, err := p.blnk.datasource.CountUnchainedTransactions(ctx, cutoff); err == nil {
		metrics.ChainBacklog.Record(ctx, backlog)
	}
}

// ChainVerifyResult is the outcome of replaying the chain from genesis.
type ChainVerifyResult struct {
	Verified    bool
	LastSeq     int64
	HeadHash    string
	BrokenSeq   int64
	BrokenTxnID string
	Reason      string
}

// VerifyChain replays the whole chain from genesis, checking that each row's
// chain_seq is contiguous, its chain_prev_hash matches the running head, and its
// recomputed hash matches the stored chain_hash. It reports the first broken
// link, or success with the final head. progress (optional) is called with each
// verified seq.
func (l *Blnk) VerifyChain(ctx context.Context, progress func(seq int64)) (ChainVerifyResult, error) {
	state, err := l.datasource.GetChainState(ctx)
	if err != nil {
		return ChainVerifyResult{}, err
	}

	head := state.GenesisHash
	var lastSeq int64
	expectedSeq := int64(1)
	const page = 1000

	for {
		rows, err := l.datasource.GetChainedTransactionsAfter(ctx, lastSeq, page)
		if err != nil {
			return ChainVerifyResult{}, err
		}
		if len(rows) == 0 {
			break
		}
		for _, ct := range rows {
			if ct.ChainSeq != expectedSeq {
				return ChainVerifyResult{BrokenSeq: ct.ChainSeq, BrokenTxnID: ct.Row.TransactionID,
					Reason: fmt.Sprintf("non-contiguous chain_seq: expected %d, got %d", expectedSeq, ct.ChainSeq)}, nil
			}
			if ct.ChainPrevHash != head {
				return ChainVerifyResult{BrokenSeq: ct.ChainSeq, BrokenTxnID: ct.Row.TransactionID,
					Reason: "chain_prev_hash does not match the running head"}, nil
			}
			if model.ComputeChainHash(head, ct.Row) != ct.ChainHash {
				return ChainVerifyResult{BrokenSeq: ct.ChainSeq, BrokenTxnID: ct.Row.TransactionID,
					Reason: "recomputed hash does not match stored chain_hash"}, nil
			}
			head = ct.ChainHash
			lastSeq = ct.ChainSeq
			expectedSeq++
			if progress != nil {
				progress(lastSeq)
			}
		}
	}

	// The replayed head must match the recorded head at the recorded length.
	if lastSeq == state.LastSeq && head != state.HeadHash {
		return ChainVerifyResult{BrokenSeq: lastSeq, Reason: "replayed head does not match chain_state head_hash"}, nil
	}

	return ChainVerifyResult{Verified: true, LastSeq: lastSeq, HeadHash: head}, nil
}
