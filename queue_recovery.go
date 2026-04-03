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
	"time"

	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

type QueuedTransactionRecoveryProcessor struct {
	blnk                     *Blnk
	batchSize                int
	maxWorkers               int
	pollInterval             time.Duration
	stuckThreshold           time.Duration
	maxRecoveryAttempts      int
	stopCh                   chan struct{}
	wg                       sync.WaitGroup
	running                  bool
	mu                       sync.Mutex
	processQueuedTransaction func(ctx context.Context, txn *model.Transaction, hotLane bool) (transactionExecutionResult, error)
}

// NewQueuedTransactionRecoveryProcessor creates the stuck queued-transaction recovery loop
// with conservative single-worker defaults to avoid recovery-induced lock storms.
func NewQueuedTransactionRecoveryProcessor(blnk *Blnk) *QueuedTransactionRecoveryProcessor {
	return &QueuedTransactionRecoveryProcessor{
		blnk:                     blnk,
		batchSize:                100,
		maxWorkers:               1,
		pollInterval:             30 * time.Second,
		stuckThreshold:           2 * time.Hour,
		maxRecoveryAttempts:      3,
		stopCh:                   make(chan struct{}),
		processQueuedTransaction: blnk.processQueuedTransaction,
	}
}

// Start begins the background recovery loop for stuck queued transactions.
func (p *QueuedTransactionRecoveryProcessor) Start(ctx context.Context) {
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

	logrus.Info("Queued transaction recovery processor started")
}

// Stop shuts down the background recovery loop and waits for the worker goroutine to exit.
func (p *QueuedTransactionRecoveryProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	logrus.Info("Queued transaction recovery processor stopped")
}

// IsRunning reports whether the recovery processor is currently active.
func (p *QueuedTransactionRecoveryProcessor) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// run executes the poll loop that periodically scans for stuck queued transactions.
func (p *QueuedTransactionRecoveryProcessor) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Queued transaction recovery processor context cancelled")
			return
		case <-p.stopCh:
			logrus.Info("Queued transaction recovery processor stop signal received")
			return
		case <-ticker.C:
			p.processBatch(ctx)
		}
	}
}

// processBatch performs one periodic stuck-queue recovery pass using the configured threshold.
func (p *QueuedTransactionRecoveryProcessor) processBatch(ctx context.Context) {
	p.recoverWithThreshold(ctx, p.stuckThreshold)
}

// RecoverQueuedTransactions triggers an immediate recovery of stuck queued transactions
// using the provided threshold. This is exposed for the manual trigger API endpoint.
func (b *Blnk) RecoverQueuedTransactions(ctx context.Context, threshold time.Duration) (int, error) {
	if threshold < 2*time.Minute {
		threshold = 2 * time.Minute
	}

	processor := NewQueuedTransactionRecoveryProcessor(b)
	return processor.recoverWithThreshold(ctx, threshold), nil
}

// recoverWithThreshold loads currently stuck queued transactions and reprocesses them serially.
func (p *QueuedTransactionRecoveryProcessor) recoverWithThreshold(ctx context.Context, threshold time.Duration) int {
	stuckTxns, err := p.blnk.datasource.GetStuckQueuedTransactions(ctx, threshold, p.batchSize)
	if err != nil {
		logrus.Errorf("failed to get stuck queued transactions: %v", err)
		return 0
	}

	if len(stuckTxns) == 0 {
		return 0
	}

	logrus.Infof("Processing %d stuck queued transactions with %d workers (threshold=%v)", len(stuckTxns), p.maxWorkers, threshold)

	for _, txn := range stuckTxns {
		if err := p.processStuckTransaction(ctx, txn); err != nil {
			logrus.Errorf("failed to process stuck transaction %s: %v", txn.TransactionID, err)
		}
	}

	return len(stuckTxns)
}

// processStuckTransaction replays one stuck queued transaction, preserving the existing recovery
// attempt tracking and rejection semantics while preferring the shared queued executor path.
func (p *QueuedTransactionRecoveryProcessor) processStuckTransaction(ctx context.Context, stuckTxn *model.Transaction) error {
	restoreTransactionFlagsFromMetadata(stuckTxn)

	attempts := 0
	if stuckTxn.MetaData != nil {
		if v, ok := stuckTxn.MetaData["recovery_attempts"]; ok {
			switch val := v.(type) {
			case float64:
				attempts = int(val)
			case int:
				attempts = val
			}
		}
	}
	attempts++

	if attempts > p.maxRecoveryAttempts {
		logrus.Warnf("Stuck transaction %s exceeded max recovery attempts (%d), rejecting", stuckTxn.TransactionID, p.maxRecoveryAttempts)
		rejectionCopy := createQueueCopy(stuckTxn, stuckTxn.Reference)
		_, err := p.blnk.RejectTransaction(ctx, rejectionCopy, "exceeded max queued recovery attempts")
		if err != nil {
			if isReferenceAlreadyUsedError(err) {
				return nil
			}
			return err
		}
		return nil
	}

	if stuckTxn.Atomic {
		if parentID, ok := stuckTxn.MetaData["QUEUED_PARENT_TRANSACTION"].(string); ok && parentID != "" {
			siblings, err := p.blnk.datasource.GetTransactionsByParent(ctx, parentID, 100, 0)
			if err != nil {
				return err
			}
			for _, sibling := range siblings {
				if sibling.Status == StatusRejected {
					logrus.Infof("Skipping stuck transaction %s: sibling %s is REJECTED in atomic group", stuckTxn.TransactionID, sibling.TransactionID)
					return nil
				}
			}
		}
	}

	queueCopy := createQueueCopy(stuckTxn, stuckTxn.Reference)
	result, err := p.tryRecordRecoveredTransaction(ctx, queueCopy)
	if err != nil {
		if isReferenceAlreadyUsedError(err) {
			logrus.Infof("Stuck transaction %s already processed (reference %s already used)", stuckTxn.TransactionID, queueCopy.Reference)
			p.updateRecoveryMetadata(ctx, stuckTxn, attempts, "already_processed")
			return nil
		}

		p.updateRecoveryMetadata(ctx, stuckTxn, attempts, "failed")
		return err
	}

	if result.usedCoalescing() {
		logrus.Infof("Successfully recovered stuck transaction %s via coalesced batch", stuckTxn.TransactionID)
	} else {
		logrus.Infof("Successfully recovered stuck transaction %s via queue copy %s", stuckTxn.TransactionID, queueCopy.TransactionID)
	}
	p.updateRecoveryMetadata(ctx, stuckTxn, attempts, "recovered")
	return nil
}

// tryRecordRecoveredTransaction routes stuck-transaction replay through the shared queued
// processing path, selecting hot-lane execution when the queued metadata requires it.
func (p *QueuedTransactionRecoveryProcessor) tryRecordRecoveredTransaction(ctx context.Context, queueCopy *model.Transaction) (transactionExecutionResult, error) {
	hotLane := hotpairs.QueueLaneFromMetadata(queueCopy.MetaData) == hotpairs.LaneHot
	return p.processQueuedTransaction(ctx, queueCopy, hotLane)
}

// updateRecoveryMetadata stores recovery attempt and status information on the stuck parent
// transaction so later recovery passes can make bounded retry decisions.
func (p *QueuedTransactionRecoveryProcessor) updateRecoveryMetadata(ctx context.Context, txn *model.Transaction, attempts int, status string) {
	if txn.MetaData == nil {
		txn.MetaData = make(map[string]interface{})
	}
	txn.MetaData["recovery_attempts"] = attempts
	txn.MetaData["recovery_status"] = status
	txn.MetaData["recovery_last_attempt"] = time.Now().UTC().Format(time.RFC3339)

	if err := p.blnk.datasource.UpdateTransactionMetadata(ctx, txn.TransactionID, txn.MetaData); err != nil {
		logrus.Errorf("failed to update recovery metadata for transaction %s: %v", txn.TransactionID, err)
	}
}

func isReferenceAlreadyUsedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "has already been used")
}
