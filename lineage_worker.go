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
	"sync"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

// LineageOutboxProcessor processes pending lineage outbox entries.
// It polls the database for pending entries and processes them asynchronously,
// ensuring that lineage work is never lost even if Redis/queue operations fail.
type LineageOutboxProcessor struct {
	blnk         *Blnk
	batchSize    int
	pollInterval time.Duration
	lockDuration time.Duration
	stopCh       chan struct{}
	wg           sync.WaitGroup
	running      bool
	mu           sync.Mutex
}

// NewLineageOutboxProcessor creates a new outbox processor.
//
// Parameters:
// - blnk *Blnk: The Blnk instance containing the datasource and processing logic.
//
// Returns:
// - *LineageOutboxProcessor: The configured processor.
func NewLineageOutboxProcessor(blnk *Blnk) *LineageOutboxProcessor {
	return &LineageOutboxProcessor{
		blnk:         blnk,
		batchSize:    100,
		pollInterval: 1 * time.Second,
		lockDuration: 30 * time.Second,
		stopCh:       make(chan struct{}),
	}
}

// WithBatchSize sets the batch size for processing outbox entries.
//
// Parameters:
// - size int: The number of entries to process per batch.
//
// Returns:
// - *LineageOutboxProcessor: The processor for chaining.
func (p *LineageOutboxProcessor) WithBatchSize(size int) *LineageOutboxProcessor {
	p.batchSize = size
	return p
}

// WithPollInterval sets the interval between polls.
//
// Parameters:
// - interval time.Duration: The poll interval.
//
// Returns:
// - *LineageOutboxProcessor: The processor for chaining.
func (p *LineageOutboxProcessor) WithPollInterval(interval time.Duration) *LineageOutboxProcessor {
	p.pollInterval = interval
	return p
}

// WithLockDuration sets the lock duration for claimed entries.
//
// Parameters:
// - duration time.Duration: The lock duration.
//
// Returns:
// - *LineageOutboxProcessor: The processor for chaining.
func (p *LineageOutboxProcessor) WithLockDuration(duration time.Duration) *LineageOutboxProcessor {
	p.lockDuration = duration
	return p
}

// Start begins processing outbox entries in the background.
// The processor will poll for pending entries at the configured interval.
//
// Parameters:
// - ctx context.Context: The context for the operation. When cancelled, processing stops.
func (p *LineageOutboxProcessor) Start(ctx context.Context) {
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
}

// Stop gracefully stops the outbox processor.
// It signals the processor to stop and waits for pending work to complete.
func (p *LineageOutboxProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	logrus.Info("Lineage outbox processor stopped")
}

// IsRunning returns whether the processor is currently running.
//
// Returns:
// - bool: True if the processor is running.
func (p *LineageOutboxProcessor) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// run is the main processing loop.
func (p *LineageOutboxProcessor) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Lineage outbox processor context cancelled")
			return
		case <-p.stopCh:
			logrus.Info("Lineage outbox processor stop signal received")
			return
		case <-ticker.C:
			p.processBatch(ctx)
		}
	}
}

// processBatch claims and processes a batch of pending outbox entries.
func (p *LineageOutboxProcessor) processBatch(ctx context.Context) {
	entries, err := p.blnk.datasource.ClaimPendingOutboxEntries(ctx, p.batchSize, p.lockDuration)
	if err != nil {
		logrus.Errorf("failed to claim outbox entries: %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	logrus.Infof("Processing %d lineage outbox entries", len(entries))

	for _, entry := range entries {
		if err := p.processEntry(ctx, entry); err != nil {
			logrus.Errorf("failed to process outbox entry %d (txn: %s): %v", entry.ID, entry.TransactionID, err)
			if markErr := p.blnk.datasource.MarkOutboxFailed(ctx, entry.ID, err.Error()); markErr != nil {
				logrus.Errorf("failed to mark outbox entry %d as failed: %v", entry.ID, markErr)
			}
		} else {
			if markErr := p.blnk.datasource.MarkOutboxCompleted(ctx, entry.ID); markErr != nil {
				logrus.Errorf("failed to mark outbox entry %d as completed: %v", entry.ID, markErr)
			}
		}
	}
}

// processEntry processes a single outbox entry.
func (p *LineageOutboxProcessor) processEntry(ctx context.Context, entry model.LineageOutbox) error {
	return p.blnk.ProcessLineageFromOutbox(ctx, entry)
}
