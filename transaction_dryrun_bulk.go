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

	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel/attribute"
)

// PreviewBulkTransactions projects a batch without applying any of it.
//
// How the batch would really execute decides how it is projected:
//
//	skip_queue=true   items are applied inline, one after another, so a later
//	                  item sees what earlier ones did. The projection carries
//	                  balances forward across items, which is what catches a
//	                  batch that spends within itself more than it has.
//
//	skip_queue=false  each item is handed to its own goroutine and the batch
//	                  loop moves on, so the items race and no order is
//	                  guaranteed. Each item is projected independently against
//	                  the balances as they stand.
//
// The mode is reported as `cumulative` so a caller can tell which answer they
// were given.
func (l *Blnk) PreviewBulkTransactions(ctx context.Context, request *model.BulkTransactionRequest) (*model.BulkTransactionPreview, error) {
	ctx, span := tracer.Start(ctx, "PreviewBulkTransactions")
	defer span.End()

	preview := &model.BulkTransactionPreview{
		DryRun:     true,
		WouldApply: true,
		Cumulative: request.SkipQueue,
		Atomic:     request.Atomic,
		Results:    make([]model.TransactionPreview, 0, len(request.Transactions)),
	}

	if !request.SkipQueue {
		preview.AddNote("items are dispatched concurrently unless skip_queue is set, so each item is projected independently against current balances and real execution order is not guaranteed")
	}
	if request.Atomic {
		preview.AddNote("an atomic batch compensates on failure by voiding or refunding already-applied items rather than rolling back, and that compensation can itself fail")
	}
	if request.RunAsync {
		preview.AddNote("run_async is ignored in a dry run; the projection is returned immediately")
	}

	// One working copy per balance, shared across items in cumulative mode so a
	// balance touched twice accumulates, and reused in either mode so each
	// distinct balance is read once rather than once per item.
	working := newPreviewBalanceSet(l)

	for _, transaction := range request.Transactions {
		if transaction == nil {
			continue
		}

		item := transaction.Clone()
		item.Inflight = request.Inflight
		item.SkipQueue = request.SkipQueue

		itemPreview, err := l.previewBulkItem(ctx, item, working, request.SkipQueue)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		if !itemPreview.WouldApply {
			preview.WouldApply = false
		}
		preview.Results = append(preview.Results, *itemPreview)
	}

	if request.SkipQueue {
		preview.Balances = working.projections()
	}

	span.SetAttributes(
		attribute.Bool("preview.would_apply", preview.WouldApply),
		attribute.Bool("preview.cumulative", preview.Cumulative),
		attribute.Int("preview.items", len(preview.Results)),
	)
	return preview, nil
}

// previewBulkItem projects one item of a batch.
//
// In cumulative mode the item is applied to the shared working balances so the
// next item sees it. Otherwise the item is projected on its own, which is what
// a concurrently dispatched item would actually see.
func (l *Blnk) previewBulkItem(ctx context.Context, item *model.Transaction, working *previewBalanceSet, cumulative bool) (*model.TransactionPreview, error) {
	if !cumulative {
		return l.PreviewTransaction(ctx, item)
	}

	normalizePreviewStatus(item)
	item.PreciseAmount = model.ApplyPrecision(item)

	// An item may itself be a split, so the two-level shape (batch, item, legs)
	// has to be expanded before balances are touched.
	legs := []*model.Transaction{item}
	if len(item.Sources) > 0 || len(item.Destinations) > 0 {
		split, err := item.SplitTransactionPrecise(ctx)
		if err != nil {
			return nil, err
		}
		legs = split
	}

	preview := newPreviewFor(item)
	preview.WouldApply = true
	preview.PreciseAmount = preciseString(item.PreciseAmount)
	preview.Amount = item.Amount

	for _, leg := range legs {
		normalizePreviewStatus(leg)
		leg.PreciseAmount = model.ApplyPrecision(leg)

		source, destination, err := working.resolvePair(ctx, leg)
		if err != nil {
			return nil, err
		}

		if applyErr := l.processBalances(ctx, leg, source.balance, destination.balance); applyErr != nil {
			preview.WouldApply = false
			if preview.Rejection == nil {
				preview.Rejection = previewRejection(applyErr)
			}
		}
	}

	return preview, nil
}
