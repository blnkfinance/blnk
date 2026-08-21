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

	// References seen so far in this batch. A reference collision is a flat
	// fact independent of cumulative vs independent execution — either mode
	// hits the same database uniqueness constraint — so it is checked the
	// same way, in request order, regardless of skip_queue.
	seenRefs := make(map[string]bool, len(request.Transactions))

	for _, transaction := range request.Transactions {
		if transaction == nil {
			continue
		}

		item := transaction.Clone()
		item.Inflight = request.Inflight
		item.SkipQueue = request.SkipQueue

		itemPreview, err := l.bulkItemDuplicateReference(ctx, item, seenRefs)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if itemPreview == nil {
			itemPreview, err = l.previewBulkItem(ctx, item, working, request.SkipQueue)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
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
	preview.Finalize()
	return preview, nil
}

// bulkItemDuplicateReference checks item's reference against every reference
// already seen earlier in this batch, then — if it clears that — against the
// database, recording it into seenRefs once it clears both.
//
// A hit either way is fatal on a real post: transaction validation rejects a
// duplicate reference before balances are ever touched, in cumulative mode
// or not. So a hit here is projected as a rejection without calling into
// previewBulkItem at all — a rejected item never reaches balance application
// for real, and in cumulative mode letting it touch the shared working
// balances anyway would leak its amount into how later items are projected.
//
// Returns a non-nil preview only when item.Reference collides; nil, nil
// means the caller should preview the item normally.
func (l *Blnk) bulkItemDuplicateReference(ctx context.Context, item *model.Transaction, seenRefs map[string]bool) (*model.TransactionPreview, error) {
	if item.Reference == "" {
		return nil, nil
	}

	duplicate := seenRefs[item.Reference]
	if !duplicate {
		exists, err := l.datasource.TransactionExistsByRef(ctx, item.Reference)
		if err != nil {
			return nil, err
		}
		duplicate = exists
	}

	if !duplicate {
		seenRefs[item.Reference] = true
		return nil, nil
	}

	normalizePreviewStatus(item)
	item.PreciseAmount = model.ApplyPrecision(item)

	preview := newPreviewFor(item)
	preview.WouldApply = false
	preview.PreciseAmount = preciseString(item.PreciseAmount)
	preview.Amount = item.Amount
	preview.Rejection = previewRejection(fmt.Errorf("transaction validation failed: reference %s has already been used", item.Reference))
	preview.Finalize()
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

	// Whether this item is itself a split decides both what gets reported and
	// how a failure is handled below.
	isSplit := len(item.Sources) > 0 || len(item.Destinations) > 0

	for _, leg := range legs {
		normalizePreviewStatus(leg)
		leg.PreciseAmount = model.ApplyPrecision(leg)

		source, destination, err := working.resolvePair(ctx, leg)
		if err != nil {
			return nil, err
		}

		failed := false
		if err := sameBalanceErr(source, destination); err != nil {
			preview.WouldApply = false
			if preview.Rejection == nil {
				preview.Rejection = previewRejection(err)
			}
			failed = true
		} else if applyErr := l.processBalances(ctx, leg, source.balance, destination.balance); applyErr != nil {
			preview.WouldApply = false
			if preview.Rejection == nil {
				preview.Rejection = previewRejection(applyErr)
			}
			failed = true
		}

		// Report the per-leg breakdown a split item produces, as
		// previewSplitTransaction does for the same item outside a batch.
		// Without this the same split reported its legs when the batch was
		// queued — that path runs through PreviewTransaction — and reported
		// none when the batch was cumulative.
		if isSplit && !failed {
			role, identifier := model.PreviewRoleDestination, leg.Destination
			if len(item.Sources) > 0 {
				role, identifier = model.PreviewRoleSource, leg.Source
			}
			preview.Legs = append(preview.Legs, model.LegProjection{
				Identifier:    identifier,
				Role:          role,
				PreciseAmount: preciseString(leg.PreciseAmount),
				Amount:        leg.Amount,
			})
		}

		// A cumulative batch records its items synchronously and stops at the
		// first one that fails, and a split item's legs are recorded the same
		// way within it, so the legs after a failure are never attempted.
		if failed {
			break
		}
	}

	preview.Finalize()
	return preview, nil
}
