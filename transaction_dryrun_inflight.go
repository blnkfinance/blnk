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
	"math/big"

	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel/attribute"
)

// PreviewInflightAction projects a commit or void of an inflight transaction
// without settling it.
//
// txID may name a single inflight transaction or the parent of several legs. A
// parent is expanded and every leg is projected, so the caller sees the whole
// balance effect rather than one representative leg's.
//
// Nothing is written: no settlement transaction is recorded, no hold is
// released, and nothing is queued.
func (l *Blnk) PreviewInflightAction(ctx context.Context, txID, action string, amount *big.Int) (*model.TransactionPreview, error) {
	ctx, span := tracer.Start(ctx, "PreviewInflightAction")
	defer span.End()

	if action != InflightActionCommit && action != InflightActionVoid {
		return nil, fmt.Errorf("status not supported. use either commit or void")
	}

	legs, err := l.inflightLegsForPreview(ctx, txID)
	if err != nil {
		// A transaction that's already been voided, or was never inflight,
		// is exactly the kind of "would this apply" fact a preview answers
		// with would_apply: false — the same treatment already-committed
		// and over-limit amounts get further down. Only a genuinely missing
		// id, or an infrastructure failure, stays a hard error.
		switch classifyInflightError(err) {
		case "ALREADY_VOIDED", "NOT_INFLIGHT":
			status := StatusApplied
			if action == InflightActionVoid {
				status = StatusVoid
			}
			rejected := &model.TransactionPreview{
				DryRun:        true,
				WouldApply:    false,
				Operation:     action,
				Status:        status,
				Rejection:     previewRejection(err),
				PreciseAmount: preciseString(nil),
				Balances:      []model.BalanceProjection{},
			}
			rejected.Finalize()
			return rejected, nil
		}
		span.RecordError(err)
		return nil, err
	}

	preview := &model.TransactionPreview{
		DryRun:     true,
		WouldApply: true,
		Operation:  action,
	}

	// Void always releases the whole remaining hold, so an amount sent with one
	// would be silently ignored rather than honoured — say so instead.
	if action == InflightActionVoid && amount != nil && amount.Sign() > 0 {
		preview.AddNote("amount is ignored when voiding; a void always releases the full remaining hold")
	}

	working := newPreviewBalanceSet(l)
	total := big.NewInt(0)

	// Collected across every leg rather than keeping only the first, matching
	// ProcessTransactionInBatches — the real engine behind a multi-leg
	// commit/void — which appends every worker's error to allErrors and
	// reports them together. A real post on a split parent doesn't stop at
	// leg one's failure and go silent about leg two's; the preview shouldn't
	// either, or a caller fixing the reported cause could still be surprised
	// by an unrelated one it never heard about.
	var legErrors []error

	for _, leg := range legs {
		settlement, err := l.buildInflightSettlementForPreview(ctx, leg, action, amount)
		if err != nil {
			preview.WouldApply = false
			legErrors = append(legErrors, err)
			continue
		}

		source, destination, err := working.resolvePair(ctx, settlement)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		if err := sameBalanceErr(source, destination); err != nil {
			preview.WouldApply = false
			legErrors = append(legErrors, err)
			continue
		}

		if applyErr := l.processBalances(ctx, settlement, source.balance, destination.balance); applyErr != nil {
			preview.WouldApply = false
			legErrors = append(legErrors, applyErr)
			continue
		}

		total = new(big.Int).Add(total, settlement.PreciseAmount)
		preview.Currency = settlement.Currency
		preview.Precision = settlement.Precision

		if len(legs) > 1 {
			preview.Legs = append(preview.Legs, model.LegProjection{
				Identifier:    leg.TransactionID,
				Role:          model.PreviewRoleSource,
				PreciseAmount: preciseString(settlement.PreciseAmount),
				Amount:        settlement.Amount,
			})
		}
	}

	if len(legErrors) > 0 {
		// Mirrors ProcessTransactionInBatches exactly, including its
		// single-error case: one failing leg is reported as itself, several
		// are reported together. Kept in lockstep with that function so a
		// projected rejection keeps reading the same as the real one.
		if len(legErrors) == 1 {
			preview.Rejection = previewRejection(legErrors[0])
		} else {
			preview.Rejection = previewRejection(fmt.Errorf("error occurred during processing: %v", legErrors))
		}
	}

	preview.PreciseAmount = preciseString(total)

	// Status and Amount are reported for the same reason the create and bulk
	// previews report them: they are the headline fields of the projection,
	// and leaving them at their zero values made an inflight preview look
	// like it had answered "" and 0 rather than "did not populate these".
	//
	// Status is the status the settlement would carry once applied, which is
	// what the real endpoint returns for the same call — APPLIED for a
	// commit, VOID for a void — not the COMMIT/VOID action marker used
	// internally while the settlement is being built.
	//
	// Amount is the float rendering of the precise total already reported
	// above, so the two always describe the same number. Precision is only
	// non-zero once at least one leg projected successfully; with no such leg
	// there is no scale to divide by and the amount stays at zero alongside
	// the zero precise total.
	if action == InflightActionVoid {
		preview.Status = StatusVoid
	} else {
		preview.Status = StatusApplied
	}
	if preview.Precision > 0 {
		preview.Amount = l.convertPreciseToFloat(total, preview.Precision)
	}

	preview.Balances = working.projections()

	span.SetAttributes(
		attribute.Bool("preview.would_apply", preview.WouldApply),
		attribute.Int("preview.legs", len(legs)),
	)
	preview.Finalize()
	return preview, nil
}

// inflightLegsForPreview resolves txID to the inflight transactions an action
// against it would settle.
//
// A transaction id resolves to itself. An id that names no inflight transaction
// directly may still be the parent of a split, so it is expanded the same way
// the real commit and void paths expand it.
func (l *Blnk) inflightLegsForPreview(ctx context.Context, txID string) ([]*model.Transaction, error) {
	transaction, err := l.fetchAndValidateInflightTransaction(ctx, txID)
	if err == nil {
		return []*model.Transaction{transaction}, nil
	}

	if !shouldExpandInflightParent(err) {
		return nil, err
	}

	legs, legErr := l.collectInflightLegs(ctx, txID)
	if legErr != nil {
		return nil, legErr
	}
	if len(legs) == 0 {
		return nil, err
	}
	return legs, nil
}

// buildInflightSettlementForPreview builds the settlement transaction one leg
// would produce, without recording it.
//
// The commit path's own validation decides the amount, so a projected commit
// refuses exactly what a real one would: an already-settled hold, or an amount
// beyond what remains.
func (l *Blnk) buildInflightSettlementForPreview(ctx context.Context, leg *model.Transaction, action string, amount *big.Int) (*model.Transaction, error) {
	// Work on a copy: validateAndUpdateAmount writes the settled amount onto
	// the transaction it is given.
	settlement := leg.Clone()

	if action == InflightActionVoid {
		amountLeft, err := l.calculateRemainingAmount(ctx, settlement)
		if err != nil {
			return nil, err
		}
		if amountLeft.Cmp(big.NewInt(0)) == 0 {
			return nil, fmt.Errorf("cannot void. Transaction already committed")
		}
		settlement.Status = StatusVoid
		settlement.PreciseAmount = amountLeft
		settlement.Amount = l.convertPreciseToFloat(amountLeft, settlement.Precision)
		return settlement, nil
	}

	requested := amount
	if requested == nil {
		requested = big.NewInt(0)
	}
	if err := l.validateAndUpdateAmount(ctx, settlement, requested); err != nil {
		return nil, err
	}
	settlement.Status = StatusCommit
	return settlement, nil
}
