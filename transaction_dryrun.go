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
	"strings"

	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// PreviewTransaction projects the effect of a transaction on its source and
// destination balances and returns the result without applying it.
//
// The projection runs the same arithmetic a real post runs — processBalances,
// and through it UpdateBalances and canProcessTransaction — against deep copies
// of the balances, so a preview cannot drift from enforcement as the posting
// path evolves. Nothing on this path changes ledger state: no transaction row,
// no balance update, no queue entry, no webhook, no hook, and the reference is
// not consumed.
//
// The answer is advisory. It describes the balances as they stand now, and
// another transaction may change them before the caller posts for real. Callers
// that need the funds held should use inflight instead.
func (l *Blnk) PreviewTransaction(ctx context.Context, transaction *model.Transaction) (*model.TransactionPreview, error) {
	ctx, span := tracer.Start(ctx, "PreviewTransaction")
	defer span.End()

	if transaction == nil {
		return nil, fmt.Errorf("transaction is required")
	}

	// The arithmetic below writes PreciseAmount onto the transaction, so work
	// from a copy and leave the caller's object untouched.
	projected := transaction.Clone()
	normalizePreviewStatus(projected)
	projected.PreciseAmount = model.ApplyPrecision(projected)

	if len(projected.Sources) > 0 || len(projected.Destinations) > 0 {
		return l.previewSplitTransaction(ctx, projected)
	}

	preview, err := l.previewSingleTransaction(ctx, projected)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.SetAttributes(attribute.Bool("preview.would_apply", preview.WouldApply))
	return preview, nil
}

// PreviewRefund projects the reversal of an existing transaction without
// creating it.
//
// It runs the same lookup a real refund runs — GetRefundableTransactionsByParentID,
// batch size 1 — rather than a separate hand-rolled eligibility check. That
// query is a single WHERE clause covering both "does this id exist" and "is
// it in a refundable status"; a real refund can't and doesn't distinguish
// the two; either one comes back as an empty result set, reported as "no
// transaction to refund" (404). A preview with its own, more detailed
// eligibility check would answer a different question than the one the real
// endpoint actually answers, and its rejection code would stop matching what
// a real post returns — the one thing a preview promises. Nothing is
// written: the refund is never queued and the parent is not marked refunded.
func (l *Blnk) PreviewRefund(ctx context.Context, transactionID string) (*model.TransactionPreview, error) {
	ctx, span := tracer.Start(ctx, "PreviewRefund")
	defer span.End()

	refundable, err := l.datasource.GetRefundableTransactionsByParentID(ctx, transactionID, 1, 0)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if len(refundable) == 0 {
		err := fmt.Errorf("transaction %s not found for refund", transactionID)
		span.RecordError(err)
		return nil, err
	}
	originalTxn := refundable[0]

	// Same builder the real refund uses: source and destination swapped,
	// overdraft allowed, status reset. skipQueue is irrelevant here because the
	// projection never reaches the queue.
	refund := prepareRefundTransaction(originalTxn, RefundOptions{SkipQueue: true})

	preview, err := l.PreviewTransaction(ctx, refund)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	preview.AddNote(fmt.Sprintf("projected refund of transaction %s", originalTxn.TransactionID))
	return preview, nil
}

// normalizePreviewStatus assigns the status the transaction would carry by the
// time balances are applied.
//
// A real create picks this up from setTransactionStatus and updateTransactionDetails
// as it passes through the queue; a preview bypasses both, so an unset status
// would otherwise reach the apply path empty.
func normalizePreviewStatus(transaction *model.Transaction) {
	switch transaction.Status {
	case StatusCommit, StatusVoid:
		// Inflight settlement statuses select a different apply branch and are
		// set deliberately by the caller.
		return
	}

	if transaction.Inflight {
		transaction.Status = StatusInflight
		return
	}
	transaction.Status = StatusApplied
}

// previewSingleTransaction projects one source-to-destination movement.
func (l *Blnk) previewSingleTransaction(ctx context.Context, transaction *model.Transaction) (*model.TransactionPreview, error) {
	ctx, span := tracer.Start(ctx, "PreviewSingleTransaction")
	defer span.End()

	source, destination, err := l.resolveBalancesForPreview(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Hold the same locks a real post would, so both balances are read as of a
	// single consistent moment rather than torn across two reads. Acquired
	// directly rather than through executeWithLock: a preview must not record
	// hot-pair contention and steer hot-lane routing for real traffic.
	locker, err := l.acquireLock(ctx, source.balance.BalanceID, destination.balance.BalanceID)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	preview := newPreviewFor(transaction)
	l.notePreviewCaveats(ctx, preview, transaction, source, destination)

	// Snapshot before applying: processBalances mutates what it is given.
	sourceBefore, destinationBefore := source.balance.Clone(), destination.balance.Clone()

	applyErr := l.processBalances(ctx, transaction, source.balance, destination.balance)
	if applyErr != nil {
		preview.WouldApply = false
		preview.Rejection = previewRejection(applyErr)
	} else if preview.Rejection == nil {
		// A caveat recorded above (e.g. a duplicate reference) already
		// rejected this preview; balances applying cleanly on top of that
		// doesn't undo it.
		preview.WouldApply = true
	}

	// On rejection the balances hold whatever partial state the apply path left
	// behind — or, for a caveat-triggered rejection like a duplicate
	// reference, balances applied cleanly even though the preview as a whole
	// didn't — neither is a meaningful projection. Report the unchanged
	// snapshot as the outcome instead.
	sourceAfter, destinationAfter := source.balance, destination.balance
	if !preview.WouldApply {
		sourceAfter, destinationAfter = sourceBefore, destinationBefore
	}

	preview.PreciseAmount = preciseString(transaction.PreciseAmount)
	preview.Amount = transaction.Amount
	preview.Balances = []model.BalanceProjection{
		balanceProjection(model.PreviewRoleSource, sourceBefore, sourceAfter, source.virtual),
		balanceProjection(model.PreviewRoleDestination, destinationBefore, destinationAfter, destination.virtual),
	}

	span.AddEvent("Transaction projected", trace.WithAttributes(
		attribute.Bool("preview.would_apply", preview.WouldApply),
	))
	return preview, nil
}

// previewSplitTransaction projects a multi-source or multi-destination
// transaction, one entry per split.
//
// How the split would really execute decides how it is projected, mirroring
// the same distinction the bulk endpoint makes:
//
//	skip_queue: true   processTxns records each leg synchronously, one after
//	                    another, and stops at the first one that fails —
//	                    later legs are never attempted. The projection
//	                    carries balances forward across legs and stops
//	                    projecting as soon as one fails, for the same
//	                    reason: it never really ran either.
//
//	skip_queue: false  each leg is persisted and queued for independent
//	                    async processing with no ordering guarantee, so one
//	                    leg's outcome doesn't depend on another's. Each leg
//	                    here is projected on its own, against the balances
//	                    as they stand, not carrying an earlier leg's effect
//	                    forward.
func (l *Blnk) previewSplitTransaction(ctx context.Context, transaction *model.Transaction) (*model.TransactionPreview, error) {
	ctx, span := tracer.Start(ctx, "PreviewSplitTransaction")
	defer span.End()

	legs, err := transaction.SplitTransactionPrecise(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to split transaction: %w", err)
	}

	preview := newPreviewFor(transaction)
	preview.WouldApply = true
	preview.PreciseAmount = preciseString(transaction.PreciseAmount)
	preview.Amount = transaction.Amount

	cumulative := transaction.SkipQueue
	if !cumulative {
		preview.AddNote("skip_queue is false: legs are queued for independent async processing with no ordering guarantee, so each is projected on its own rather than cumulatively")
	}

	// Shared only in cumulative mode, so a balance touched twice accumulates
	// exactly as it would in a real sequential apply. Independent mode gives
	// each leg its own fresh set instead, so one leg's projection can't leak
	// into another's the way it could for legs that will really run at
	// different, uncoordinated times.
	shared := newPreviewBalanceSet(l)

	for _, leg := range legs {
		normalizePreviewStatus(leg)
		leg.PreciseAmount = model.ApplyPrecision(leg)

		set := shared
		if !cumulative {
			set = newPreviewBalanceSet(l)
		}

		source, destination, err := set.resolvePair(ctx, leg)
		if err != nil {
			span.RecordError(err)
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

		// A failed leg was never actually recorded, so its would-be amount
		// doesn't belong next to the legs that really would apply.
		if !failed {
			role, identifier := model.PreviewRoleDestination, leg.Destination
			if len(transaction.Sources) > 0 {
				role, identifier = model.PreviewRoleSource, leg.Source
			}
			preview.Legs = append(preview.Legs, model.LegProjection{
				Identifier:    identifier,
				Role:          role,
				PreciseAmount: preciseString(leg.PreciseAmount),
				Amount:        leg.Amount,
			})
		}

		if !cumulative {
			preview.Balances = append(preview.Balances, set.projections()...)
		} else if failed {
			// Mirrors processTxns: a real skip_queue: true split stops at
			// the first failing leg and never attempts the rest.
			break
		}
	}

	if cumulative {
		preview.Balances = shared.projections()
	}

	return preview, nil
}

// previewBalance is a balance resolved for projection, along with whether it
// had to be invented because it does not exist yet.
type previewBalance struct {
	balance *model.Balance
	before  *model.Balance
	virtual bool
	role    string
}

// resolveBalancesForPreview loads the source and destination balances without
// creating anything.
//
// A real post resolves @indicators through getOrCreateBalanceByIndicator, which
// creates the balance when it is missing. A preview must not, so an unknown
// indicator is projected against a zeroed stand-in and reported as virtual.
func (l *Blnk) resolveBalancesForPreview(ctx context.Context, transaction *model.Transaction) (*previewBalance, *previewBalance, error) {
	source, err := l.resolveBalanceForPreview(ctx, transaction.Source, transaction.Currency, model.PreviewRoleSource)
	if err != nil {
		return nil, nil, err
	}

	destination, err := l.resolveBalanceForPreview(ctx, transaction.Destination, transaction.Currency, model.PreviewRoleDestination)
	if err != nil {
		return nil, nil, err
	}

	return source, destination, nil
}

func (l *Blnk) resolveBalanceForPreview(ctx context.Context, identifier, currency, role string) (*previewBalance, error) {
	_, span := tracer.Start(ctx, "ResolveBalanceForPreview")
	defer span.End()

	if identifier == "" {
		return nil, fmt.Errorf("%s is required", role)
	}

	if strings.HasPrefix(identifier, "@") {
		balance, err := l.datasource.GetBalanceByIndicator(identifier, currency)
		if err != nil {
			// The indicator has no balance yet. A real post would create one
			// and start it at zero, so project against that rather than
			// creating it here.
			return &previewBalance{
				balance: &model.Balance{
					BalanceID: identifier,
					Indicator: identifier,
					Currency:  currency,
					LedgerID:  GeneralLedgerID,
				},
				virtual: true,
				role:    role,
			}, nil
		}
		return &previewBalance{balance: balance, role: role}, nil
	}

	balance, err := l.fetchBalanceForPreview(identifier)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &previewBalance{balance: balance, role: role}, nil
}

// fetchBalanceForPreview mirrors the fetch getSourceAndDestination performs,
// including the queued-checks variant.
//
// This has to match: queued debits are only loaded by the withQueued fetch, and
// canProcessTransaction subtracts them only when present. Reading the lighter
// row while the deployment enforces queued checks would let a preview approve a
// transaction the real post then rejects.
func (l *Blnk) fetchBalanceForPreview(balanceID string) (*model.Balance, error) {
	if l.config != nil && l.config.Transaction.EnableQueuedChecks {
		return l.datasource.GetBalanceByID(balanceID, []string{}, true)
	}
	return l.datasource.GetBalanceByIDLite(balanceID)
}

// previewBalanceSet caches balances across the legs of a split so that a
// balance touched more than once accumulates, and so each distinct balance is
// read once.
type previewBalanceSet struct {
	blnk  *Blnk
	order []string
	byID  map[string]*previewBalance
}

func newPreviewBalanceSet(l *Blnk) *previewBalanceSet {
	return &previewBalanceSet{blnk: l, byID: make(map[string]*previewBalance)}
}

func (s *previewBalanceSet) resolvePair(ctx context.Context, transaction *model.Transaction) (*previewBalance, *previewBalance, error) {
	source, err := s.resolve(ctx, transaction.Source, transaction.Currency, model.PreviewRoleSource)
	if err != nil {
		return nil, nil, err
	}
	destination, err := s.resolve(ctx, transaction.Destination, transaction.Currency, model.PreviewRoleDestination)
	if err != nil {
		return nil, nil, err
	}
	return source, destination, nil
}

func (s *previewBalanceSet) resolve(ctx context.Context, identifier, currency, role string) (*previewBalance, error) {
	if existing, ok := s.byID[identifier]; ok {
		return existing, nil
	}

	resolved, err := s.blnk.resolveBalanceForPreview(ctx, identifier, currency, role)
	if err != nil {
		return nil, err
	}
	resolved.before = resolved.balance.Clone()

	s.byID[identifier] = resolved
	s.order = append(s.order, identifier)
	return resolved, nil
}

func (s *previewBalanceSet) projections() []model.BalanceProjection {
	projections := make([]model.BalanceProjection, 0, len(s.order))
	for _, id := range s.order {
		entry := s.byID[id]
		projections = append(projections, balanceProjection(entry.role, entry.before, entry.balance, entry.virtual))
	}
	return projections
}

// newPreviewFor builds the projection shell shared by every preview shape.
func newPreviewFor(transaction *model.Transaction) *model.TransactionPreview {
	return &model.TransactionPreview{
		DryRun:    true,
		Status:    transaction.Status,
		Reference: transaction.Reference,
		Currency:  transaction.Currency,
		Precision: transaction.Precision,
	}
}

// sameBalanceErr reports the error a real post would fail with when source
// and destination resolve to the same balance row, or nil if they don't.
//
// A real post fetches and updates source and destination as two
// independent balance reads, each guarded by optimistic locking. When
// they're the same row, the first update advances its version and the
// second's WHERE version=<stale> clause matches nothing — a guaranteed
// conflict every single time, not a rare race. In-memory preview
// arithmetic has no such check, so without this a self-transfer would
// always preview as would_apply: true for a transfer that can never
// actually apply.
func sameBalanceErr(source, destination *previewBalance) error {
	if source.balance.BalanceID != destination.balance.BalanceID {
		return nil
	}
	return fmt.Errorf("Optimistic locking failure: balance with ID '%s' may have been updated or deleted by another transaction", source.balance.BalanceID)
}

// notePreviewCaveats records conditions worth surfacing that are not rejections.
func (l *Blnk) notePreviewCaveats(ctx context.Context, preview *model.TransactionPreview, transaction *model.Transaction, source, destination *previewBalance) {
	if err := sameBalanceErr(source, destination); err != nil {
		preview.WouldApply = false
		preview.Rejection = previewRejection(err)
	}

	if !transaction.ScheduledFor.IsZero() {
		preview.AddNote("scheduled_for is ignored in a dry run; the projection shows the effect as if applied now")
	}

	// The apply path does not compare a transaction's currency against its
	// balances, so a mismatch is projected rather than rejected — but it is
	// almost always a mistake, and better seen here than after posting.
	for _, resolved := range []*previewBalance{source, destination} {
		if resolved.virtual || resolved.balance.Currency == "" || transaction.Currency == "" {
			continue
		}
		if resolved.balance.Currency != transaction.Currency {
			preview.AddNote(fmt.Sprintf(
				"currency mismatch: transaction is %s but %s balance %s is %s; the ledger applies this as raw minor units",
				transaction.Currency, resolved.role, resolved.balance.BalanceID, resolved.balance.Currency,
			))
		}
	}

	if transaction.Reference != "" {
		if exists, err := l.datasource.TransactionExistsByRef(ctx, transaction.Reference); err == nil && exists {
			// A duplicate reference is fatal on a real post (transaction
			// validation rejects it before balances are ever touched), so this
			// is a rejection like any other — not a caveat balance projection
			// can still say would_apply: true around.
			preview.WouldApply = false
			preview.Rejection = previewRejection(fmt.Errorf("transaction validation failed: reference %s has already been used", transaction.Reference))
		}
	}
}

// previewRejection converts an apply-path error into the projected rejection,
// reusing the same reason vocabulary a real rejection is recorded under.
//
// Code is left for the API layer to fill in: the message-to-code table lives
// there alongside the classifier every other endpoint uses, and duplicating it
// here would let the two drift.
func previewRejection(err error) *model.PreviewRejection {
	message := err.Error()
	return &model.PreviewRejection{
		Reason:  categorizeRejectionReason(message),
		Message: message,
	}
}

// balanceProjection renders one balance's before and after state.
func balanceProjection(role string, before, after *model.Balance, virtual bool) model.BalanceProjection {
	before, after = before.Clone(), after.Clone()
	before.InitializeBalanceFields()
	after.InitializeBalanceFields()

	return model.BalanceProjection{
		BalanceID: before.BalanceID,
		Role:      role,
		Currency:  before.Currency,
		Virtual:   virtual,

		CurrentBalance:               preciseString(before.Balance),
		CurrentAvailable:             preciseString(availableBalance(before)),
		CurrentCreditBalance:         preciseString(before.CreditBalance),
		CurrentDebitBalance:          preciseString(before.DebitBalance),
		CurrentInflightDebitBalance:  preciseString(before.InflightDebitBalance),
		CurrentInflightCreditBalance: preciseString(before.InflightCreditBalance),

		ResultingBalance:               preciseString(after.Balance),
		ResultingAvailable:             preciseString(availableBalance(after)),
		ResultingCreditBalance:         preciseString(after.CreditBalance),
		ResultingDebitBalance:          preciseString(after.DebitBalance),
		ResultingInflightDebitBalance:  preciseString(after.InflightDebitBalance),
		ResultingInflightCreditBalance: preciseString(after.InflightCreditBalance),
	}
}

// availableBalance computes spendable funds the way canProcessTransaction does,
// so the figure shown matches the one enforcement will use.
func availableBalance(balance *model.Balance) *big.Int {
	available := new(big.Int).Sub(balance.Balance, balance.InflightDebitBalance)
	if balance.QueuedDebitBalance != nil {
		available = new(big.Int).Sub(available, balance.QueuedDebitBalance)
	}
	return available
}

func preciseString(value *big.Int) string {
	if value == nil {
		return "0"
	}
	return value.String()
}
