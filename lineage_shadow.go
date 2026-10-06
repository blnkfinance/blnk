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
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// commitShadowTransactions commits the inflight shadow transactions of a parent
// transaction. A nil or non-positive amount commits every shadow in full. A
// positive amount is the part of the parent that was committed, and each shadow
// is committed only for its share of it, so a partial commit does not mark the
// whole allocation as spent.
func (l *Blnk) commitShadowTransactions(ctx context.Context, parentTransactionID string, amount *big.Int) error {
	ctx, span := tracer.Start(ctx, "CommitShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	commitAmounts, err := l.shadowCommitAmounts(ctx, shadowTxns, amount)
	if err != nil {
		return err
	}

	var failedShadows []string
	for i, shadow := range shadowTxns {
		shadowAmount := shadow.PreciseAmount
		if commitAmounts != nil {
			shadowAmount = commitAmounts[i]
			if shadowAmount.Sign() == 0 {
				continue
			}
		}
		_, err := l.CommitInflightTransaction(ctx, shadow.TransactionID, shadowAmount)
		if err != nil {
			if strings.Contains(err.Error(), "already committed") ||
				strings.Contains(err.Error(), "not in inflight status") {
				span.AddEvent("Shadow transaction already processed, skipping", trace.WithAttributes(
					attribute.String("shadow.id", shadow.TransactionID),
				))
				continue
			}
			logrus.Errorf("failed to commit shadow transaction %s: %v", shadow.TransactionID, err)
			failedShadows = append(failedShadows, shadow.TransactionID)
			continue
		}
		span.AddEvent("Shadow transaction committed", trace.WithAttributes(
			attribute.String("shadow.id", shadow.TransactionID),
			attribute.String("parent.id", parentTransactionID),
		))
	}

	if len(failedShadows) > 0 {
		return fmt.Errorf("failed to commit %d shadow transactions: %v", len(failedShadows), failedShadows)
	}

	return nil
}

// shadowCommitAmounts works out how much of each shadow transaction to commit
// when the parent transaction was committed for amount. It returns nil when
// every shadow should be committed in full: no amount was given, or a shadow has
// no precise amount to split.
func (l *Blnk) shadowCommitAmounts(ctx context.Context, shadows []model.Transaction, amount *big.Int) ([]*big.Int, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, nil
	}

	remaining := make([]*big.Int, len(shadows))
	for i, shadow := range shadows {
		if shadow.PreciseAmount == nil {
			return nil, nil
		}
		if shadow.Status != "" && shadow.Status != StatusInflight {
			remaining[i] = big.NewInt(0)
			continue
		}
		committed, err := l.datasource.GetTotalCommittedTransactions(ctx, shadow.TransactionID)
		if err != nil {
			return nil, fmt.Errorf("failed to get committed amount for shadow transaction %s: %w", shadow.TransactionID, err)
		}
		remaining[i] = new(big.Int).Sub(shadow.PreciseAmount, committed)
		if remaining[i].Sign() < 0 {
			remaining[i] = big.NewInt(0)
		}
	}

	return splitShadowCommit(shadows, remaining, amount), nil
}

// splitShadowCommit divides a commit of amount across the shadow transactions of
// a parent transaction. remaining[i] is the uncommitted part of shadows[i].
//
// Release shadows follow the allocation strategy the debit used: FIFO and LIFO
// fill the shadows in order, proportional splits the amount by each shadow's
// remaining share. A receive shadow commits what its provider's release shadow
// did; any other shadow commits up to the full amount.
func splitShadowCommit(shadows []model.Transaction, remaining []*big.Int, amount *big.Int) []*big.Int {
	commits := make([]*big.Int, len(shadows))
	for i := range commits {
		commits[i] = big.NewInt(0)
	}

	shadowMeta := func(shadow model.Transaction, key string) string {
		value, _ := shadow.MetaData[key].(string)
		return value
	}

	releaseTotal := big.NewInt(0)
	proportional := false
	for i, shadow := range shadows {
		if shadowMeta(shadow, "_lineage_type") != "release" {
			continue
		}
		releaseTotal.Add(releaseTotal, remaining[i])
		if shadowMeta(shadow, "_allocation") == AllocationProp {
			proportional = true
		}
	}

	byProvider := make(map[string]*big.Int)
	left := new(big.Int).Set(amount)
	for i, shadow := range shadows {
		if shadowMeta(shadow, "_lineage_type") != "release" {
			continue
		}
		take := new(big.Int)
		if proportional && releaseTotal.Sign() > 0 {
			take.Mul(remaining[i], amount)
			take.Quo(take, releaseTotal)
		} else {
			take.Set(left)
		}
		if take.Cmp(remaining[i]) > 0 {
			take.Set(remaining[i])
		}
		left.Sub(left, take)
		commits[i] = take
		byProvider[shadowMeta(shadow, "_provider")] = take
	}

	// Rounding in the proportional split can leave a few units unassigned.
	if proportional {
		for i, shadow := range shadows {
			if left.Sign() <= 0 {
				break
			}
			if shadowMeta(shadow, "_lineage_type") != "release" {
				continue
			}
			extra := new(big.Int).Sub(remaining[i], commits[i])
			if extra.Cmp(left) > 0 {
				extra.Set(left)
			}
			commits[i].Add(commits[i], extra)
			left.Sub(left, extra)
		}
	}

	for i, shadow := range shadows {
		if shadowMeta(shadow, "_lineage_type") == "release" {
			continue
		}
		take := new(big.Int).Set(amount)
		if released, ok := byProvider[shadowMeta(shadow, "_provider")]; ok {
			take.Set(released)
		}
		if take.Cmp(remaining[i]) > 0 {
			take.Set(remaining[i])
		}
		commits[i] = take
	}

	return commits
}

// voidShadowTransactions voids all inflight shadow transactions for a parent transaction.
// It attempts to void all shadows and returns an error if any fail (for outbox retry).
// Already-voided/committed shadows return "already committed" error which is treated as success.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID.
//
// Returns:
// - error: An error if any shadow transaction failed to void (excluding already-processed).
func (l *Blnk) voidShadowTransactions(ctx context.Context, parentTransactionID string) error {
	ctx, span := tracer.Start(ctx, "VoidShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	var failedShadows []string
	for _, shadow := range shadowTxns {
		_, err := l.VoidInflightTransaction(ctx, shadow.TransactionID)
		if err != nil {
			if strings.Contains(err.Error(), "already committed") ||
				strings.Contains(err.Error(), "not in inflight status") {
				span.AddEvent("Shadow transaction already processed, skipping", trace.WithAttributes(
					attribute.String("shadow.id", shadow.TransactionID),
				))
				continue
			}
			logrus.Errorf("failed to void shadow transaction %s: %v", shadow.TransactionID, err)
			failedShadows = append(failedShadows, shadow.TransactionID)
			continue
		}
		span.AddEvent("Shadow transaction voided", trace.WithAttributes(
			attribute.String("shadow.id", shadow.TransactionID),
			attribute.String("parent.id", parentTransactionID),
		))
	}

	if len(failedShadows) > 0 {
		return fmt.Errorf("failed to void %d shadow transactions: %v", len(failedShadows), failedShadows)
	}

	return nil
}

// inflightTransactionNeedsShadowWork reports whether an inflight commit or void should
// propagate to shadow transactions. Matches PrepareLineageOutbox semantics: debit shadows
// follow source TrackFundLineage; credit shadows require a provider and destination
// TrackFundLineage. Shadow children never have nested shadows.
//
// A balance lookup failure is treated as unknown eligibility: the error is recorded on
// the span and the function returns true so callers process shadows conservatively
// instead of skipping and stranding inflight companions.
func (l *Blnk) inflightTransactionNeedsShadowWork(ctx context.Context, txn *model.Transaction) bool {
	_, span := tracer.Start(ctx, "InflightTransactionNeedsShadowWork")
	defer span.End()

	if txn == nil {
		return false
	}
	if txn.MetaData != nil {
		if _, isShadow := txn.MetaData["_lineage_type"]; isShadow {
			return false
		}
		if _, hasAlloc := txn.MetaData[LineageFundAllocation]; hasAlloc {
			return true
		}
	}

	provider := l.getLineageProvider(txn)
	if provider != "" {
		dst, err := l.datasource.GetBalanceByIDLite(txn.Destination)
		if err != nil {
			span.RecordError(err)
			return true
		}
		return dst != nil && dst.TrackFundLineage
	}

	// Only real balance IDs can track lineage; @ indicators (e.g. @world) cannot.
	if strings.HasPrefix(txn.Source, "@") {
		return false
	}

	src, err := l.datasource.GetBalanceByIDLite(txn.Source)
	if err != nil {
		span.RecordError(err)
		return true
	}
	return src != nil && src.TrackFundLineage
}

// queueShadowWork processes shadow commit or void work synchronously first, and queues
// to outbox for retry only if there are failures. This provides both immediate processing
// and guaranteed delivery for failed operations.
//
// parentTransactionID is the original inflight transaction ID. Callers must pass the ID
// captured before finalizeCommitment/finalizeVoidTransaction reassign txn.TransactionID
// to the commit/void child row — shadows are keyed by _shadow_for on that original ID.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The original inflight parent transaction ID.
// - txn *model.Transaction: The parent inflight transaction (for lineage eligibility).
// - lineageType string: Either LineageTypeShadowCommit or LineageTypeShadowVoid.
//
// Returns:
// - error: An error if all processing attempts failed.
func (l *Blnk) queueShadowWork(ctx context.Context, parentTransactionID string, txn *model.Transaction, lineageType string) error {
	if !l.inflightTransactionNeedsShadowWork(ctx, txn) {
		return nil
	}

	ctx, span := tracer.Start(ctx, "QueueShadowWork")
	defer span.End()

	var processingErr error

	// txn carries the amount that was just committed, which a partial commit
	// must pass on to the shadows.
	commitAmount := txn.PreciseAmount

	// Try to process shadows synchronously first
	switch lineageType {
	case model.LineageTypeShadowCommit:
		processingErr = l.commitShadowTransactions(ctx, parentTransactionID, commitAmount)
	case model.LineageTypeShadowVoid:
		processingErr = l.voidShadowTransactions(ctx, parentTransactionID)
	}

	// If synchronous processing succeeded, we're done
	if processingErr == nil {
		span.AddEvent("Shadow work processed synchronously", trace.WithAttributes(
			attribute.String("parent.id", parentTransactionID),
			attribute.String("lineage.type", lineageType),
		))
		return nil
	}

	// Synchronous processing failed - queue to outbox for retry
	logrus.Warnf("Shadow %s failed for %s, queueing for retry: %v", lineageType, parentTransactionID, processingErr)

	// Create outbox entry for shadow work retry
	// Use a distinct ID to avoid conflict with regular lineage entries for same transaction
	shadowWorkID := fmt.Sprintf("%s_%s", parentTransactionID, lineageType)
	outbox := &model.LineageOutbox{
		TransactionID: shadowWorkID,
		LineageType:   lineageType,
		Payload:       fmt.Appendf(nil, `{"parent_transaction_id":"%s"}`, parentTransactionID),
		MaxAttempts:   5,
	}
	if lineageType == model.LineageTypeShadowCommit && commitAmount != nil {
		outbox.Payload = fmt.Appendf(nil, `{"parent_transaction_id":"%s","amount":"%s"}`, parentTransactionID, commitAmount.String())
	}

	if err := l.datasource.InsertLineageOutbox(ctx, outbox); err != nil {
		span.RecordError(err)
		// Log but don't fail - the original error is more important
		logrus.Errorf("failed to queue shadow work for retry: %v", err)
		return processingErr
	}

	span.AddEvent("Shadow work queued for retry via outbox", trace.WithAttributes(
		attribute.String("parent.id", parentTransactionID),
		attribute.String("lineage.type", lineageType),
		attribute.String("original.error", processingErr.Error()),
	))

	// Return original error since processing failed
	return processingErr
}
