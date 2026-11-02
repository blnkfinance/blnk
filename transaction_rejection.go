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
	"errors"
	"fmt"
	"strings"

	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RejectTransaction")
	defer span.End()

	// A rejected item of a queued atomic bulk request fails its batch.
	var batchID string
	if transaction.Atomic {
		parentTransactionID, ok := transaction.MetaData["QUEUED_PARENT_TRANSACTION"].(string)
		if !ok {
			return nil, fmt.Errorf("parent transaction ID not found in meta data")
		}
		batchID = parentTransactionID

		// The batch is marked before anything is recorded: if the mark is not
		// saved, the item is left for the worker to retry.
		if err := l.markQueuedAtomicBatchFailed(ctx, batchID); err != nil {
			span.RecordError(err)
			return nil, err
		}

		// An item recorded before (rejected by an attempt whose rollback did
		// not finish, or applied before its batch failed) is not recorded
		// again: this attempt only finishes the rollback.
		recorded, err := l.datasource.TransactionExistsByRef(ctx, transaction.Reference)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if recorded {
			if err := l.failQueuedAtomicBatch(ctx, errors.New("transaction rejected"), batchID, transaction.Inflight); err != nil {
				span.RecordError(err)
				return nil, err
			}
			existing, err := l.datasource.GetTransactionByRef(ctx, transaction.Reference)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			return &existing, nil
		}
	}

	// Update the transaction status to rejected
	transaction.Status = StatusRejected

	// Initialize MetaData if it's nil and add the rejection reason
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}
	transaction.MetaData["blnk_rejection_reason"] = reason

	// Fill AmountString (including "0") before insert. Recovery copies of
	// zero-amount QUEUED parents otherwise persist "" into a numeric column.
	model.ApplyPrecision(transaction)

	// Persist the transaction with the updated status and metadata
	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		logrus.WithError(err).Error("failed to save transaction to db")
		return nil, err
	}

	span.AddEvent("Transaction rejected", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	// Record rejection metrics.
	rejectionReason := categorizeRejectionReason(reason)
	metrics.TransactionRejectedTotal.Add(ctx, 1,
		otelmetric.WithAttributes(attribute.String("reason", rejectionReason)),
	)
	metrics.TransactionTotal.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("status", StatusRejected),
			attribute.String("currency", transaction.Currency),
		),
	)

	// For rejected transactions, no balances were updated, so pass nil
	l.postTransactionActions(ctx, transaction, nil, nil)

	if batchID != "" {
		logrus.Info(transaction.ParentTransaction, "parent transaction", transaction.Atomic, "atomic", transaction.Inflight, "inflight")
		if err := l.failQueuedAtomicBatch(ctx, errors.New("transaction rejected"), batchID, transaction.Inflight); err != nil {
			span.RecordError(err)
			return nil, err
		}
	}
	return transaction, nil
}

// categorizeRejectionReason maps a free-text rejection reason to a bounded set of metric labels
// to keep Prometheus cardinality under control.
func categorizeRejectionReason(reason string) string {
	lower := strings.ToLower(reason)
	switch {
	case strings.Contains(lower, "insufficient funds"):
		return "insufficient_funds"
	case strings.Contains(lower, "overdraft limit"):
		return "overdraft_limit"
	case hotpairs.IsLockContentionError(errors.New(reason)):
		return "lock_contention"
	case strings.Contains(lower, "exceeded max"):
		return "max_retries"
	default:
		return "other"
	}
}
