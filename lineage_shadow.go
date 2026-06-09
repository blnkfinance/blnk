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

func (l *Blnk) commitShadowTransactions(ctx context.Context, parentTransactionID string, amount *big.Int) error {
	ctx, span := tracer.Start(ctx, "CommitShadowTransactions")
	defer span.End()

	shadowTxns, err := l.datasource.GetTransactionsByShadowFor(ctx, parentTransactionID)
	if err != nil {
		return fmt.Errorf("failed to get shadow transactions: %w", err)
	}

	var failedShadows []string
	for _, shadow := range shadowTxns {
		_, err := l.CommitInflightTransaction(ctx, shadow.TransactionID, shadow.PreciseAmount)
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

// queueShadowWork processes shadow commit or void work synchronously first, and queues
// to outbox for retry only if there are failures. This provides both immediate processing
// and guaranteed delivery for failed operations.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The parent transaction ID whose shadows need processing.
// - lineageType string: Either LineageTypeShadowCommit or LineageTypeShadowVoid.
//
// Returns:
// - error: An error if all processing attempts failed.
func (l *Blnk) queueShadowWork(ctx context.Context, parentTransactionID string, lineageType string) error {
	ctx, span := tracer.Start(ctx, "QueueShadowWork")
	defer span.End()

	var processingErr error

	// Try to process shadows synchronously first
	switch lineageType {
	case model.LineageTypeShadowCommit:
		processingErr = l.commitShadowTransactions(ctx, parentTransactionID, nil)
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
		Payload:       []byte(fmt.Sprintf(`{"parent_transaction_id":"%s"}`, parentTransactionID)),
		MaxAttempts:   5,
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

// PrepareLineageOutbox creates a LineageOutbox entry for atomic insertion with the transaction.
// This ensures lineage processing intent is captured in the same database transaction,
// guaranteeing no lineage work is lost even if subsequent async operations fail.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The transaction being processed.
// - sourceBalance *model.Balance: The source balance (may be nil).
// - destinationBalance *model.Balance: The destination balance (may be nil).
//
// Returns:
// - *model.LineageOutbox: The outbox entry to insert, or nil if no lineage processing needed.
