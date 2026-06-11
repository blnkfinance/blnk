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
	"encoding/json"
	"fmt"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) PrepareLineageOutbox(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.LineageOutbox {
	_, span := tracer.Start(ctx, "PrepareLineageOutbox")
	defer span.End()

	provider := l.getLineageProvider(txn)

	// Determine what type of lineage processing is needed
	needsCredit := provider != "" && destinationBalance != nil && destinationBalance.TrackFundLineage
	needsDebit := sourceBalance != nil && sourceBalance.TrackFundLineage

	if !needsCredit && !needsDebit {
		span.AddEvent("No lineage processing needed")
		return nil
	}

	lineageType := "both"
	if needsCredit && !needsDebit {
		lineageType = "credit"
	} else if needsDebit && !needsCredit {
		lineageType = "debit"
	}

	// Build the payload with transaction data needed for processing
	payload := LineageOutboxPayload{
		Amount:        txn.Amount,
		PreciseAmount: txn.PreciseAmount.String(),
		Currency:      txn.Currency,
		Precision:     txn.Precision,
		Reference:     txn.Reference,
		SkipQueue:     true,
		Inflight:      txn.Inflight,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		logrus.Errorf("failed to marshal lineage outbox payload: %v", err)
		span.RecordError(err)
		return nil
	}

	var srcID, dstID string
	if sourceBalance != nil {
		srcID = sourceBalance.BalanceID
	}
	if destinationBalance != nil {
		dstID = destinationBalance.BalanceID
	}

	outbox := &model.LineageOutbox{
		TransactionID:        txn.TransactionID,
		SourceBalanceID:      srcID,
		DestinationBalanceID: dstID,
		Provider:             provider,
		LineageType:          lineageType,
		Payload:              payloadBytes,
		MaxAttempts:          5,
		Inflight:             txn.Inflight, // explicit column, don't rely on JSON payload
	}

	span.AddEvent("Lineage outbox entry prepared", trace.WithAttributes(
		attribute.String("transaction_id", txn.TransactionID),
		attribute.String("lineage_type", lineageType),
		attribute.String("provider", provider),
	))

	return outbox
}

// ProcessLineageFromOutbox processes a lineage outbox entry.
// This is called by the outbox worker to perform deferred lineage processing.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - entry model.LineageOutbox: The outbox entry to process.
//
// Returns:
// - error: An error if processing fails.
func (l *Blnk) ProcessLineageFromOutbox(ctx context.Context, entry model.LineageOutbox) error {
	ctx, span := tracer.Start(ctx, "ProcessLineageFromOutbox")
	defer span.End()

	span.SetAttributes(
		attribute.String("outbox.id", fmt.Sprintf("%d", entry.ID)),
		attribute.String("outbox.transaction_id", entry.TransactionID),
		attribute.String("outbox.lineage_type", entry.LineageType),
	)

	// Handle shadow commit/void operations
	switch entry.LineageType {
	case model.LineageTypeShadowCommit, model.LineageTypeShadowVoid:
		// Extract parent transaction ID from payload
		var payload struct {
			ParentTransactionID string `json:"parent_transaction_id"`
		}
		if err := json.Unmarshal(entry.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal shadow work payload: %w", err)
		}
		parentTxnID := payload.ParentTransactionID
		if parentTxnID == "" {
			return fmt.Errorf("parent_transaction_id missing in shadow work payload")
		}

		if entry.LineageType == model.LineageTypeShadowCommit {
			span.AddEvent("Processing shadow commit from outbox", trace.WithAttributes(
				attribute.String("parent.transaction_id", parentTxnID),
			))
			if err := l.commitShadowTransactions(ctx, parentTxnID, nil); err != nil {
				return fmt.Errorf("failed to commit shadow transactions: %w", err)
			}
			span.AddEvent("Shadow commit completed from outbox")
		} else {
			span.AddEvent("Processing shadow void from outbox", trace.WithAttributes(
				attribute.String("parent.transaction_id", parentTxnID),
			))
			if err := l.voidShadowTransactions(ctx, parentTxnID); err != nil {
				return fmt.Errorf("failed to void shadow transactions: %w", err)
			}
			span.AddEvent("Shadow void completed from outbox")
		}
		return nil
	}

	// Handle regular lineage processing (credit, debit, both)
	// Fetch the transaction
	txn, err := l.GetTransaction(ctx, entry.TransactionID)
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}

	txn.Inflight = entry.Inflight

	// Fetch balances
	var sourceBalance, destinationBalance *model.Balance
	if entry.SourceBalanceID != "" {
		sourceBalance, err = l.datasource.GetBalanceByIDLite(entry.SourceBalanceID)
		if err != nil {
			logrus.Warnf("failed to get source balance %s for lineage processing: %v", entry.SourceBalanceID, err)
		}
	}
	if entry.DestinationBalanceID != "" {
		destinationBalance, err = l.datasource.GetBalanceByIDLite(entry.DestinationBalanceID)
		if err != nil {
			logrus.Warnf("failed to get destination balance %s for lineage processing: %v", entry.DestinationBalanceID, err)
		}
	}

	if err := l.processLineage(ctx, txn, sourceBalance, destinationBalance); err != nil {
		return err
	}

	span.AddEvent("Lineage processing completed from outbox")
	return nil
}
