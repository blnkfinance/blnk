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

	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// processLineage tracks fund lineage for a transaction. A benign provider
// mismatch is not an error; database/processing failures are returned so the
// outbox worker retries the entry.
func (l *Blnk) processLineage(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessLineage")
	defer span.End()

	provider := l.getLineageProvider(txn)

	validatedProvider, err := l.validateLineageProvider(ctx, provider, sourceBalance)
	if err != nil {
		span.RecordError(err)
		notification.NotifyError(err)
		return fmt.Errorf("lineage provider validation failed: %w", err)
	}

	if provider != "" && validatedProvider == "" && sourceBalance != nil {
		span.AddEvent("Provider validation failed", trace.WithAttributes(
			attribute.String("requested_provider", provider),
			attribute.String("source_balance_id", sourceBalance.BalanceID),
		))
	}

	// Credit must run before debit: debit allocates from the shadow balances credit creates.
	if validatedProvider != "" && destinationBalance != nil && destinationBalance.TrackFundLineage {
		if err := l.processLineageCredit(ctx, txn, destinationBalance, validatedProvider); err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
			return fmt.Errorf("lineage credit processing failed: %w", err)
		}
	}

	// Debit processing doesn't require a provider - it allocates from existing shadow balances
	if sourceBalance != nil && sourceBalance.TrackFundLineage {
		if err := l.processLineageDebit(ctx, txn, sourceBalance, destinationBalance); err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
			return fmt.Errorf("lineage debit processing failed: %w", err)
		}
	}

	span.AddEvent("Lineage processing completed")
	return nil
}

// getLineageProvider extracts the fund provider from the transaction metadata.
//
// Parameters:
// - txn *model.Transaction: The transaction to extract the provider from.
//
// Returns:
// - string: The provider identifier, or empty string if not set.
func (l *Blnk) getLineageProvider(txn *model.Transaction) string {
	if txn.MetaData == nil {
		return ""
	}
	provider, ok := txn.MetaData[LineageProviderKey].(string)
	if !ok {
		return ""
	}
	return provider
}

// validateLineageProvider checks if the specified provider exists on the source balance.
// If the source tracks fund lineage but doesn't have the specified provider,
// returns empty string (provider should be ignored).
// If the source doesn't track lineage (e.g., @world), any provider is valid.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - provider string: The provider specified in transaction metadata.
// - sourceBalance *model.Balance: The source balance (may be nil or not track lineage).
//
// Returns:
// - string: The validated provider name (empty if invalid/should be ignored).
// - error: An error if validation fails (database errors only, not for invalid providers).
func (l *Blnk) validateLineageProvider(ctx context.Context, provider string, sourceBalance *model.Balance) (string, error) {
	if provider == "" {
		return "", nil
	}

	// Source is nil or doesn't track lineage - provider is valid
	if sourceBalance == nil || !sourceBalance.TrackFundLineage {
		return provider, nil
	}

	// Source tracks lineage - verify provider exists
	mapping, err := l.datasource.GetLineageMappingByProvider(ctx, sourceBalance.BalanceID, provider)
	if err != nil {
		return "", fmt.Errorf("failed to validate provider on source: %w", err)
	}

	if mapping == nil {
		logrus.Warnf("lineage provider validation: provider %q does not exist on source balance %s - ignoring provider",
			provider, sourceBalance.BalanceID)
		return "", nil
	}

	return provider, nil
}

// processLineageCredit processes a credit transaction for fund lineage tracking.
// It creates shadow balances and queues a shadow transaction to track the provider's funds.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - txn *model.Transaction: The credit transaction being processed.
// - destBalance *model.Balance: The destination balance receiving the funds.
// - provider string: The identifier of the fund provider.
//
// Returns:
// - error: An error if the credit processing fails.
