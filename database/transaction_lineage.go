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

package database

import (
	"context"
	"encoding/json"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetTransactionsByShadowFor(ctx context.Context, parentTransactionID string) ([]model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransactionsByShadowFor")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, "precision", currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE meta_data->>'_shadow_for' = $1
		ORDER BY created_at ASC
	`, parentTransactionID)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve shadow transactions", err)
	}
	defer func() { _ = rows.Close() }()

	var transactions []model.Transaction
	for rows.Next() {
		var transaction model.Transaction
		var metaDataJSON []byte
		var preciseAmountStr string

		err := rows.Scan(
			&transaction.TransactionID,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&preciseAmountStr,
			&transaction.Precision,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.CreatedAt,
			&metaDataJSON,
			&transaction.ScheduledFor,
			&transaction.Hash,
		)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan shadow transaction", err)
		}

		if err := json.Unmarshal(metaDataJSON, &transaction.MetaData); err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transaction.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Invalid shadow transaction precise amount", err)
		}
		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating shadow transactions", err)
	}

	span.AddEvent("Shadow transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
		attribute.String("parent_transaction_id", parentTransactionID),
	))
	return transactions, nil
}

// GetAllTransactionsWithFilter retrieves transactions with advanced filtering support.
// It delegates to GetAllTransactionsWithFilterAndOptions with nil options.
//
// Parameters:
// - ctx: Context for the database operation.
// - filters: A QueryFilterSet containing the filter conditions.
// - limit: The maximum number of transactions to return.
// - offset: The offset to start fetching transactions from (for pagination).
//
// Returns:
// - []model.Transaction: A slice of transactions matching the filter criteria.
// - error: An error if the query fails or if there's an issue processing the results.
