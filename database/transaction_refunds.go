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
	"fmt"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetRefundableTransactionsByParentID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT 
			t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, 
			t.precision, t.currency, t.destination, t.description, t.status, t.created_at, 
			t.meta_data, t.scheduled_for, t.hash
		FROM 
			blnk.transactions t
		WHERE 
			-- Case 1: The transaction is the parent itself and is APPLIED
			(t.transaction_id = $1 AND t.status = 'APPLIED')
			
			-- Case 2: The transaction is a child and is APPLIED or VOID
			OR (t.parent_transaction = $1 AND t.status IN ('APPLIED', 'VOID'))

			-- Case 3: Transaction is APPLIED and linked via metadata QUEUED_PARENT_TRANSACTION
			OR (t.status = 'APPLIED' AND t.meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)

		ORDER BY 
			t.created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve refundable transactions", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	var transactions []*model.Transaction

	// Iterate over the result set and map to transaction models
	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
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
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transaction.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	span.AddEvent("Refundable transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

// GetQueuedAmounts retrieves the total queued debit and credit amounts for a given balance ID.
// It only includes transactions with status 'QUEUED' that don't have child transactions with status 'APPLIED' or 'REJECTED'.
// This ensures that queued transactions that have been processed (either applied or rejected) are excluded from the totals.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - balanceID: The ID of the balance to retrieve queued amounts for.
// Returns:
// - The total debit and credit amounts as big.Int values, or an error if the retrieval fails.
