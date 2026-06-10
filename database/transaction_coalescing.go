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
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetQueuedTransactionsForCoalescing(ctx context.Context, source, destination, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetQueuedTransactionsForCoalescing")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions t
		WHERE t.status = 'QUEUED'
		  AND t.source = $1
		  AND t.destination = $2
		  AND t.currency = $3
		  AND t.transaction_id <> $4
		  AND t.created_at >= $5
		  AND NOT EXISTS (
		      SELECT 1 FROM blnk.transactions child
		      WHERE child.parent_transaction = t.transaction_id
		  )
		ORDER BY t.created_at ASC
		LIMIT $6
	`, source, destination, currency, excludeTransactionID, createdAtOrAfter.UTC(), limit)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve queued transactions for coalescing", err)
	}
	transactions, err := scanQueuedTransactionsForCoalescing(rows)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Queued transactions for coalescing retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
		attribute.String("source.balance_id", source),
		attribute.String("destination.balance_id", destination),
	))

	return transactions, nil
}

func (d Datasource) GetQueuedTransactionsForSourceCoalescing(ctx context.Context, source, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetQueuedTransactionsForSourceCoalescing")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions t
		WHERE t.status = 'QUEUED'
		  AND t.source = $1
		  AND t.currency = $2
		  AND t.transaction_id <> $3
		  AND t.created_at >= $4
		  AND NOT EXISTS (
		      SELECT 1 FROM blnk.transactions child
		      WHERE child.parent_transaction = t.transaction_id
		  )
		ORDER BY t.created_at ASC
		LIMIT $5
	`, source, currency, excludeTransactionID, createdAtOrAfter.UTC(), limit)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve source-scoped queued transactions for coalescing", err)
	}

	transactions, err := scanQueuedTransactionsForCoalescing(rows)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Source-scoped queued transactions for coalescing retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
		attribute.String("source.balance_id", source),
	))

	return transactions, nil
}

func (d Datasource) GetQueuedTransactionsForDestinationCoalescing(ctx context.Context, destination, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetQueuedTransactionsForDestinationCoalescing")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions t
		WHERE t.status = 'QUEUED'
		  AND t.destination = $1
		  AND t.currency = $2
		  AND t.transaction_id <> $3
		  AND t.created_at >= $4
		  AND NOT EXISTS (
		      SELECT 1 FROM blnk.transactions child
		      WHERE child.parent_transaction = t.transaction_id
		  )
		ORDER BY t.created_at ASC
		LIMIT $5
	`, destination, currency, excludeTransactionID, createdAtOrAfter.UTC(), limit)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve destination-scoped queued transactions for coalescing", err)
	}

	transactions, err := scanQueuedTransactionsForCoalescing(rows)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Destination-scoped queued transactions for coalescing retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
		attribute.String("destination.balance_id", destination),
	))

	return transactions, nil
}

func scanQueuedTransactionsForCoalescing(rows *sql.Rows) ([]*model.Transaction, error) {
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	var transactions []*model.Transaction
	for rows.Next() {
		txn := &model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string
		if err := rows.Scan(
			&txn.TransactionID, &txn.ParentTransaction, &txn.Source, &txn.Reference,
			&txn.Amount, &preciseAmountStr, &txn.Precision,
			&txn.Currency, &txn.Destination, &txn.Description, &txn.Status,
			&txn.CreatedAt, &metaDataJSON, &txn.ScheduledFor, &txn.Hash,
		); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan queued transaction for coalescing", err)
		}

		if err := json.Unmarshal(metaDataJSON, &txn.MetaData); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		preciseAmount, err := parseBigInt(preciseAmountStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}
		txn.PreciseAmount = preciseAmount

		model.ApplyPrecision(txn)
		transactions = append(transactions, txn)
	}

	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating queued transactions for coalescing", err)
	}

	return transactions, nil
}

func (d Datasource) CountQueuedTransactionsForPairLane(ctx context.Context, source, destination, currency, lane string) (int, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountQueuedTransactionsForPairLane")
	defer span.End()

	var count int
	err := d.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.transactions t
		WHERE t.status = 'QUEUED'
		  AND t.source = $1
		  AND t.destination = $2
		  AND t.currency = $3
		  AND COALESCE(t.meta_data->>'queue_lane', 'normal') = $4
		  AND NOT EXISTS (
		      SELECT 1 FROM blnk.transactions child
		      WHERE child.parent_transaction = t.transaction_id
		  )
	`, source, destination, currency, lane).Scan(&count)
	if err != nil {
		span.RecordError(err)
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to count queued transactions for pair lane", err)
	}

	return count, nil
}
