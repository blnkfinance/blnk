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
	"math/big"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetQueuedAmounts(ctx context.Context, balanceID string) (debit, credit *big.Int, err error) {
	// Aggregate in SQL with separate indexed scans for source (debit) and
	// destination (credit) instead of an un-indexable OR predicate plus
	// row-by-row summation in Go. A transaction where source = destination =
	// balanceID counts only as a debit (hence the t.source <> $1 guard),
	// preserving the previous if/else semantics.
	var debitStr, creditStr string
	err = d.Conn.QueryRowContext(ctx, `
        SELECT
            (SELECT COALESCE(SUM(t.precise_amount), 0)
             FROM blnk.transactions t
             WHERE t.source = $1
             AND t.status = 'QUEUED'
             AND NOT EXISTS (
                 SELECT 1
                 FROM blnk.transactions child
                 WHERE child.parent_transaction = t.transaction_id
                 AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
             ))::text AS queued_debit,
            (SELECT COALESCE(SUM(t.precise_amount), 0)
             FROM blnk.transactions t
             WHERE t.destination = $1
             AND t.source <> $1
             AND t.status = 'QUEUED'
             AND NOT EXISTS (
                 SELECT 1
                 FROM blnk.transactions child
                 WHERE child.parent_transaction = t.transaction_id
                 AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
             ))::text AS queued_credit`, balanceID).Scan(&debitStr, &creditStr)
	if err != nil {
		return nil, nil, err
	}

	debit, ok := new(big.Int).SetString(debitStr, 10)
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse queued debit amount: %s", debitStr)
	}
	credit, ok = new(big.Int).SetString(creditStr, 10)
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse queued credit amount: %s", creditStr)
	}

	return debit, credit, nil
}

// TransactionExistsByIDOrParentID checks if a transaction exists either by its direct ID
// or as a parent transaction ID for other transactions.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The ID to search for in both transaction_id and parent_transaction fields.
// Returns:
// - A boolean indicating whether the transaction exists in either capacity, and an error if the check fails.
func (d Datasource) TransactionExistsByIDOrParentID(ctx context.Context, id string) (bool, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "TransactionExistsByIDOrParentID")
	defer span.End()

	var exists bool
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM blnk.transactions 
			WHERE transaction_id = $1 OR parent_transaction = $1
		)
	`, id).Scan(&exists)
	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if transaction exists", err)
	}

	span.AddEvent("Transaction existence check", trace.WithAttributes(
		attribute.String("transaction.id", id),
		attribute.Bool("transaction.exists", exists),
	))

	return exists, nil
}

// GetTransactionsByParent retrieves all transactions associated with a given parent transaction ID.
// It supports pagination via limit and offset parameters.
// Parameters:
// - ctx: Context for managing request and tracing.
// - parentID: The ID of the parent transaction to filter by.
// - limit: Maximum number of transactions to retrieve.
// - offset: Number of transactions to skip before retrieving the batch.
// Returns:
// - A slice of transactions or an error if retrieval fails.
func (d Datasource) GetTransactionsByParent(ctx context.Context, parentID string, limit int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransactionsByParent")
	defer span.End()

	// Create a cache key based on the parameters
	cacheKey := fmt.Sprintf("transactions:parent:%s:%d:%d", parentID, limit, offset)

	var transactions []*model.Transaction
	// Attempt to retrieve from cache first
	err := d.Cache.Get(ctx, cacheKey, &transactions)
	if err == nil && len(transactions) > 0 {
		span.AddEvent("Transactions retrieved from cache", trace.WithAttributes(
			attribute.Int("transaction.count", len(transactions)),
		))
		return transactions, nil
	}

	// If not in cache, query the database
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, 
			   currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE parent_transaction = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentID, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transactions by parent", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	transactions = []*model.Transaction{}

	// Iterate over the result set
	for rows.Next() {
		transaction := &model.Transaction{}
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
		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	// Cache the results if there are any
	if len(transactions) > 0 {
		if err := d.Cache.Set(ctx, cacheKey, transactions, 5*time.Minute); err != nil {
			logrus.Errorf("Failed to cache transactions by parent: %v", err)
		}
	}

	span.AddEvent("Transactions by parent retrieved", trace.WithAttributes(
		attribute.String("parent_transaction.id", parentID),
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

// IsTransactionRefunded checks if a transaction has already been refunded by looking for
// a transaction that has the inverse source/destination and references the original
// transaction as its parent.
// Parameters:
// - ctx: Context for managing request and tracing.
// - transaction: The original transaction to check for refunds.
// Returns:
// - bool: true if the transaction has been refunded, false otherwise
// - error: an error if the check fails
func (d Datasource) IsTransactionRefunded(ctx context.Context, transaction *model.Transaction) (bool, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "IsTransactionRefunded")
	defer span.End()

	var exists bool
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 
			FROM blnk.transactions 
			WHERE parent_transaction = $1 
			AND source = $2 
			AND destination = $3
		)
	`, transaction.TransactionID, transaction.Destination, transaction.Source).Scan(&exists)
	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check refund status", err)
	}

	span.AddEvent("Refund status checked", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
		attribute.Bool("is_refunded", exists),
	))

	return exists, nil
}

// GetTransactionsByCriteria retrieves transactions based on specified criteria (amount range, currency, date range).
// It supports pagination via limit and offset.
