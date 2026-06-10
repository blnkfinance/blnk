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
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetTransactionsPaginated(ctx context.Context, _ string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetInternalTransactionsPaginated")
	defer span.End()

	// Create a cache key based on the pagination parameters
	cacheKey := fmt.Sprintf("transactions:paginated:%d:%d", batchSize, offset)

	var transactions []*model.Transaction
	// Attempt to retrieve transactions from cache
	err := d.Cache.Get(ctx, cacheKey, &transactions)
	if err == nil && len(transactions) > 0 {
		span.AddEvent("Transactions retrieved from cache", trace.WithAttributes(
			attribute.Int("transaction.count", len(transactions)),
		))
		return transactions, nil
	}

	// If not found in cache, fetch from the database
	rows, err := d.Conn.QueryContext(ctx, `
        SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        ORDER BY created_at ASC
        LIMIT $1 OFFSET $2
    `, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve paginated transactions", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	transactions = []*model.Transaction{}

	// Scan the rows into transactions
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

	// Handle any errors that occurred while iterating over the rows
	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	// Cache the fetched transactions for future use
	if len(transactions) > 0 {
		err = d.Cache.Set(ctx, cacheKey, transactions, 1*time.Hour) // Cache for 1 hour
		if err != nil {
			// Log the error but don't return it since the main operation succeeded
			logrus.Errorf("Failed to cache transactions: %v", err)
		}
	}

	span.AddEvent("Paginated transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

// GroupTransactions retrieves and groups transactions from the database based on a specified column (groupCriteria).
// It supports pagination and caches the grouped results for efficiency. If the data is found in the cache, it returns the cached data.
// Parameters:
// - ctx: Context for managing request and tracing.
// - groupCriteria: Column to group transactions by (e.g., "currency", "status").
// - batchSize: Number of transactions to retrieve in one batch.
// - offset: Number of transactions to skip before retrieving the batch.
// Returns:
// - A map of grouped transactions, or an error if retrieval or grouping fails.
func (d Datasource) GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GroupTransactions")
	defer span.End()

	query, ok := groupedTransactionsQuery(groupCriteria)
	if !ok {
		span.RecordError(fmt.Errorf("invalid group criteria: %s", groupCriteria))
		return nil, apierror.NewAPIError(apierror.ErrBadRequest, fmt.Sprintf("Invalid group criteria: %s", groupCriteria), nil)
	}

	// Create a cache key based on the grouping and pagination parameters
	cacheKey := fmt.Sprintf("transactions:grouped:%s:%d:%d", groupCriteria, batchSize, offset)

	var groupedTransactions map[string][]*model.Transaction
	err := d.Cache.Get(ctx, cacheKey, &groupedTransactions)
	if err == nil && len(groupedTransactions) > 0 {
		span.AddEvent("Grouped transactions retrieved from cache", trace.WithAttributes(
			attribute.Int("group.count", len(groupedTransactions)),
		))
		return groupedTransactions, nil
	}

	// If not in cache or error occurred, fetch from database
	rows, err := d.Conn.QueryContext(ctx, query, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve grouped transactions", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	groupedTransactions = make(map[string][]*model.Transaction)

	// Group transactions by the selected groupCriteria
	for rows.Next() {
		var groupKey string
		transaction := &model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string
		err = rows.Scan(
			&groupKey,
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

		if err := json.Unmarshal(metaDataJSON, &transaction.MetaData); err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transaction.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}

		groupedTransactions[groupKey] = append(groupedTransactions[groupKey], transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	// Cache the fetched data if not empty
	if len(groupedTransactions) > 0 {
		if err := d.Cache.Set(ctx, cacheKey, groupedTransactions, 5*time.Minute); err != nil {
			logrus.Errorf("Failed to cache grouped transactions: %v", err)
		}
	}

	span.AddEvent("Grouped transactions retrieved", trace.WithAttributes(
		attribute.Int("group.count", len(groupedTransactions)),
	))
	return groupedTransactions, nil
}

func groupedTransactionsQuery(groupCriteria string) (string, bool) {
	switch groupCriteria {
	case "transaction_id":
		return `
        SELECT transaction_id::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE transaction_id::text IS NOT NULL AND transaction_id::text != ''
        ORDER BY transaction_id::text
        LIMIT $1 OFFSET $2
    `, true
	case "parent_transaction":
		return `
        SELECT parent_transaction::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE parent_transaction::text IS NOT NULL AND parent_transaction::text != ''
        ORDER BY parent_transaction::text
        LIMIT $1 OFFSET $2
    `, true
	case "source":
		return `
        SELECT source::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE source::text IS NOT NULL AND source::text != ''
        ORDER BY source::text
        LIMIT $1 OFFSET $2
    `, true
	case "reference":
		return `
        SELECT reference::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE reference::text IS NOT NULL AND reference::text != ''
        ORDER BY reference::text
        LIMIT $1 OFFSET $2
    `, true
	case "currency":
		return `
        SELECT currency::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE currency::text IS NOT NULL AND currency::text != ''
        ORDER BY currency::text
        LIMIT $1 OFFSET $2
    `, true
	case "destination":
		return `
        SELECT destination::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE destination::text IS NOT NULL AND destination::text != ''
        ORDER BY destination::text
        LIMIT $1 OFFSET $2
    `, true
	case "status":
		return `
        SELECT status::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE status::text IS NOT NULL AND status::text != ''
        ORDER BY status::text
        LIMIT $1 OFFSET $2
    `, true
	case "created_at":
		return `
        SELECT created_at::text AS group_key, transaction_id, parent_transaction, source, reference,
               amount, precise_amount, precision, currency, destination,
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE created_at::text IS NOT NULL AND created_at::text != ''
        ORDER BY created_at::text
        LIMIT $1 OFFSET $2
    `, true
	default:
		return "", false
	}
}

// GetInflightTransactionsByParentID retrieves all inflight transactions associated with a given parent transaction ID.
// It supports pagination via batchSize and offset. Transactions with status 'INFLIGHT' are fetched.
// If no INFLIGHT transactions exist, then transactions with status 'QUEUED' and meta_data.inflight=true are considered.
// Parameters:
// - ctx: Context for managing request and tracing.
// - parentTransactionID: The ID of the parent transaction to filter by.
// - batchSize: Number of transactions to retrieve in one batch.
// - offset: Number of transactions to skip before retrieving the batch.
// Returns:
// - A slice of inflight transactions or an error if retrieval fails.
func (d Datasource) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetInflightTransactionsByParentID")
	defer span.End()

	// This query first checks if there are any INFLIGHT transactions for this parentTransactionID
	// If there are, it returns only those. If not, it falls back to QUEUED with inflight=true
	// It excludes any REJECTED transactions
	rows, err := d.Conn.QueryContext(ctx, `
		WITH inflight_transactions AS (
			SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
				   currency, destination, description, status, created_at, meta_data, scheduled_for, hash
			FROM blnk.transactions
			WHERE (transaction_id = $1 OR parent_transaction = $1 OR meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)
			AND status = 'INFLIGHT'
		), 
		queued_inflight_transactions AS (
			SELECT t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, t.precision, 
				   t.currency, t.destination, t.description, t.status, t.created_at, t.meta_data, t.scheduled_for, t.hash
			FROM blnk.transactions t
			WHERE (t.transaction_id = $1 OR t.parent_transaction = $1) 
			AND t.status = 'QUEUED' AND t.meta_data->>'inflight' = 'true'
			-- Don't include transactions that have been rejected (check by reference with _q suffix)
			AND NOT EXISTS (
				SELECT 1 
				FROM blnk.transactions rejected
				WHERE rejected.reference = t.reference || '_q' AND rejected.status = 'REJECTED'
			)
			-- Also don't include if there are child transactions with INFLIGHT status
			AND NOT EXISTS (
				SELECT 1 
				FROM blnk.transactions child
				WHERE child.parent_transaction = t.transaction_id AND child.status = 'INFLIGHT'
			)
		)
		
		SELECT * FROM inflight_transactions
		UNION ALL
		-- Only include queued_inflight if there are no inflight transactions
		SELECT * FROM queued_inflight_transactions 
		WHERE NOT EXISTS (SELECT 1 FROM inflight_transactions)
		
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve inflight transactions", err)
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

	// Handle any errors that occurred while iterating over the rows
	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	span.AddEvent("Inflight transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

// GetRefundableTransactionsByParentID retrieves transactions associated with a given parent transaction ID that are eligible for refunds.
// Refundable transactions are those with status 'APPLIED' or 'VOID'. It supports pagination with batchSize and offset.
// Parameters:
// - ctx: Context for managing request and tracing.
// - parentTransactionID: The ID of the parent transaction to filter by.
// - batchSize: Number of transactions to retrieve in one batch.
// - offset: Number of transactions to skip before retrieving the batch.
// Returns:
// - A slice of refundable transactions or an error if retrieval fails.
