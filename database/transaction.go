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
	"errors"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func (d Datasource) RecordTransaction(ctx context.Context, txn *model.Transaction) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransaction")
	defer span.End()

	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.Amount, txn.PreciseAmount, txn.Precision, txn.Rate, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt, metaDataJSON, txn.ScheduledFor, txn.Hash,
	)

	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	span.AddEvent("Transaction recorded", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))
	return txn, nil
}

func (d Datasource) GetTransaction(ctx context.Context, id string) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransaction")
	defer span.End()

	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data
		FROM blnk.transactions
		WHERE transaction_id = $1
	`, id)

	txn := &model.Transaction{}
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Precision, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with ID '%s' not found", id), err)
		}
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transaction", err)
	}

	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	span.AddEvent("Transaction retrieved", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))
	return txn, nil
}

func (d Datasource) IsParentTransactionVoid(ctx context.Context, parentID string) (bool, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "IsParentTransactionVoid")
	defer span.End()

	var exists bool
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`, parentID).Scan(&exists)

	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if parent transaction is void", err)
	}

	span.AddEvent("Parent transaction void check", trace.WithAttributes(
		attribute.String("parent_transaction.id", parentID),
		attribute.Bool("parent_transaction.void", exists),
	))
	return exists, nil
}

func (d Datasource) TransactionExistsByRef(ctx context.Context, reference string) (bool, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "TransactionExistsByRef")
	defer span.End()

	var exists bool
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
	`, reference).Scan(&exists)

	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if transaction exists", err)
	}

	span.AddEvent("Transaction existence check", trace.WithAttributes(
		attribute.String("transaction.reference", reference),
		attribute.Bool("transaction.exists", exists),
	))
	return exists, nil
}

func (d Datasource) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransactionByRef")
	defer span.End()

	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data
		FROM blnk.transactions
		WHERE reference = $1
	`, reference)

	txn := model.Transaction{}
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			span.RecordError(err)
			return model.Transaction{}, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with reference '%s' not found", reference), err)
		}
		span.RecordError(err)
		return model.Transaction{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transaction", err)
	}

	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return model.Transaction{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	span.AddEvent("Transaction retrieved by reference", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))
	return txn, nil
}

func (d Datasource) UpdateTransactionStatus(ctx context.Context, id string, status string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "UpdateTransactionStatus")
	defer span.End()

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`, id, status)

	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update transaction status", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		span.AddEvent("Transaction not found for status update", trace.WithAttributes(
			attribute.String("transaction.id", id),
			attribute.String("transaction.status", status),
		))
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with ID '%s' not found", id), nil)
	}

	span.AddEvent("Transaction status updated", trace.WithAttributes(
		attribute.String("transaction.id", id),
		attribute.String("transaction.status", status),
	))
	return nil
}

func (d Datasource) GetAllTransactions(ctx context.Context) ([]model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetAllTransactions")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, source, reference, amount, currency, destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
	`)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transactions", err)
	}
	defer rows.Close()

	var transactions []model.Transaction

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.Hash,
			&transaction.CreatedAt,
			&metaDataJSON,
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

		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	span.AddEvent("All transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

func (d Datasource) GetTotalCommittedTransactions(ctx context.Context, parentID string) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTotalCommittedTransactions")
	defer span.End()

	query := `
		SELECT SUM(precise_amount) AS total_amount
		FROM blnk.transactions
		WHERE parent_transaction = $1
		GROUP BY parent_transaction;
	`

	var totalAmount int64
	err := d.Conn.QueryRowContext(ctx, query, parentID).Scan(&totalAmount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		span.RecordError(err)
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get total committed transactions", err)
	}

	span.AddEvent("Total committed transactions retrieved", trace.WithAttributes(
		attribute.String("parent_transaction.id", parentID),
		attribute.Int64("total_amount", totalAmount),
	))
	return totalAmount, nil
}

func (d Datasource) GetTransactionsPaginated(ctx context.Context, _ string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetInternalTransactionsPaginated")
	defer span.End()

	// Create a cache key based on the pagination parameters
	cacheKey := fmt.Sprintf("transactions:paginated:%d:%d", batchSize, offset)

	var transactions []*model.Transaction
	err := d.Cache.Get(ctx, cacheKey, &transactions)
	if err == nil && len(transactions) > 0 {
		span.AddEvent("Transactions retrieved from cache", trace.WithAttributes(
			attribute.Int("transaction.count", len(transactions)),
		))
		return transactions, nil
	}

	// If not in cache or error occurred, fetch from database
	rows, err := d.Conn.QueryContext(ctx, `
        SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        ORDER BY created_at ASC
        LIMIT $1 OFFSET $2
    `, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve paginated transactions", err)
	}
	defer rows.Close()

	transactions = []*model.Transaction{}

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
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

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	// Cache the fetched data
	if len(transactions) > 0 {
		err = d.Cache.Set(ctx, cacheKey, transactions, 1*time.Hour) // Cache for 1 hr
		if err != nil {
			// Log the error, but don't return it as the main operation succeeded
			log.Printf("Failed to cache transactions: %v", err)
		}
	}

	span.AddEvent("Paginated transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

func (d Datasource) GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GroupTransactions")
	defer span.End()

	validColumns := map[string]bool{
		"transaction_id": true, "parent_transaction": true, "source": true,
		"reference": true, "currency": true, "destination": true,
		"status": true, "created_at": true,
	}
	if !validColumns[groupCriteria] {
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
	query := `
        SELECT $1::text AS group_key, transaction_id, parent_transaction, source, reference, 
               amount, precise_amount, precision, rate, currency, destination, 
               description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE $1::text IS NOT NULL AND $1::text != ''
        ORDER BY $1::text
        LIMIT $2 OFFSET $3
    `

	rows, err := d.Conn.QueryContext(ctx, query, groupCriteria, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve grouped transactions", err)
	}
	defer rows.Close()

	groupedTransactions = make(map[string][]*model.Transaction)

	for rows.Next() {
		var groupKey string
		transaction := &model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&groupKey,
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
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
		groupedTransactions[groupKey] = append(groupedTransactions[groupKey], transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	// Cache the fetched data if not empty
	if len(groupedTransactions) > 0 {
		if err := d.Cache.Set(ctx, cacheKey, groupedTransactions, 5*time.Minute); err != nil {
			log.Printf("Failed to cache grouped transactions: %v", err)
		}
	}

	span.AddEvent("Grouped transactions retrieved", trace.WithAttributes(
		attribute.Int("group.count", len(groupedTransactions)),
	))
	return groupedTransactions, nil
}

func (d Datasource) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetInflightTransactionsByParentID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 OR parent_transaction = $1 AND status = 'INFLIGHT'
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve inflight transactions", err)
	}
	defer rows.Close()

	var transactions []*model.Transaction

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
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

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	span.AddEvent("Inflight transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}

func (d Datasource) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetRefundableTransactionsByParentID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 AND status = 'APPLIED' OR parent_transaction = $1 AND (status = 'VOID' OR status = 'APPLIED')
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve refundable transactions", err)
	}
	defer rows.Close()

	var transactions []*model.Transaction

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
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
