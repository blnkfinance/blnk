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

// RecordTransaction records a new transaction in the database.
// It logs the transaction details using OpenTelemetry tracing.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - txn: The transaction object containing details to be recorded.
// Returns:
// - The recorded transaction if successful, or an error if the recording fails.
func (d Datasource) RecordTransaction(ctx context.Context, txn *model.Transaction) (*model.Transaction, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransaction")
	defer span.End()

	// Marshal transaction metadata into JSON format
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		span.RecordError(err) // Record the error in the tracing span
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Execute the SQL insert statement to record the transaction
	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.Amount, txn.PreciseAmount, txn.Precision, txn.Rate, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt, metaDataJSON, txn.ScheduledFor, txn.Hash,
	)

	// Handle errors that may occur during the execution of the query
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	// Log the successful transaction recording as an event in the tracing span
	span.AddEvent("Transaction recorded", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))

	return txn, nil
}

// GetTransaction retrieves a transaction by its ID from the database.
// It logs the transaction retrieval using OpenTelemetry tracing.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The unique transaction ID.
// Returns:
// - The retrieved transaction if successful, or an error if retrieval fails.
func (d Datasource) GetTransaction(ctx context.Context, id string) (*model.Transaction, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransaction")
	defer span.End()

	// Execute the SQL query to retrieve the transaction by its ID
	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data
		FROM blnk.transactions
		WHERE transaction_id = $1
	`, id)

	// Initialize a Transaction model and scan the result into it
	txn := &model.Transaction{}
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Precision, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON)

	// Handle errors, including no rows found
	if err != nil {
		if err == sql.ErrNoRows {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with ID '%s' not found", id), err)
		}
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transaction", err)
	}

	// Unmarshal the metadata JSON into the transaction's MetaData field
	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	// Log the successful transaction retrieval as an event in the tracing span
	span.AddEvent("Transaction retrieved", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))

	return txn, nil
}

// IsParentTransactionVoid checks if a parent transaction has a status of 'VOID'.
// It uses OpenTelemetry to trace the operation and returns a boolean indicating
// whether any child transaction linked to the parent has a 'VOID' status.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - parentID: The unique ID of the parent transaction.
// Returns:
// - A boolean indicating whether the parent transaction is void, or an error if the check fails.
func (d Datasource) IsParentTransactionVoid(ctx context.Context, parentID string) (bool, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "IsParentTransactionVoid")
	defer span.End()

	// Variable to store whether the parent transaction is void
	var exists bool

	// Execute the SQL query to check if any child transaction has a 'VOID' status
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`, parentID).Scan(&exists)

	// Handle errors from the query
	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if parent transaction is void", err)
	}

	// Log the void check result in the tracing span
	span.AddEvent("Parent transaction void check", trace.WithAttributes(
		attribute.String("parent_transaction.id", parentID),
		attribute.Bool("parent_transaction.void", exists),
	))

	return exists, nil
}

// TransactionExistsByRef checks if a transaction with a given reference exists in the database.
// It uses OpenTelemetry to trace the operation and returns a boolean indicating whether the transaction exists.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - reference: The reference of the transaction to check for existence.
// Returns:
// - A boolean indicating whether the transaction exists, or an error if the check fails.
func (d Datasource) TransactionExistsByRef(ctx context.Context, reference string) (bool, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "TransactionExistsByRef")
	defer span.End()

	// Variable to store whether the transaction exists
	var exists bool

	// Execute the SQL query to check if the transaction exists by reference
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
	`, reference).Scan(&exists)

	// Handle errors from the query
	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if transaction exists", err)
	}

	// Log the transaction existence result in the tracing span
	span.AddEvent("Transaction existence check", trace.WithAttributes(
		attribute.String("transaction.reference", reference),
		attribute.Bool("transaction.exists", exists),
	))

	return exists, nil
}

// GetTransactionByRef retrieves a transaction from the database using the provided reference.
// It traces the operation using OpenTelemetry and returns the transaction or an error.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - reference: The reference of the transaction to retrieve.
// Returns:
// - A model.Transaction representing the transaction or an error if the retrieval fails.
func (d Datasource) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransactionByRef")
	defer span.End()

	// Query the transaction by reference
	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data
		FROM blnk.transactions
		WHERE reference = $1
	`, reference)

	// Initialize the transaction object and scan the query result into it
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

	// Unmarshal the metadata JSON into the transaction's MetaData field
	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return model.Transaction{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	// Log the successful transaction retrieval in the tracing span
	span.AddEvent("Transaction retrieved by reference", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))

	return txn, nil
}

// UpdateTransactionStatus updates the status of a transaction in the database.
// It traces the operation using OpenTelemetry and returns an error if the update fails or if the transaction is not found.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The ID of the transaction to update.
// - status: The new status to set for the transaction.
// Returns:
// - An error if the update fails or if the transaction is not found.
func (d Datasource) UpdateTransactionStatus(ctx context.Context, id string, status string) error {
	// Start a new tracing span for the update operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "UpdateTransactionStatus")
	defer span.End()

	// Execute the update query
	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`, id, status)

	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update transaction status", err)
	}

	// Check how many rows were affected by the update
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were affected, return a not found error
	if rowsAffected == 0 {
		span.AddEvent("Transaction not found for status update", trace.WithAttributes(
			attribute.String("transaction.id", id),
			attribute.String("transaction.status", status),
		))
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with ID '%s' not found", id), nil)
	}

	// Log the successful status update in the tracing span
	span.AddEvent("Transaction status updated", trace.WithAttributes(
		attribute.String("transaction.id", id),
		attribute.String("transaction.status", status),
	))
	return nil
}

// GetAllTransactions retrieves all transactions from the database, ordered by creation date in descending order.
// It traces the operation using OpenTelemetry and returns an error if the retrieval or processing fails.
// Parameters:
// - ctx: Context for managing the request and tracing.
// Returns:
// - A slice of transactions or an error if the retrieval fails.
func (d Datasource) GetAllTransactions(ctx context.Context, limit, offset int) ([]model.Transaction, error) {
	// Start a new tracing span for the operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetAllTransactions")
	defer span.End()

	// Execute the query to retrieve all transactions
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, source, reference, amount, currency, destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transactions", err)
	}
	defer rows.Close()

	// Initialize a slice to store the transactions
	var transactions []model.Transaction

	// Iterate through the result set
	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte

		// Scan each row into the Transaction struct
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

		// Unmarshal metadata into the transaction's MetaData field
		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		// Append the transaction to the slice
		transactions = append(transactions, transaction)
	}

	// Check for errors after the iteration
	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	// Log the successful retrieval of transactions
	span.AddEvent("All transactions retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))

	// Return the slice of transactions
	return transactions, nil
}

// GetTotalCommittedTransactions calculates the total committed transaction amounts for a given parent transaction.
// It uses OpenTelemetry for tracing and returns the total or an error if the retrieval fails.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - parentID: The ID of the parent transaction to retrieve totals for.
// Returns:
// - The total committed amount as int64, or 0 if no transactions are found, along with an error if the retrieval fails.
func (d Datasource) GetTotalCommittedTransactions(ctx context.Context, parentID string) (int64, error) {
	// Start a new tracing span for the operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTotalCommittedTransactions")
	defer span.End()

	// SQL query to calculate the total precise amount for the given parent transaction
	query := `
		SELECT SUM(precise_amount) AS total_amount
		FROM blnk.transactions
		WHERE parent_transaction = $1
		GROUP BY parent_transaction;
	`

	// Initialize the variable to store the total amount
	var totalAmount int64

	// Execute the query and scan the result into totalAmount
	err := d.Conn.QueryRowContext(ctx, query, parentID).Scan(&totalAmount)
	if err != nil {
		// If no rows are found, return 0 without error
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		// Record the error in the tracing span and return the error
		span.RecordError(err)
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get total committed transactions", err)
	}

	// Log the successful retrieval of the total amount
	span.AddEvent("Total committed transactions retrieved", trace.WithAttributes(
		attribute.String("parent_transaction.id", parentID),
		attribute.Int64("total_amount", totalAmount),
	))

	// Return the total amount
	return totalAmount, nil
}

// GetTransactionsPaginated retrieves a batch of transactions from the database with pagination support and caches the result.
// If the data is found in cache, it is returned from there; otherwise, it is fetched from the database and then cached.
// Parameters:
// - ctx: Context for managing request and tracing.
// - batchSize: Number of transactions to retrieve in one batch.
// - offset: Number of transactions to skip before retrieving the batch.
// Returns:
// - A slice of transactions, or an error if the retrieval or caching fails.
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

	// Scan the rows into transactions
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
			log.Printf("Failed to cache transactions: %v", err)
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

	// Group transactions by the selected groupCriteria
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

// GetInflightTransactionsByParentID retrieves all inflight transactions associated with a given parent transaction ID.
// It supports pagination via batchSize and offset. Only transactions with status 'INFLIGHT' are fetched.
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

	// Iterate over the result set and map to transaction models
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

	// Handle errors that may occur while iterating over the rows
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

	// Iterate over the result set and map to transaction models
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
