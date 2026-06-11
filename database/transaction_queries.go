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

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetTransaction(ctx context.Context, id string) (*model.Transaction, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransaction")
	defer span.End()

	// Execute the SQL query to retrieve the transaction by its ID
	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, parent_transaction, hash
		FROM blnk.transactions
		WHERE transaction_id = $1
	`, id)

	// Initialize a Transaction model and scan the result into it
	txn := &model.Transaction{}
	var metaDataJSON []byte
	var preciseAmountStr string
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &preciseAmountStr, &txn.Precision, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON, &txn.ParentTransaction, &txn.Hash)
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

	txn.PreciseAmount, err = parseBigInt(preciseAmountStr)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
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

// GetExistingTransactionReferences retrieves the subset of the provided references that already
// exist in the database. It returns an empty set when the input is empty.
func (d Datasource) GetExistingTransactionReferences(ctx context.Context, references []string) (map[string]struct{}, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetExistingTransactionReferences")
	defer span.End()

	existing := make(map[string]struct{})
	if len(references) == 0 {
		return existing, nil
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT reference
		FROM blnk.transactions
		WHERE reference = ANY($1)
	`, pq.Array(references))
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve existing transaction references", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	for rows.Next() {
		var reference string
		if err := rows.Scan(&reference); err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan existing transaction reference", err)
		}
		existing[reference] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating existing transaction references", err)
	}

	span.AddEvent("Existing transaction references retrieved", trace.WithAttributes(
		attribute.Int("reference.requested_count", len(references)),
		attribute.Int("reference.existing_count", len(existing)),
	))

	return existing, nil
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
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data, parent_transaction
		FROM blnk.transactions
		WHERE reference = $1
	`, reference)

	// Initialize the transaction object and scan the query result into it
	txn := model.Transaction{}
	var metaDataJSON []byte
	var preciseAmountStr string
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &preciseAmountStr, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON, &txn.ParentTransaction)
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

	txn.PreciseAmount, err = parseBigInt(preciseAmountStr)
	if err != nil {
		span.RecordError(err)
		return model.Transaction{}, fmt.Errorf("failed to parse precise_amount: %w", err)
	}

	// Log the successful transaction retrieval in the tracing span
	span.AddEvent("Transaction retrieved by reference", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))

	return txn, nil
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
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

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
// - The total committed amount as *big.Int, or 0 if no transactions are found, along with an error if the retrieval fails.
