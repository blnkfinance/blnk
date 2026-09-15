package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// UpdateLedgerMetadata updates the metadata for a specific ledger in the database.
// It marshals the metadata map to JSON before storing it.
//
// Parameters:
// - id: The ID of the ledger to update.
// - metadata: The new metadata to store.
//
// Returns:
// - error: An error if the update operation fails.
func (d *Datasource) UpdateLedgerMetadata(id string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = d.Conn.ExecContext(context.Background(), `
		UPDATE blnk.ledgers 
		SET meta_data = $1
		WHERE ledger_id = $2
	`, metadataJSON, id)
	return err
}

// UpdateTransactionMetadata updates the metadata for a specific transaction in the database.
// It merges the provided metadata with existing metadata for each matching transaction.
// The update applies to both the transaction with the provided ID and any transactions
// where this ID is set as the parent_transaction.
//
// Parameters:
// - ctx: The context for the database operation.
// - id: The ID of the transaction to update.
// - metadata: The new metadata to merge with existing metadata.
//
// Returns:
// - error: An error if the update operation fails.
func (d *Datasource) UpdateTransactionMetadata(ctx context.Context, id string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	// Merge into the existing metadata rather than replacing it. The left operand
	// is coerced to an object first: jsonb concatenation of a non-object (a JSON
	// null from a transaction stored without metadata, or a scalar) with an object
	// yields an array, which corrupts the column.
	_, err = d.Conn.ExecContext(ctx, `
		UPDATE blnk.transactions
		SET meta_data = (CASE WHEN jsonb_typeof(meta_data) = 'object' THEN meta_data ELSE '{}'::jsonb END) || $1::jsonb
		WHERE transaction_id = $2 OR parent_transaction = $2
	`, metadataJSON, id)
	return err
}

// ListTransactionsByMetadataScope returns every transaction row that
// UpdateTransactionMetadata would touch for scopeID: the row whose
// transaction_id matches, and any row whose parent_transaction matches.
// Used after a metadata write to reindex Typesense without calling
// GetTransaction(scopeID), which misses bulk parent IDs.
func (d *Datasource) ListTransactionsByMetadataScope(ctx context.Context, scopeID string, limit int, offset int64) ([]*model.Transaction, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
			   currency, destination, description, status, created_at, effective_date, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 OR parent_transaction = $1
		ORDER BY transaction_id
		LIMIT $2 OFFSET $3
	`, scopeID, limit, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to list transactions for metadata scope", err)
	}
	defer func() { _ = rows.Close() }()

	var transactions []*model.Transaction
	for rows.Next() {
		transaction := &model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string
		// Root txn_ rows store parent_transaction as NULL (and often scheduled_for
		// too). Scanning those into string/time.Time fails the whole reindex.
		var parentTransaction sql.NullString
		var effectiveDate sql.NullTime
		var scheduledFor sql.NullTime
		if err := rows.Scan(
			&transaction.TransactionID,
			&parentTransaction,
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
			&effectiveDate,
			&metaDataJSON,
			&scheduledFor,
			&transaction.Hash,
		); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}
		transaction.ParentTransaction = nullableString(parentTransaction)
		if effectiveDate.Valid {
			transaction.EffectiveDate = &effectiveDate.Time
		}
		if scheduledFor.Valid {
			transaction.ScheduledFor = scheduledFor.Time
		}
		// SQL NULL meta_data is a nil slice; skip rather than failing the page.
		// After UpdateTransactionMetadata the column is an object, so this is
		// only a guard for legacy/empty rows in the same scope.
		if len(metaDataJSON) > 0 {
			if err := json.Unmarshal(metaDataJSON, &transaction.MetaData); err != nil {
				return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
			}
		}
		transaction.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}
		transactions = append(transactions, transaction)
	}
	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}
	return transactions, nil
}

// UpdateBalanceMetadata updates the metadata for a specific balance in the database.
// It marshals the metadata map to JSON before storing it.
//
// Parameters:
// - ctx: The context for the database operation.
// - id: The ID of the balance to update.
// - metadata: The new metadata to store.
//
// Returns:
// - error: An error if the update operation fails.
func (d *Datasource) UpdateBalanceMetadata(ctx context.Context, id string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = d.Conn.ExecContext(ctx, `
		UPDATE blnk.balances 
		SET meta_data = $1
		WHERE balance_id = $2
	`, metadataJSON, id)
	return err
}

// UpdateIdentityMetadata updates the metadata for a specific identity in the database.
// It marshals the metadata map to JSON before storing it.
//
// Parameters:
// - id: The ID of the identity to update.
// - metadata: The new metadata to store.
//
// Returns:
// - error: An error if the update operation fails.
func (d *Datasource) UpdateIdentityMetadata(id string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = d.Conn.ExecContext(context.Background(), `
		UPDATE blnk.identity 
		SET meta_data = $1
		WHERE identity_id = $2
	`, metadataJSON, id)
	return err
}
