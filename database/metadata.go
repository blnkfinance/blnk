package database

import (
	"context"
	"encoding/json"
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

	_, err = d.Conn.Exec(`
		UPDATE blnk.ledgers 
		SET meta_data = $1
		WHERE ledger_id = $2
	`, metadataJSON, id)
	return err
}

// UpdateTransactionMetadata updates the metadata for a specific transaction in the database.
// It marshals the metadata map to JSON before storing it.
//
// Parameters:
// - ctx: The context for the database operation.
// - id: The ID of the transaction to update.
// - metadata: The new metadata to store.
//
// Returns:
// - error: An error if the update operation fails.
func (d *Datasource) UpdateTransactionMetadata(ctx context.Context, id string, metadata map[string]interface{}) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = d.Conn.ExecContext(ctx, `
		UPDATE blnk.transactions 
		SET meta_data = $1
		WHERE transaction_id = $2
	`, metadataJSON, id)
	return err
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

	_, err = d.Conn.Exec(`
		UPDATE blnk.identity 
		SET meta_data = $1
		WHERE identity_id = $2
	`, metadataJSON, id)
	return err
}
