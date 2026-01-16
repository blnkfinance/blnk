package database

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestUpdateLedgerMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	metadata := map[string]interface{}{
		"key":    "value",
		"number": 123,
	}

	metadataJSON, _ := json.Marshal(metadata)
	mock.ExpectExec("UPDATE blnk.ledgers").
		WithArgs(metadataJSON, "ldg_123").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateLedgerMetadata("ldg_123", metadata)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateTransactionMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	metadata := map[string]interface{}{
		"status": "completed",
		"ref":    "TX123",
	}

	metadataJSON, _ := json.Marshal(metadata)

	// Verify the SQL uses jsonb merge operator and updates both direct and parent matches
	mock.ExpectExec(`UPDATE blnk\.transactions SET meta_data = meta_data \|\| \$1::jsonb WHERE transaction_id = \$2 OR parent_transaction = \$2`).
		WithArgs(metadataJSON, "txn_123").
		WillReturnResult(sqlmock.NewResult(1, 2)) // 2 rows affected (1 direct + 1 parent match)

	err = ds.UpdateTransactionMetadata(ctx, "txn_123", metadata)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateBalanceMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	metadata := map[string]interface{}{
		"type": "savings",
		"tier": "premium",
	}

	metadataJSON, _ := json.Marshal(metadata)
	mock.ExpectExec("UPDATE blnk.balances").
		WithArgs(metadataJSON, "bal_123").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateBalanceMetadata(ctx, "bal_123", metadata)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateIdentityMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	metadata := map[string]interface{}{
		"verified": true,
		"level":    2,
	}

	metadataJSON, _ := json.Marshal(metadata)
	mock.ExpectExec("UPDATE blnk.identity").
		WithArgs(metadataJSON, "idt_123").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateIdentityMetadata("idt_123", metadata)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateMetadata_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	metadata := map[string]interface{}{
		"invalid": make(chan int), // This will cause json.Marshal to fail
	}

	// Test for marshal error
	err = ds.UpdateLedgerMetadata("ldg_123", metadata)
	assert.Error(t, err)

	// Test for database error
	validMetadata := map[string]interface{}{"key": "value"}
	metadataJSON, _ := json.Marshal(validMetadata)
	mock.ExpectExec("UPDATE blnk.ledgers").
		WithArgs(metadataJSON, "ldg_123").
		WillReturnError(sqlmock.ErrCancelled)

	err = ds.UpdateLedgerMetadata("ldg_123", validMetadata)
	assert.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
