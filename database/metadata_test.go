package database

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	// Verify the SQL coerces the existing metadata to an object before the jsonb
	// merge and updates both direct and parent matches.
	mock.ExpectExec(`UPDATE blnk\.transactions\s+SET meta_data = \(CASE WHEN jsonb_typeof\(meta_data\) = 'object' THEN meta_data ELSE '\{\}'::jsonb END\) \|\| \$1::jsonb\s+WHERE transaction_id = \$2 OR parent_transaction = \$2`).
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

func TestListTransactionsByMetadataScope(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	now := time.Now().UTC()
	metaDataJSON, _ := json.Marshal(map[string]interface{}{"batch": "B-1"})

	rows := sqlmock.NewRows([]string{
		"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
		"precision", "currency", "destination", "description", "status", "created_at", "effective_date",
		"meta_data", "scheduled_for", "hash",
	}).AddRow(
		"txn_child_1", "bulk_meta_scope", "@world", "ref-1", 100.0, "100", 100, "USD",
		"@world", "child one", "APPLIED", now, nil, metaDataJSON, now, "hash-1",
	).AddRow(
		"txn_child_2", "bulk_meta_scope", "@world", "ref-2", 150.0, "150", 100, "USD",
		"@world", "child two", "APPLIED", now, nil, metaDataJSON, now, "hash-2",
	)

	mock.ExpectQuery(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
			   currency, destination, description, status, created_at, effective_date, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = \$1 OR parent_transaction = \$1
		ORDER BY transaction_id
		LIMIT \$2 OFFSET \$3`).
		WithArgs("bulk_meta_scope", 100, int64(0)).
		WillReturnRows(rows)

	txns, err := ds.ListTransactionsByMetadataScope(ctx, "bulk_meta_scope", 100, 0)
	assert.NoError(t, err)
	require.Len(t, txns, 2)
	assert.Equal(t, "txn_child_1", txns[0].TransactionID)
	assert.Equal(t, "bulk_meta_scope", txns[0].ParentTransaction)
	assert.Equal(t, "B-1", txns[0].MetaData["batch"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListTransactionsByMetadataScope_NullParentAndScheduledFor is the
// regression for POST /txn_…/metadata reindex: the updated row is usually a
// root transaction with parent_transaction IS NULL (and often scheduled_for
// IS NULL). Those must scan as "" / zero time, not fail the listing.
func TestListTransactionsByMetadataScope_NullParentAndScheduledFor(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	now := time.Now().UTC()
	metaDataJSON, _ := json.Marshal(map[string]interface{}{"invoice": "INV-1"})

	rows := sqlmock.NewRows([]string{
		"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
		"precision", "currency", "destination", "description", "status", "created_at", "effective_date",
		"meta_data", "scheduled_for", "hash",
	}).AddRow(
		"txn_root", nil, "@world", "ref-root", 100.0, "100", 100, "USD",
		"@world", "root txn", "APPLIED", now, nil, metaDataJSON, nil, "hash-root",
	)

	mock.ExpectQuery(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
			   currency, destination, description, status, created_at, effective_date, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = \$1 OR parent_transaction = \$1
		ORDER BY transaction_id
		LIMIT \$2 OFFSET \$3`).
		WithArgs("txn_root", 100, int64(0)).
		WillReturnRows(rows)

	txns, err := ds.ListTransactionsByMetadataScope(ctx, "txn_root", 100, 0)
	assert.NoError(t, err)
	require.Len(t, txns, 1)
	assert.Equal(t, "txn_root", txns[0].TransactionID)
	assert.Equal(t, "", txns[0].ParentTransaction)
	assert.True(t, txns[0].ScheduledFor.IsZero())
	assert.Equal(t, "INV-1", txns[0].MetaData["invoice"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListTransactionsByMetadataScope_EffectiveDate(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	createdAt := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	effectiveDate := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	metaDataJSON, _ := json.Marshal(map[string]interface{}{"inflight": true, "allow_overdraft": true})

	rows := sqlmock.NewRows([]string{
		"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
		"precision", "currency", "destination", "description", "status", "created_at", "effective_date",
		"meta_data", "scheduled_for", "hash",
	}).AddRow(
		"txn_inflight", nil, "@world", "ref-inflight", 50.0, "50", 100, "USD",
		"@world", "inflight hold", "INFLIGHT", createdAt, effectiveDate, metaDataJSON, nil, "hash-inflight",
	)

	mock.ExpectQuery(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
			   currency, destination, description, status, created_at, effective_date, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = \$1 OR parent_transaction = \$1
		ORDER BY transaction_id
		LIMIT \$2 OFFSET \$3`).
		WithArgs("txn_inflight", 100, int64(0)).
		WillReturnRows(rows)

	txns, err := ds.ListTransactionsByMetadataScope(ctx, "txn_inflight", 100, 0)
	assert.NoError(t, err)
	require.Len(t, txns, 1)
	require.NotNil(t, txns[0].EffectiveDate)
	assert.Equal(t, effectiveDate, txns[0].EffectiveDate.UTC())
	assert.Equal(t, true, txns[0].MetaData["inflight"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateTransactionMetadata_BulkScope_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "bulk-meta-" + marker})
	require.NoError(t, err)
	balance, err := ds.CreateBalance(model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	bulkID := model.GenerateUUIDWithSuffix("bulk")
	now := time.Now().UTC()

	for i, suffix := range []string{"a", "b"} {
		txnID := model.GenerateUUIDWithSuffix("txn")
		_, err := ds.RecordTransaction(ctx, &model.Transaction{
			TransactionID:     txnID,
			ParentTransaction: bulkID,
			Source:            balance.BalanceID,
			Reference:         "bulk-scope-" + marker + "-" + suffix,
			Amount:            10,
			AmountString:      "10",
			PreciseAmount:     big.NewInt(10),
			Precision:         100,
			Currency:          "USD",
			Destination:       balance.BalanceID,
			Description:       "bulk metadata scope child",
			Status:            "APPLIED",
			CreatedAt:         now,
			ScheduledFor:      now,
			Hash:              "hash-" + suffix,
			MetaData:          map[string]interface{}{"marker": marker, "idx": i},
		})
		require.NoError(t, err)
	}

	patch := map[string]interface{}{"settlement_batch": "B-42"}
	require.NoError(t, ds.UpdateTransactionMetadata(ctx, bulkID, patch))

	txns, err := ds.ListTransactionsByMetadataScope(ctx, bulkID, 100, 0)
	require.NoError(t, err)
	require.Len(t, txns, 2)
	for _, txn := range txns {
		assert.Equal(t, bulkID, txn.ParentTransaction)
		assert.Equal(t, marker, txn.MetaData["marker"])
		assert.Equal(t, "B-42", txn.MetaData["settlement_batch"])
	}
}

func TestListTransactionsByMetadataScope_RootTransactionNullParent_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "root-meta-" + marker})
	require.NoError(t, err)
	balance, err := ds.CreateBalance(model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	txnID := model.GenerateUUIDWithSuffix("txn")
	now := time.Now().UTC()
	metaJSON, err := json.Marshal(map[string]interface{}{"marker": marker})
	require.NoError(t, err)
	// Insert without parent_transaction / scheduled_for so both stay NULL —
	// RecordTransaction writes "" / zero-time, and a later UPDATE is rejected
	// by the immutability trigger.
	_, err = ds.Conn.ExecContext(ctx, `
		INSERT INTO blnk.transactions
			(transaction_id, source, destination, reference, amount, precise_amount, precision,
			 currency, status, description, hash, created_at, meta_data)
		VALUES ($1, $2, $3, $4, 10, 10, 100, 'USD', 'APPLIED', 'root metadata scope', $5, $6, $7::jsonb)
	`, txnID, balance.BalanceID, balance.BalanceID, "root-scope-"+marker, "hash-root-"+marker, now, metaJSON)
	require.NoError(t, err)

	patch := map[string]interface{}{"invoice": "INV-1"}
	require.NoError(t, ds.UpdateTransactionMetadata(ctx, txnID, patch))

	txns, err := ds.ListTransactionsByMetadataScope(ctx, txnID, 100, 0)
	require.NoError(t, err)
	require.Len(t, txns, 1)
	assert.Equal(t, txnID, txns[0].TransactionID)
	assert.Equal(t, "", txns[0].ParentTransaction)
	assert.True(t, txns[0].ScheduledFor.IsZero())
	assert.Equal(t, marker, txns[0].MetaData["marker"])
	assert.Equal(t, "INV-1", txns[0].MetaData["invoice"])
}
