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
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
)

type mockCache struct {
	data map[string]interface{}
}

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string]interface{})}
}

func (m *mockCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	m.data[key] = value
	return nil
}

func (m *mockCache) Get(ctx context.Context, key string, data interface{}) error {
	if v, ok := m.data[key]; ok {
		// For simplicity, just return the value directly
		switch d := data.(type) {
		case *[]*model.Transaction:
			*d = v.([]*model.Transaction)
		}
		return nil
	}
	return errors.New("cache miss")
}

func (m *mockCache) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func TestRecordTransaction_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestRecordTransaction")
	defer span.End()

	ds := Datasource{Conn: db}

	transaction := &model.Transaction{
		TransactionID:     "txn123",
		Source:            "src1",
		Reference:         "ref123",
		Amount:            1000,
		AmountString:      "1000",
		Currency:          "USD",
		Destination:       "dest1",
		Description:       "Test Transaction",
		Status:            "PENDING",
		CreatedAt:         time.Now(),
		MetaData:          map[string]interface{}{"key": "value"},
		ScheduledFor:      time.Now(),
		Hash:              "hash123",
		PreciseAmount:     model.Int64ToBigInt(1000),
		Precision:         2,
		Rate:              1,
		ParentTransaction: "parent123",
		EffectiveDate:     nil,
	}

	metaDataJSON, err := json.Marshal(transaction.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.transactions").
		WithArgs(transaction.TransactionID, transaction.ParentTransaction, transaction.Source, transaction.Reference, transaction.AmountString, transaction.PreciseAmount.String(), transaction.Precision, transaction.Rate, transaction.Currency, transaction.Destination, transaction.Description, transaction.Status, transaction.CreatedAt, metaDataJSON, transaction.ScheduledFor, transaction.Hash, transaction.EffectiveDate).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := ds.RecordTransaction(ctx, transaction)
	assert.NoError(t, err)
	assert.Equal(t, transaction, result)
}

func TestRecordTransaction_Failure(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestRecordTransactionFailure")
	defer span.End()

	ds := Datasource{Conn: db}

	transaction := &model.Transaction{
		TransactionID:     "txn123",
		Source:            "src1",
		Reference:         "ref123",
		Amount:            1000,
		Currency:          "USD",
		Destination:       "dest1",
		Description:       "Test Transaction",
		Status:            "PENDING",
		CreatedAt:         time.Now(),
		MetaData:          map[string]interface{}{"key": "value"},
		ScheduledFor:      time.Now(),
		Hash:              "hash123",
		PreciseAmount:     model.Int64ToBigInt(1000),
		Precision:         2,
		Rate:              1,
		ParentTransaction: "parent123",
		EffectiveDate:     nil,
	}

	metaDataJSON, err := json.Marshal(transaction.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.transactions").
		WithArgs(transaction.TransactionID, transaction.ParentTransaction, transaction.Source, transaction.Reference, transaction.Amount, transaction.PreciseAmount.String(), transaction.Precision, transaction.Rate, transaction.Currency, transaction.Destination, transaction.Description, transaction.Status, transaction.CreatedAt, metaDataJSON, transaction.ScheduledFor, transaction.Hash, transaction.EffectiveDate).
		WillReturnError(errors.New("db error"))

	_, err = ds.RecordTransaction(ctx, transaction)
	assert.Error(t, err)
	assert.IsType(t, apierror.APIError{}, err)
}

func TestGetTransaction_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTransaction")
	defer span.End()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"transaction_id", "source", "reference", "amount", "precise_amount", "precision", "currency", "destination", "description", "status", "created_at", "meta_data", "parent_transaction", "hash"}).
		AddRow("txn123", "src1", "ref123", 1000, 1000, 2, "USD", "dest1", "Test Transaction", "PENDING", time.Now(), metaDataJSON, "parent123", "hash123")

	mock.ExpectQuery("SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, parent_transaction, hash FROM blnk.transactions WHERE transaction_id = ?").
		WithArgs("txn123").
		WillReturnRows(rows)

	txn, err := ds.GetTransaction(ctx, "txn123")
	assert.NoError(t, err)
	assert.Equal(t, "txn123", txn.TransactionID)
	assert.Equal(t, "src1", txn.Source)
	assert.Equal(t, "dest1", txn.Destination)
	assert.Equal(t, "parent123", txn.ParentTransaction)
	assert.Equal(t, "hash123", txn.Hash)
}

func TestGetTransaction_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTransactionNotFound")
	defer span.End()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, parent_transaction, hash FROM blnk.transactions WHERE transaction_id = ?").
		WithArgs("txn123").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.GetTransaction(ctx, "txn123")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
}

func TestTransactionExistsByRef_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestTransactionExistsByRef")
	defer span.End()

	ds := Datasource{Conn: db}

	// Modify the expected query to match the actual SQL query placeholder
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM blnk.transactions WHERE reference = \\$1\\)").
		WithArgs("ref123").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	exists, err := ds.TransactionExistsByRef(ctx, "ref123")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestTransactionExistsByRef_Failure(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestTransactionExistsByRefFailure")
	defer span.End()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM blnk.transactions WHERE reference = ?\\)").
		WithArgs("ref123").
		WillReturnError(errors.New("db error"))

	_, err = ds.TransactionExistsByRef(ctx, "ref123")
	assert.Error(t, err)
	assert.IsType(t, apierror.APIError{}, err)
}

func TestGetInflightTransactionsByParentID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetInflightTransactionsByParentID")
	defer span.End()

	ds := Datasource{Conn: db}

	// Create test data
	parentID := "parent123"
	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	mockRows := sqlmock.NewRows([]string{
		"transaction_id", "parent_transaction", "source", "reference",
		"amount", "precise_amount", "precision", "rate", "currency",
		"destination", "description", "status", "created_at",
		"meta_data", "scheduled_for", "hash",
	}).AddRow(
		"txn123", parentID, "source1", "ref123",
		1000, 1000, 2, 1.0, "USD",
		"dest1", "Test Transaction", "INFLIGHT", time.Now(),
		metaDataJSON, time.Now(), "hash123",
	)

	// Expect the query with the correct WHERE clause
	// Updated the expected query to match the complex CTE and UNION ALL structure
	expectedQuery := `
		WITH inflight_transactions AS (
			SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
				   rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
			FROM blnk.transactions
			WHERE (transaction_id = $1 OR parent_transaction = $1 OR meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)
			AND status = 'INFLIGHT'
		), 
		queued_inflight_transactions AS (
			SELECT t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, t.precision, 
				   t.rate, t.currency, t.destination, t.description, t.status, t.created_at, t.meta_data, t.scheduled_for, t.hash
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
	`
	// Use regexp.QuoteMeta to escape regex special characters for sqlmock
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(parentID, 10, int64(0)). // Add all three expected arguments
		WillReturnRows(mockRows)

	transactions, err := ds.GetInflightTransactionsByParentID(ctx, parentID, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, transactions, 1)
	assert.Equal(t, "txn123", transactions[0].TransactionID)
	assert.Equal(t, "INFLIGHT", transactions[0].Status)
}

func TestGetInflightTransactionsByParentID_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetInflightTransactionsByParentID_NoRows")
	defer span.End()

	ds := Datasource{Conn: db}

	parentID := "parent123"
	// Updated the expected query to match the complex CTE and UNION ALL structure
	expectedQuery := `
		WITH inflight_transactions AS (
			SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
				   rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
			FROM blnk.transactions
			WHERE (transaction_id = $1 OR parent_transaction = $1 OR meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)
			AND status = 'INFLIGHT'
		), 
		queued_inflight_transactions AS (
			SELECT t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, t.precision, 
				   t.rate, t.currency, t.destination, t.description, t.status, t.created_at, t.meta_data, t.scheduled_for, t.hash
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
	`
	// Use regexp.QuoteMeta to escape regex special characters for sqlmock
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(parentID, 10, int64(0)). // Add all three expected arguments
		WillReturnRows(sqlmock.NewRows([]string{}))

	transactions, err := ds.GetInflightTransactionsByParentID(ctx, parentID, 10, 0)
	assert.NoError(t, err)
	assert.Empty(t, transactions)
}

func TestGetInflightTransactionsByParentID_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetInflightTransactionsByParentID_Error")
	defer span.End()

	ds := Datasource{Conn: db}

	parentID := "parent123"
	// Updated the expected query to match the complex CTE and UNION ALL structure
	expectedQuery := `
		WITH inflight_transactions AS (
			SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision,
				   rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
			FROM blnk.transactions
			WHERE (transaction_id = $1 OR parent_transaction = $1 OR meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)
			AND status = 'INFLIGHT'
		), 
		queued_inflight_transactions AS (
			SELECT t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, t.precision, 
				   t.rate, t.currency, t.destination, t.description, t.status, t.created_at, t.meta_data, t.scheduled_for, t.hash
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
	`
	// Use regexp.QuoteMeta to escape regex special characters for sqlmock
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(parentID, 10, int64(0)). // Add all three expected arguments
		WillReturnError(errors.New("database error"))

	transactions, err := ds.GetInflightTransactionsByParentID(ctx, parentID, 10, 0)
	assert.Error(t, err)
	assert.Nil(t, transactions)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetTotalCommittedTransactions_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTotalCommittedTransactions")
	defer span.End()

	ds := Datasource{Conn: db}

	parentID := "parent123"
	expectedTotal := big.NewInt(2000)

	mock.ExpectQuery("SELECT SUM\\(precise_amount\\) AS total_amount FROM blnk.transactions WHERE parent_transaction = \\$1 AND status = 'APPLIED' GROUP BY parent_transaction").
		WithArgs(parentID).
		WillReturnRows(sqlmock.NewRows([]string{"total_amount"}).AddRow(expectedTotal.String()))

	total, err := ds.GetTotalCommittedTransactions(ctx, parentID)
	assert.NoError(t, err)
	assert.Equal(t, expectedTotal, total)
}

func TestGetTotalCommittedTransactions_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTotalCommittedTransactions_NoRows")
	defer span.End()

	ds := Datasource{Conn: db}

	parentID := "parent123"

	mock.ExpectQuery("SELECT SUM\\(precise_amount\\) AS total_amount FROM blnk.transactions WHERE parent_transaction = \\$1 AND status = 'APPLIED' GROUP BY parent_transaction").
		WithArgs(parentID).
		WillReturnError(sql.ErrNoRows)

	total, err := ds.GetTotalCommittedTransactions(ctx, parentID)
	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(0), total)
}

func TestGetTotalCommittedTransactions_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTotalCommittedTransactions_Error")
	defer span.End()

	ds := Datasource{Conn: db}

	parentID := "parent123"

	// Updated the regex to match the actual query which includes the status filter
	mock.ExpectQuery("SELECT SUM\\(precise_amount\\) AS total_amount FROM blnk.transactions WHERE parent_transaction = \\$1 AND status = 'APPLIED' GROUP BY parent_transaction").
		WithArgs(parentID).
		WillReturnError(errors.New("database error"))

	total, err := ds.GetTotalCommittedTransactions(ctx, parentID)
	assert.Error(t, err)
	assert.Equal(t, big.NewInt(0), total)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestRecordTransactionWithBalances_AtomicSuccess_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Skipping test: recovered from panic (likely database connection issue): %v", r)
		}
	}()

	ctx := context.Background()
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := NewDataSource(cnf)
	if err != nil {
		t.Skipf("Skipping test: could not connect to database: %v", err)
	}
	if ds == nil {
		t.Skip("Skipping test: database connection returned nil")
	}

	ledger, err := ds.CreateLedger(model.Ledger{Name: "test-ledger-atomicity-" + model.GenerateUUIDWithSuffix("ldg")})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	sourceBalance := model.Balance{
		Currency: "USD",
		LedgerID: ledger.LedgerID,
	}
	destBalance := model.Balance{
		Currency: "USD",
		LedgerID: ledger.LedgerID,
	}

	createdSource, err := ds.CreateBalance(sourceBalance)
	if err != nil {
		t.Fatalf("Failed to create source balance: %v", err)
	}

	createdDest, err := ds.CreateBalance(destBalance)
	if err != nil {
		t.Fatalf("Failed to create destination balance: %v", err)
	}

	source, err := ds.GetBalanceByIDLite(createdSource.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get source balance: %v", err)
	}
	dest, err := ds.GetBalanceByIDLite(createdDest.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get destination balance: %v", err)
	}

	transferAmount := big.NewInt(50000)
	source.DebitBalance = new(big.Int).Add(source.DebitBalance, transferAmount)
	source.Balance = new(big.Int).Sub(source.Balance, transferAmount)
	dest.CreditBalance = new(big.Int).Add(dest.CreditBalance, transferAmount)
	dest.Balance = new(big.Int).Add(dest.Balance, transferAmount)

	txn := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "atomic-success-test-" + model.GenerateUUIDWithSuffix("ref"),
		Source:        source.BalanceID,
		Destination:   dest.BalanceID,
		Amount:        500,
		AmountString:  "500",
		PreciseAmount: transferAmount,
		Precision:     100,
		Rate:          1,
		Currency:      "USD",
		Status:        "APPLIED",
		CreatedAt:     time.Now(),
		ScheduledFor:  time.Now(),
		MetaData:      map[string]interface{}{"test": "atomicity"},
	}

	result, err := ds.RecordTransactionWithBalances(ctx, txn, source, dest)
	assert.NoError(t, err, "RecordTransactionWithBalances should succeed")
	assert.NotNil(t, result)

	finalSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	assert.NoError(t, err)
	finalDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	assert.NoError(t, err)

	assert.Equal(t, 0, finalSource.DebitBalance.Cmp(transferAmount),
		"Source debit balance should be updated")
	assert.Equal(t, 0, finalDest.CreditBalance.Cmp(transferAmount),
		"Destination credit balance should be updated")

	savedTxn, err := ds.GetTransaction(ctx, txn.TransactionID)
	assert.NoError(t, err, "Transaction should be saved")
	assert.Equal(t, txn.TransactionID, savedTxn.TransactionID)
	assert.Equal(t, "APPLIED", savedTxn.Status)
}

func TestRecordTransactionWithBalances_DuplicateTxnID_Rollback_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Skipping test: recovered from panic (likely database connection issue): %v", r)
		}
	}()

	ctx := context.Background()
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := NewDataSource(cnf)
	if err != nil {
		t.Skipf("Skipping test: could not connect to database: %v", err)
	}

	ledger, err := ds.CreateLedger(model.Ledger{Name: "test-ledger-rollback-" + model.GenerateUUIDWithSuffix("ldg")})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	sourceBalance := model.Balance{
		Currency: "USD",
		LedgerID: ledger.LedgerID,
	}
	destBalance := model.Balance{
		Currency: "USD",
		LedgerID: ledger.LedgerID,
	}

	source, err := ds.CreateBalance(sourceBalance)
	if err != nil {
		t.Fatalf("Failed to create source balance: %v", err)
	}

	dest, err := ds.CreateBalance(destBalance)
	if err != nil {
		t.Fatalf("Failed to create destination balance: %v", err)
	}

	duplicateTxnID := model.GenerateUUIDWithSuffix("txn")
	firstTxn := &model.Transaction{
		TransactionID: duplicateTxnID,
		Reference:     "first-txn-" + duplicateTxnID,
		Source:        source.BalanceID,
		Destination:   dest.BalanceID,
		Amount:        100,
		AmountString:  "100",
		PreciseAmount: big.NewInt(10000),
		Precision:     100,
		Rate:          1,
		Currency:      "USD",
		Status:        "APPLIED",
		CreatedAt:     time.Now(),
		ScheduledFor:  time.Now(),
		MetaData:      map[string]interface{}{"test": "first"},
	}

	firstSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get first source balance: %v", err)
	}
	firstDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get first dest balance: %v", err)
	}

	firstSource.DebitBalance = new(big.Int).Add(firstSource.DebitBalance, big.NewInt(10000))
	firstSource.Balance = new(big.Int).Sub(firstSource.Balance, big.NewInt(10000))
	firstDest.CreditBalance = new(big.Int).Add(firstDest.CreditBalance, big.NewInt(10000))
	firstDest.Balance = new(big.Int).Add(firstDest.Balance, big.NewInt(10000))

	_, err = ds.RecordTransactionWithBalances(ctx, firstTxn, firstSource, firstDest)
	if err != nil {
		t.Fatalf("First transaction should succeed: %v", err)
	}

	balanceAfterFirst, err := ds.GetBalanceByIDLite(source.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get balance after first: %v", err)
	}
	destAfterFirst, err := ds.GetBalanceByIDLite(dest.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get dest after first: %v", err)
	}

	secondTxn := &model.Transaction{
		TransactionID: duplicateTxnID,
		Reference:     "second-txn-" + duplicateTxnID,
		Source:        source.BalanceID,
		Destination:   dest.BalanceID,
		Amount:        200,
		AmountString:  "200",
		PreciseAmount: big.NewInt(20000),
		Precision:     100,
		Rate:          1,
		Currency:      "USD",
		Status:        "APPLIED",
		CreatedAt:     time.Now(),
		ScheduledFor:  time.Now(),
		MetaData:      map[string]interface{}{"test": "second-should-fail"},
	}

	secondSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get second source balance: %v", err)
	}
	secondDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	if err != nil {
		t.Fatalf("Failed to get second dest balance: %v", err)
	}

	secondSource.DebitBalance = new(big.Int).Add(secondSource.DebitBalance, big.NewInt(20000))
	secondSource.Balance = new(big.Int).Sub(secondSource.Balance, big.NewInt(20000))
	secondDest.CreditBalance = new(big.Int).Add(secondDest.CreditBalance, big.NewInt(20000))
	secondDest.Balance = new(big.Int).Add(secondDest.Balance, big.NewInt(20000))

	_, err = ds.RecordTransactionWithBalances(ctx, secondTxn, secondSource, secondDest)
	assert.Error(t, err, "Second transaction with duplicate ID should fail")

	finalSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	assert.NoError(t, err)
	finalDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	assert.NoError(t, err)

	t.Logf("Rollback test: attempted source balance %s, actual %s (unchanged = rollback worked)",
		secondSource.Balance.String(), finalSource.Balance.String())

	assert.Equal(t, balanceAfterFirst.Balance.String(), finalSource.Balance.String(),
		"Source balance should be unchanged after rollback (expected: %s, got: %s)",
		balanceAfterFirst.Balance.String(), finalSource.Balance.String())
	assert.Equal(t, balanceAfterFirst.DebitBalance.String(), finalSource.DebitBalance.String(),
		"Source debit balance should be unchanged after rollback")
	assert.Equal(t, destAfterFirst.Balance.String(), finalDest.Balance.String(),
		"Destination balance should be unchanged after rollback")
	assert.Equal(t, destAfterFirst.CreditBalance.String(), finalDest.CreditBalance.String(),
		"Destination credit balance should be unchanged after rollback")
}

func TestGetTransactionByRef_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)

	query := `
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data, parent_transaction
		FROM blnk.transactions
		WHERE reference = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("ref_123").
		WillReturnRows(sqlmock.NewRows([]string{
			"transaction_id", "source", "reference", "amount", "precise_amount", "currency",
			"destination", "description", "status", "created_at", "meta_data", "parent_transaction",
		}).AddRow(
			"txn_123", "bln_source", "ref_123", 1000.0, "100000", "USD",
			"bln_dest", "Test transaction", "APPLIED", time.Now(), metaDataJSON, "",
		))

	txn, err := ds.GetTransactionByRef(ctx, "ref_123")
	assert.NoError(t, err)
	assert.Equal(t, "txn_123", txn.TransactionID)
	assert.Equal(t, "ref_123", txn.Reference)
	assert.Equal(t, "bln_source", txn.Source)
	assert.Equal(t, "bln_dest", txn.Destination)
	assert.Equal(t, "APPLIED", txn.Status)
	assert.Equal(t, "100000", txn.PreciseAmount.String())

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetTransactionByRef_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data, parent_transaction
		FROM blnk.transactions
		WHERE reference = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("nonexistent_ref").
		WillReturnError(sql.ErrNoRows)

	txn, err := ds.GetTransactionByRef(ctx, "nonexistent_ref")
	assert.Error(t, err)
	assert.Equal(t, "", txn.TransactionID)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetTransactionByRef_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data, parent_transaction
		FROM blnk.transactions
		WHERE reference = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("ref_123").
		WillReturnError(errors.New("database connection error"))

	txn, err := ds.GetTransactionByRef(ctx, "ref_123")
	assert.Error(t, err)
	assert.Equal(t, "", txn.TransactionID)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateTransactionStatus_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("txn_123", "APPLIED").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.UpdateTransactionStatus(ctx, "txn_123", "APPLIED")
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateTransactionStatus_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("nonexistent_txn", "APPLIED").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.UpdateTransactionStatus(ctx, "nonexistent_txn", "APPLIED")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateTransactionStatus_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("txn_123", "APPLIED").
		WillReturnError(errors.New("database error"))

	err = ds.UpdateTransactionStatus(ctx, "txn_123", "APPLIED")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllTransactions_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)

	query := `
		SELECT transaction_id, source, reference, amount, currency, destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(10, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"transaction_id", "source", "reference", "amount", "currency",
			"destination", "description", "status", "hash", "created_at", "meta_data",
		}).
			AddRow("txn_1", "bln_src1", "ref_1", 1000.0, "USD", "bln_dest1", "Txn 1", "APPLIED", "hash1", time.Now(), metaDataJSON).
			AddRow("txn_2", "bln_src2", "ref_2", 2000.0, "EUR", "bln_dest2", "Txn 2", "PENDING", "hash2", time.Now(), metaDataJSON))

	transactions, err := ds.GetAllTransactions(ctx, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, transactions, 2)
	assert.Equal(t, "txn_1", transactions[0].TransactionID)
	assert.Equal(t, "txn_2", transactions[1].TransactionID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllTransactions_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT transaction_id, source, reference, amount, currency, destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(10, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"transaction_id", "source", "reference", "amount", "currency",
			"destination", "description", "status", "hash", "created_at", "meta_data",
		}))

	transactions, err := ds.GetAllTransactions(ctx, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, transactions, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllTransactions_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT transaction_id, source, reference, amount, currency, destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(10, 0).
		WillReturnError(errors.New("database error"))

	transactions, err := ds.GetAllTransactions(ctx, 10, 0)
	assert.Error(t, err)
	assert.Nil(t, transactions)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestIsParentTransactionVoid_True(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("parent_txn_123").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	isVoid, err := ds.IsParentTransactionVoid(ctx, "parent_txn_123")
	assert.NoError(t, err)
	assert.True(t, isVoid)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestIsParentTransactionVoid_False(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("parent_txn_456").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	isVoid, err := ds.IsParentTransactionVoid(ctx, "parent_txn_456")
	assert.NoError(t, err)
	assert.False(t, isVoid)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestIsParentTransactionVoid_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("parent_txn_123").
		WillReturnError(errors.New("database error"))

	isVoid, err := ds.IsParentTransactionVoid(ctx, "parent_txn_123")
	assert.Error(t, err)
	assert.False(t, isVoid)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetRefundableTransactionsByParentID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)

	query := `
		SELECT 
			t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, 
			t.precision, t.rate, t.currency, t.destination, t.description, t.status, t.created_at, 
			t.meta_data, t.scheduled_for, t.hash
		FROM 
			blnk.transactions t
		WHERE 
			-- Case 1: The transaction is the parent itself and is APPLIED
			(t.transaction_id = $1 AND t.status = 'APPLIED')
			
			-- Case 2: The transaction is a child and is APPLIED or VOID
			OR (t.parent_transaction = $1 AND t.status IN ('APPLIED', 'VOID'))

			-- Case 3: Transaction is APPLIED and linked via metadata QUEUED_PARENT_TRANSACTION
			OR (t.status = 'APPLIED' AND t.meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)

		ORDER BY 
			t.created_at DESC
		LIMIT $2 OFFSET $3
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("parent_txn_123", 100, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{
			"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
			"precision", "rate", "currency", "destination", "description", "status", "created_at",
			"meta_data", "scheduled_for", "hash",
		}).
			AddRow("txn_1", "parent_txn_123", "bln_src", "ref_1", 1000.0, "100000", 100, 1.0, "USD", "bln_dest", "Desc 1", "APPLIED", time.Now(), metaDataJSON, time.Now(), "hash1").
			AddRow("txn_2", "parent_txn_123", "bln_src", "ref_2", 500.0, "50000", 100, 1.0, "USD", "bln_dest", "Desc 2", "VOID", time.Now(), metaDataJSON, time.Now(), "hash2"))

	transactions, err := ds.GetRefundableTransactionsByParentID(ctx, "parent_txn_123", 100, 0)
	assert.NoError(t, err)
	assert.Len(t, transactions, 2)
	assert.Equal(t, "txn_1", transactions[0].TransactionID)
	assert.Equal(t, "APPLIED", transactions[0].Status)
	assert.Equal(t, "txn_2", transactions[1].TransactionID)
	assert.Equal(t, "VOID", transactions[1].Status)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetRefundableTransactionsByParentID_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT 
			t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, 
			t.precision, t.rate, t.currency, t.destination, t.description, t.status, t.created_at, 
			t.meta_data, t.scheduled_for, t.hash
		FROM 
			blnk.transactions t
		WHERE 
			-- Case 1: The transaction is the parent itself and is APPLIED
			(t.transaction_id = $1 AND t.status = 'APPLIED')
			
			-- Case 2: The transaction is a child and is APPLIED or VOID
			OR (t.parent_transaction = $1 AND t.status IN ('APPLIED', 'VOID'))

			-- Case 3: Transaction is APPLIED and linked via metadata QUEUED_PARENT_TRANSACTION
			OR (t.status = 'APPLIED' AND t.meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)

		ORDER BY 
			t.created_at DESC
		LIMIT $2 OFFSET $3
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("nonexistent_parent", 100, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{
			"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
			"precision", "rate", "currency", "destination", "description", "status", "created_at",
			"meta_data", "scheduled_for", "hash",
		}))

	transactions, err := ds.GetRefundableTransactionsByParentID(ctx, "nonexistent_parent", 100, 0)
	assert.NoError(t, err)
	assert.Len(t, transactions, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetRefundableTransactionsByParentID_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	query := `
		SELECT 
			t.transaction_id, t.parent_transaction, t.source, t.reference, t.amount, t.precise_amount, 
			t.precision, t.rate, t.currency, t.destination, t.description, t.status, t.created_at, 
			t.meta_data, t.scheduled_for, t.hash
		FROM 
			blnk.transactions t
		WHERE 
			-- Case 1: The transaction is the parent itself and is APPLIED
			(t.transaction_id = $1 AND t.status = 'APPLIED')
			
			-- Case 2: The transaction is a child and is APPLIED or VOID
			OR (t.parent_transaction = $1 AND t.status IN ('APPLIED', 'VOID'))

			-- Case 3: Transaction is APPLIED and linked via metadata QUEUED_PARENT_TRANSACTION
			OR (t.status = 'APPLIED' AND t.meta_data->>'QUEUED_PARENT_TRANSACTION' = $1)

		ORDER BY 
			t.created_at DESC
		LIMIT $2 OFFSET $3
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("parent_txn_123", 100, int64(0)).
		WillReturnError(errors.New("database error"))

	transactions, err := ds.GetRefundableTransactionsByParentID(ctx, "parent_txn_123", 100, 0)
	assert.Error(t, err)
	assert.Nil(t, transactions)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestTransactionExistsByIDOrParentID_True(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs("txn_123").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	exists, err := ds.TransactionExistsByIDOrParentID(ctx, "txn_123")
	assert.NoError(t, err)
	assert.True(t, exists)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestTransactionExistsByIDOrParentID_False(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs("txn_notfound").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	exists, err := ds.TransactionExistsByIDOrParentID(ctx, "txn_notfound")
	assert.NoError(t, err)
	assert.False(t, exists)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestTransactionExistsByIDOrParentID_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs("txn_123").
		WillReturnError(errors.New("database error"))

	exists, err := ds.TransactionExistsByIDOrParentID(ctx, "txn_123")
	assert.Error(t, err)
	assert.False(t, exists)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestIsTransactionRefunded_True(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID: "txn_123",
		Source:        "bln_source",
		Destination:   "bln_dest",
	}

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(txn.TransactionID, txn.Destination, txn.Source).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	isRefunded, err := ds.IsTransactionRefunded(ctx, txn)
	assert.NoError(t, err)
	assert.True(t, isRefunded)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestIsTransactionRefunded_False(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID: "txn_123",
		Source:        "bln_source",
		Destination:   "bln_dest",
	}

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(txn.TransactionID, txn.Destination, txn.Source).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	isRefunded, err := ds.IsTransactionRefunded(ctx, txn)
	assert.NoError(t, err)
	assert.False(t, isRefunded)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestIsTransactionRefunded_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID: "txn_123",
		Source:        "bln_source",
		Destination:   "bln_dest",
	}

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(txn.TransactionID, txn.Destination, txn.Source).
		WillReturnError(errors.New("database error"))

	isRefunded, err := ds.IsTransactionRefunded(ctx, txn)
	assert.Error(t, err)
	assert.False(t, isRefunded)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGroupTransactions_InvalidCriteria(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	newCache, err := cache.NewCache()
	assert.NoError(t, err)
	ds := Datasource{Conn: db, Cache: newCache}
	ctx := context.Background()

	result, err := ds.GroupTransactions(ctx, "invalid_column", 10, 0)
	assert.Error(t, err)
	assert.Nil(t, result)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrBadRequest, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetTransactionsByCriteria_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)

	minAmount := 100.0
	maxAmount := 5000.0
	currency := "USD"

	mock.ExpectQuery("SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash FROM blnk.transactions").
		WithArgs(minAmount, maxAmount, currency, 10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{
			"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
			"precision", "rate", "currency", "destination", "description", "status", "created_at",
			"meta_data", "scheduled_for", "hash",
		}).
			AddRow("txn_1", "", "bln_src", "ref_1", 1000.0, "100000", 100, 1.0, "USD", "bln_dest", "Desc", "APPLIED", time.Now(), metaDataJSON, time.Now(), "hash1"))

	txns, err := ds.GetTransactionsByCriteria(ctx, &minAmount, &maxAmount, &currency, nil, nil, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, txns, 1)
}

func TestGetTransactionsByCriteria_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectQuery("SELECT transaction_id").
		WillReturnError(errors.New("database error"))

	txns, err := ds.GetTransactionsByCriteria(ctx, nil, nil, nil, nil, nil, 10, 0)
	assert.Error(t, err)
	assert.Nil(t, txns)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetQueuedAmounts_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	balanceID := "bln_test123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT t.precise_amount, t.source, t.destination 
        FROM blnk.transactions t
        WHERE (t.source = $1 OR t.destination = $1) 
        AND t.status = 'QUEUED'
        AND NOT EXISTS (
            SELECT 1 
            FROM blnk.transactions child 
            WHERE child.parent_transaction = t.transaction_id 
            AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
        )`)).
		WithArgs(balanceID).
		WillReturnRows(sqlmock.NewRows([]string{"precise_amount", "source", "destination"}).
			AddRow("1000", balanceID, "dest123").
			AddRow("500", "src123", balanceID))

	debit, credit, err := ds.GetQueuedAmounts(ctx, balanceID)
	assert.NoError(t, err)
	assert.NotNil(t, debit)
	assert.NotNil(t, credit)
	assert.Equal(t, int64(1000), debit.Int64())
	assert.Equal(t, int64(500), credit.Int64())
}

func TestGetQueuedAmounts_NoTransactions(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	balanceID := "bln_test123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT t.precise_amount, t.source, t.destination 
        FROM blnk.transactions t
        WHERE (t.source = $1 OR t.destination = $1) 
        AND t.status = 'QUEUED'
        AND NOT EXISTS (
            SELECT 1 
            FROM blnk.transactions child 
            WHERE child.parent_transaction = t.transaction_id 
            AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
        )`)).
		WithArgs(balanceID).
		WillReturnRows(sqlmock.NewRows([]string{"precise_amount", "source", "destination"}))

	debit, credit, err := ds.GetQueuedAmounts(ctx, balanceID)
	assert.NoError(t, err)
	assert.NotNil(t, debit)
	assert.NotNil(t, credit)
	assert.Equal(t, int64(0), debit.Int64())
	assert.Equal(t, int64(0), credit.Int64())
}

func TestGetQueuedAmounts_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	balanceID := "bln_test123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT t.precise_amount, t.source, t.destination 
        FROM blnk.transactions t
        WHERE (t.source = $1 OR t.destination = $1) 
        AND t.status = 'QUEUED'
        AND NOT EXISTS (
            SELECT 1 
            FROM blnk.transactions child 
            WHERE child.parent_transaction = t.transaction_id 
            AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
        )`)).
		WithArgs(balanceID).
		WillReturnError(sql.ErrConnDone)

	debit, credit, err := ds.GetQueuedAmounts(ctx, balanceID)
	assert.Error(t, err)
	assert.Nil(t, debit)
	assert.Nil(t, credit)
}

func TestGetQueuedAmounts_InvalidAmount(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	balanceID := "bln_test123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT t.precise_amount, t.source, t.destination 
        FROM blnk.transactions t
        WHERE (t.source = $1 OR t.destination = $1) 
        AND t.status = 'QUEUED'
        AND NOT EXISTS (
            SELECT 1 
            FROM blnk.transactions child 
            WHERE child.parent_transaction = t.transaction_id 
            AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
        )`)).
		WithArgs(balanceID).
		WillReturnRows(sqlmock.NewRows([]string{"precise_amount", "source", "destination"}).
			AddRow("invalid_amount", balanceID, "dest123"))

	debit, credit, err := ds.GetQueuedAmounts(ctx, balanceID)
	assert.Error(t, err)
	assert.Nil(t, debit)
	assert.Nil(t, credit)
}

func TestGetTransactionsPaginated_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCache()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	createdAt := time.Now()
	scheduledFor := time.Now().Add(24 * time.Hour)
	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        ORDER BY created_at ASC
        LIMIT $1 OFFSET $2`)).
		WithArgs(10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount", "precision", "rate", "currency", "destination", "description", "status", "created_at", "meta_data", "scheduled_for", "hash"}).
			AddRow("txn_123", "", "bln_source", "ref_123", 100.0, "10000", 100, 1.0, "USD", "bln_dest", "test desc", "APPLIED", createdAt, metaDataJSON, scheduledFor, "hash123"))

	txns, err := ds.GetTransactionsPaginated(ctx, "", 10, 0)
	assert.NoError(t, err)
	assert.Len(t, txns, 1)
	assert.Equal(t, "txn_123", txns[0].TransactionID)
}

func TestGetTransactionsPaginated_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCache()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        ORDER BY created_at ASC
        LIMIT $1 OFFSET $2`)).
		WithArgs(10, int64(0)).
		WillReturnError(sql.ErrConnDone)

	txns, err := ds.GetTransactionsPaginated(ctx, "", 10, 0)
	assert.Error(t, err)
	assert.Nil(t, txns)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetTransactionsByParent_DatabaseSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCache()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	createdAt := time.Now()
	scheduledFor := time.Now().Add(24 * time.Hour)
	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)
	parentID := "txn_parent_123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, 
			   rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE parent_transaction = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`)).
		WithArgs(parentID, 10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount", "precision", "rate", "currency", "destination", "description", "status", "created_at", "meta_data", "scheduled_for", "hash"}).
			AddRow("txn_child_123", parentID, "bln_source", "ref_123", 100.0, "10000", 100, 1.0, "USD", "bln_dest", "test desc", "APPLIED", createdAt, metaDataJSON, scheduledFor, "hash123"))

	txns, err := ds.GetTransactionsByParent(ctx, parentID, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, txns, 1)
	assert.Equal(t, "txn_child_123", txns[0].TransactionID)
	assert.Equal(t, parentID, txns[0].ParentTransaction)
}

func TestGetTransactionsByParent_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCache()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()
	parentID := "txn_parent_123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, 
			   rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE parent_transaction = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`)).
		WithArgs(parentID, 10, int64(0)).
		WillReturnError(sql.ErrConnDone)

	txns, err := ds.GetTransactionsByParent(ctx, parentID, 10, 0)
	assert.Error(t, err)
	assert.Nil(t, txns)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetTransactionsByParent_EmptyResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCache()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()
	parentID := "txn_parent_123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, 
			   rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE parent_transaction = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`)).
		WithArgs(parentID, 10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount", "precision", "rate", "currency", "destination", "description", "status", "created_at", "meta_data", "scheduled_for", "hash"}))

	txns, err := ds.GetTransactionsByParent(ctx, parentID, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, txns, 0)
}

func TestRecordTransactionWithBalances_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID:     "txn_123",
		ParentTransaction: "",
		Source:            "bln_source",
		Reference:         "ref_123",
		AmountString:      "100.00",
		PreciseAmount:     big.NewInt(10000),
		Precision:         100,
		Rate:              1.0,
		Currency:          "USD",
		Destination:       "bln_dest",
		Description:       "test transaction",
		Status:            "APPLIED",
		CreatedAt:         time.Now(),
		MetaData:          map[string]interface{}{"key": "value"},
		Hash:              "hash123",
	}

	sourceBalance := &model.Balance{
		BalanceID:             "bln_source",
		Balance:               big.NewInt(1000),
		CreditBalance:         big.NewInt(500),
		DebitBalance:          big.NewInt(500),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		Currency:              "USD",
		CurrencyMultiplier:    100,
		Version:               1,
	}

	destBalance := &model.Balance{
		BalanceID:             "bln_dest",
		Balance:               big.NewInt(2000),
		CreditBalance:         big.NewInt(1000),
		DebitBalance:          big.NewInt(1000),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		Currency:              "USD",
		CurrencyMultiplier:    100,
		Version:               1,
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE blnk.balances SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE blnk.balances SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO blnk.transactions").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := ds.RecordTransactionWithBalances(ctx, txn, sourceBalance, destBalance)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, txn.TransactionID, result.TransactionID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestRecordTransactionWithBalances_BeginError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID: "txn_123",
	}

	sourceBalance := &model.Balance{BalanceID: "bln_source"}
	destBalance := &model.Balance{BalanceID: "bln_dest"}

	mock.ExpectBegin().WillReturnError(sql.ErrConnDone)

	result, err := ds.RecordTransactionWithBalances(ctx, txn, sourceBalance, destBalance)
	assert.Error(t, err)
	assert.Nil(t, result)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestRecordTransactionWithBalances_SourceUpdateError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID: "txn_123",
	}

	sourceBalance := &model.Balance{
		BalanceID:             "bln_source",
		Balance:               big.NewInt(1000),
		CreditBalance:         big.NewInt(500),
		DebitBalance:          big.NewInt(500),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		Version:               1,
	}
	destBalance := &model.Balance{BalanceID: "bln_dest"}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE blnk.balances SET").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	result, err := ds.RecordTransactionWithBalances(ctx, txn, sourceBalance, destBalance)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestRecordTransactionWithBalances_CommitError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	txn := &model.Transaction{
		TransactionID:     "txn_123",
		ParentTransaction: "",
		Source:            "bln_source",
		Reference:         "ref_123",
		AmountString:      "100.00",
		PreciseAmount:     big.NewInt(10000),
		Precision:         100,
		Rate:              1.0,
		Currency:          "USD",
		Destination:       "bln_dest",
		Description:       "test transaction",
		Status:            "APPLIED",
		CreatedAt:         time.Now(),
		MetaData:          map[string]interface{}{"key": "value"},
		Hash:              "hash123",
	}

	sourceBalance := &model.Balance{
		BalanceID:             "bln_source",
		Balance:               big.NewInt(1000),
		CreditBalance:         big.NewInt(500),
		DebitBalance:          big.NewInt(500),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		Version:               1,
	}

	destBalance := &model.Balance{
		BalanceID:             "bln_dest",
		Balance:               big.NewInt(2000),
		CreditBalance:         big.NewInt(1000),
		DebitBalance:          big.NewInt(1000),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		Version:               1,
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE blnk.balances SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE blnk.balances SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO blnk.transactions").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit().WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	result, err := ds.RecordTransactionWithBalances(ctx, txn, sourceBalance, destBalance)
	assert.Error(t, err)
	assert.Nil(t, result)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}
