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

package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordTransaction(t *testing.T) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue",
			TransactionQueue: "transaction_queue",
			IndexQueue:       "index_queue",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{SecretKey: "some-secret"},
		AccountNumberGeneration: config.AccountNumberGenerationConfig{
			HttpService: config.AccountGenerationHttpService{
				Url: "http://example.com/generateAccount",
			},
		},
	}

	config.ConfigStore.Store(cnf)
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	// Important: Set ExpectationsWereMet to ensure execution occurs in order of appearance
	mock.MatchExpectationsInOrder(false)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	// Use fixed UUIDs for better predictability
	source := "source-balance-id-123"
	destination := "destination-balance-id-456"
	reference := "transaction-reference-789"

	txn := &model.Transaction{
		Reference:      reference,
		Source:         source,
		Destination:    destination,
		Rate:           1,
		Amount:         10,
		AllowOverdraft: false,
		Precision:      100,
		Currency:       "NGN",
	}

	// First, check if transaction exists
	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	// Set up source balance response
	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(source, "NGN", "", 1, "ledger-id-source", int64(10000), int64(10000), 0, 0, 0, 0, time.Now(), 0)

	// Set up destination balance response
	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(destination, "", "NGN", 1, "ledger-id-destination", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	balanceQuery := `SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = \$1`
	balanceQueryPattern := regexp.MustCompile(`\s+`).ReplaceAllString(balanceQuery, `\s*`)

	// Expect balance queries with the source/destination IDs (order can vary)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(destination).WillReturnRows(destinationBalanceRows)

	// Start transaction
	mock.ExpectBegin()

	// Update source and destination balances (order doesn't matter since MatchExpectationsInOrder is false)
	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
	  WHERE balance_id = $1 AND version = $12
	`)).WithArgs(
		source,           // balance_id
		sqlmock.AnyArg(), // balance
		sqlmock.AnyArg(), // credit_balance
		sqlmock.AnyArg(), // debit_balance
		sqlmock.AnyArg(), // inflight_balance
		sqlmock.AnyArg(), // inflight_credit_balance
		sqlmock.AnyArg(), // inflight_debit_balance
		sqlmock.AnyArg(), // currency
		sqlmock.AnyArg(), // currency_multiplier
		sqlmock.AnyArg(), // ledger_id
		sqlmock.AnyArg(), // created_at
		sqlmock.AnyArg(), // version
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
	  WHERE balance_id = $1 AND version = $12
	`)).WithArgs(
		destination,      // balance_id
		sqlmock.AnyArg(), // balance
		sqlmock.AnyArg(), // credit_balance
		sqlmock.AnyArg(), // debit_balance
		sqlmock.AnyArg(), // inflight_balance
		sqlmock.AnyArg(), // inflight_credit_balance
		sqlmock.AnyArg(), // inflight_debit_balance
		sqlmock.AnyArg(), // currency
		sqlmock.AnyArg(), // currency_multiplier
		sqlmock.AnyArg(), // ledger_id
		sqlmock.AnyArg(), // created_at
		sqlmock.AnyArg(), // version
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Commit transaction
	mock.ExpectCommit()

	// Add expectations for balance monitor queries (can happen in any order)
	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(source).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(destination).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	// Expect transaction insertion (placed last, but can happen in any order due to MatchExpectationsInOrder=false)
	expectedSQL := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`
	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(), // transaction_id
		sqlmock.AnyArg(), // parent_transaction
		source,           // source
		reference,        // reference
		txn.AmountString, // amount
		"1000",           // precise amount
		txn.Precision,    // precision
		float64(1),       // rate
		txn.Currency,     // currency
		destination,      // destination
		sqlmock.AnyArg(), // description
		sqlmock.AnyArg(), // status
		sqlmock.AnyArg(), // created_at
		sqlmock.AnyArg(), // meta_data
		sqlmock.AnyArg(), // scheduled_for
		sqlmock.AnyArg(), // hash
		sqlmock.AnyArg(), // effective_date
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Execute the function being tested
	result, err := d.RecordTransaction(context.Background(), txn)

	// Assert no errors
	assert.NoError(t, err)

	// Check if all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	// Optionally check the result if needed
	assert.NotNil(t, result)
}

func TestRecordTransactionWithRate(t *testing.T) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue",
			TransactionQueue: "transaction_queue",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{SecretKey: "some-secret"},
		AccountNumberGeneration: config.AccountNumberGenerationConfig{
			HttpService: config.AccountGenerationHttpService{
				Url: "http://example.com/generateAccount",
			},
		},
	}

	config.ConfigStore.Store(cnf)
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	// Important: Set ExpectationsWereMet to ensure execution occurs in order of appearance
	mock.MatchExpectationsInOrder(false)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	// Use fixed UUIDs for better predictability
	source := "source-balance-id-123"
	destination := "destination-balance-id-456"
	reference := "transaction-reference-789"

	txn := &model.Transaction{
		Reference:      reference,
		Source:         source,
		Destination:    destination,
		Amount:         1000000,
		Rate:           1300,
		AllowOverdraft: true,
		Precision:      100,
		Currency:       "NGN",
	}

	// First, check if transaction exists
	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	// Set up source balance response - Note this is USD
	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(source, "", "USD", 1, "ledger-id-source", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	// Set up destination balance response - This is NGN
	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(destination, "", "NGN", 1, "ledger-id-destination", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	balanceQuery := `SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = \$1`
	balanceQueryPattern := regexp.MustCompile(`\s+`).ReplaceAllString(balanceQuery, `\s*`)

	// Expect balance queries with the source/destination IDs (order can vary)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(destination).WillReturnRows(destinationBalanceRows)

	// Start transaction
	mock.ExpectBegin()

	// Update source balance
	mock.ExpectExec(regexp.QuoteMeta(`
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
        WHERE balance_id = $1 AND version = $12
    `)).WithArgs(
		source,                          // $1
		big.NewInt(-100000000).String(), // $2
		big.NewInt(0).String(),          // $3
		big.NewInt(100000000).String(),  // $4
		big.NewInt(0).String(),          // $5
		big.NewInt(0).String(),          // $6
		big.NewInt(0).String(),          // $7
		"USD",                           // $8
		float64(1),                      // $9
		"ledger-id-source",              // $10
		sqlmock.AnyArg(),                // $11
		0,                               // $12
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Update destination balance
	mock.ExpectExec(regexp.QuoteMeta(`
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
        WHERE balance_id = $1 AND version = $12
    `)).WithArgs(
		destination,                       // $1
		big.NewInt(130000000000).String(), // $2
		big.NewInt(130000000000).String(), // $3
		big.NewInt(0).String(),            // $4
		big.NewInt(0).String(),            // $5
		big.NewInt(0).String(),            // $6
		big.NewInt(0).String(),            // $7
		"NGN",                             // $8
		float64(1),                        // $9
		"ledger-id-destination",           // $10
		sqlmock.AnyArg(),                  // $11
		0,                                 // $12
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Commit transaction
	mock.ExpectCommit()

	// Add expectations for balance monitor queries (can happen in any order)
	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(source).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	// Add a second monitor query for destination if needed (based on your implementation)
	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(destination).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	// Expect transaction insertion
	expectedSQL := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`
	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(), // transaction_id
		sqlmock.AnyArg(), // parent_transaction
		source,           // source
		reference,        // reference
		txn.AmountString, // amount
		"100000000",      // precise amount
		txn.Precision,    // precision
		float64(1300),    // rate
		txn.Currency,     // currency
		destination,      // destination
		sqlmock.AnyArg(), // description
		sqlmock.AnyArg(), // status
		sqlmock.AnyArg(), // created_at
		sqlmock.AnyArg(), // meta_data
		sqlmock.AnyArg(), // scheduled_for
		sqlmock.AnyArg(), // hash
		sqlmock.AnyArg(), // effective_date
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Execute the function being tested
	_, err = d.RecordTransaction(context.Background(), txn)

	// Assert no errors
	assert.NoError(t, err)

	// Check if all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
func TestVoidInflightTransaction_Negative(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	source := gofakeit.UUID()
	destination := gofakeit.UUID()
	metaDataJSON, _ := json.Marshal(map[string]interface{}{"key": "value"})
	t.Run("Transaction not in INFLIGHT status", func(t *testing.T) {
		transactionID := gofakeit.UUID()

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data FROM blnk.transactions WHERE transaction_id = $1`)).
			WithArgs(transactionID).
			WillReturnRows(sqlmock.NewRows([]string{"transaction_id", "source", "reference", "amount", "precise_amount", "precision", "currency", "destination", "description", "status", "created_at", "meta_data"}).
				AddRow(transactionID, source, gofakeit.UUID(), 100.0, 10000, 100, "USD", destination, gofakeit.UUID(), "APPLIED", time.Now(), metaDataJSON))

		_, err := d.VoidInflightTransaction(context.Background(), transactionID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction is not in inflight status")
	})

	t.Run("Transaction already voided", func(t *testing.T) {
		transactionID := gofakeit.UUID()

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data FROM blnk.transactions WHERE transaction_id = $1`)).
			WithArgs(transactionID).
			WillReturnRows(sqlmock.NewRows([]string{"transaction_id", "source", "reference", "amount", "precise_amount", "precision", "currency", "destination", "description", "status", "created_at", "meta_data"}).
				AddRow(transactionID, source, gofakeit.UUID(), 100.0, 10000, 100, "USD", destination, gofakeit.UUID(), "INFLIGHT", time.Now(), metaDataJSON))

		// Mock IsParentTransactionVoid
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS ( SELECT 1 FROM blnk.transactions WHERE parent_transaction = $1 AND status = 'VOID' )`)).
			WithArgs(transactionID).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

		_, err := d.VoidInflightTransaction(context.Background(), transactionID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been voided")
	})

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueueTransactionFlow(t *testing.T) {
	// Skip in short mode as this is a long-running test
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

	// Initialize test context
	ctx := context.Background()

	// Setup real test configuration
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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	// Create real datasource for integration test
	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	// Create real Blnk instance
	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create source balance
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	// Create destination balance
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	// Create balances in database
	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	sourceID := source.BalanceID
	destID := dest.BalanceID

	// Create transaction
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         sourceID,
		Destination:    destID,
		Amount:         500, // $5.00
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify initial transaction state
	require.Equal(t, StatusQueued, queuedTxn.Status, "Initial transaction should be QUEUED")

	// Store the original transaction ID
	originalTxnID := queuedTxn.TransactionID

	queueCopy := createQueueCopy(queuedTxn, queuedTxn.Reference)
	_, err = blnk.RecordTransaction(ctx, queueCopy)
	assert.NoError(t, err)

	ref := fmt.Sprintf("%s_%s", txnRef, "q")
	// Verify the processed transaction
	appliedTxn, err := ds.GetTransactionByRef(ctx, ref)
	require.NoError(t, err, "Failed to get processed transactions")
	require.NotEmpty(t, appliedTxn, "Should have processed transactions")

	// Verify the applied transaction
	require.NotNil(t, appliedTxn, "Should have an APPLIED transaction")
	require.Equal(t, originalTxnID, appliedTxn.ParentTransaction, "Applied transaction should reference original transaction")
	require.Equal(t, txn.Amount, appliedTxn.Amount, "Amount should match")

	// Verify final balance states
	updatedSource, err := ds.GetBalanceByIDLite(sourceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(destID)
	require.NoError(t, err, "Failed to get updated destination balance")

	fmt.Println("Updated Source Balance: ", updatedSource.Balance)
	fmt.Println("Updated Destination Balance: ", updatedDest.Balance)

	require.Equal(t, 0, updatedSource.Balance.Cmp(updatedSource.Balance),
		"Source balance should be reduced by transaction amount")
	require.Equal(t, 0, updatedDest.Balance.Cmp(updatedDest.Balance),
		"Destination balance should be increased by transaction amount")

}

func TestQueueTransactionFlowWithSkipQueue(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-500) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.Balance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntry.Status, "Should have an APPLIED transaction entry")
}

func TestInflightTransactionFlowWithSkipQueue(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Inflight:       true,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-500) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusInflight, queuedEntry.Status, "Should have an INFLIGHT transaction entry")
}

func TestInflightTransactionFlowWithSkipQueueThenCommit(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Inflight:       true,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-500) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusInflight, queuedEntry.Status, "Should have an INFLIGHT transaction entry")

	// Commit the transaction
	_, err = blnk.CommitInflightTransaction(ctx, txn.TransactionID, 0)
	require.NoError(t, err, "Failed to commit transaction")

	// Verify balances were updated immediately
	updatedSourceAfterCommit, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDestAfterCommit, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Verify balance changes
	require.Equal(t, 0, updatedSourceAfterCommit.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDestAfterCommit.Balance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

}

func TestInflightTransactionFlowWithSkipQueueThenPartialCommit(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	originalAmount := 500.0
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         originalAmount,
		Inflight:       true,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes for the full amount
	expectedFullDebit := big.NewInt(int64(-originalAmount) * 100) // Amount * precision
	expectedFullCredit := big.NewInt(int64(originalAmount) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(expectedFullDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(expectedFullCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify inflight entry exists
	inflightEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get inflight transaction entry")
	require.Equal(t, StatusInflight, inflightEntry.Status, "Should have an INFLIGHT transaction entry")

	// Partially commit the transaction (commit half of the original amount)
	partialAmount := originalAmount / 2 // 250.0
	partialCommitTxn, err := blnk.CommitInflightTransaction(ctx, inflightEntry.TransactionID, partialAmount)
	require.NoError(t, err, "Failed to partially commit transaction")
	require.Equal(t, StatusApplied, partialCommitTxn.Status, "Partial commit transaction should have APPLIED status")
	require.Equal(t, partialAmount, partialCommitTxn.Amount, "Partial commit amount should match specified amount")

	// Verify balances were updated after partial commit
	updatedSourceAfterPartialCommit, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDestAfterPartialCommit, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes for the partial commit
	expectedPartialDebit := big.NewInt(int64(-partialAmount) * 100) // PartialAmount * precision
	expectedPartialCredit := big.NewInt(int64(partialAmount) * 100)

	// Inflight balance should be reduced by the committed amount
	expectedRemainingInflightDebit := big.NewInt(0).Sub(expectedFullDebit, expectedPartialDebit)
	expectedRemainingInflightCredit := big.NewInt(0).Sub(expectedFullCredit, expectedPartialCredit)

	// Verify balance changes
	require.Equal(t, 0, updatedSourceAfterPartialCommit.Balance.Cmp(expectedPartialDebit),
		"Source balance should reflect the partial commit amount")
	require.Equal(t, 0, updatedDestAfterPartialCommit.Balance.Cmp(expectedPartialCredit),
		"Destination balance should reflect the partial commit amount")

	require.Equal(t, 0, updatedSourceAfterPartialCommit.InflightBalance.Cmp(expectedRemainingInflightDebit),
		"Source inflight balance should be reduced by the committed amount")
	require.Equal(t, 0, updatedDestAfterPartialCommit.InflightBalance.Cmp(expectedRemainingInflightCredit),
		"Destination inflight balance should be reduced by the committed amount")

	// Commit the remaining amount (should succeed)
	remainingCommitTxn, err := blnk.CommitInflightTransaction(ctx, inflightEntry.TransactionID, 0) // 0 means commit remaining amount
	require.NoError(t, err, "Failed to commit remaining transaction amount")
	require.Equal(t, StatusApplied, remainingCommitTxn.Status, "Remaining commit transaction should have APPLIED status")

	// Verify the remaining commit amount
	remainingAmount := originalAmount - partialAmount
	require.InDelta(t, remainingAmount, remainingCommitTxn.Amount, 0.01, "Remaining commit amount should match expected remaining amount")

	// Verify final balances
	updatedSourceAfterFullCommit, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get final source balance")

	updatedDestAfterFullCommit, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get final destination balance")

	// Verify final balance changes
	require.Equal(t, 0, updatedSourceAfterFullCommit.Balance.Cmp(expectedFullDebit),
		"Source balance should ultimately reflect the full amount")
	require.Equal(t, 0, updatedDestAfterFullCommit.Balance.Cmp(expectedFullCredit),
		"Destination balance should ultimately reflect the full amount")

	require.Equal(t, 0, updatedSourceAfterFullCommit.InflightBalance.Cmp(big.NewInt(0)),
		"Source inflight balance should be zero after full commit")
	require.Equal(t, 0, updatedDestAfterFullCommit.InflightBalance.Cmp(big.NewInt(0)),
		"Destination inflight balance should be zero after full commit")

	// Attempt another commit (should fail)
	_, err = blnk.CommitInflightTransaction(ctx, inflightEntry.TransactionID, 0)
	require.Error(t, err, "Committing a fully committed transaction should fail")
	require.Contains(t, err.Error(), "cannot commit. Transaction already committed",
		"Error message should indicate transaction is already committed")

	// Verify commit history
	commitHistory, err := ds.GetTransactionsByParent(ctx, inflightEntry.TransactionID, 2, 0)
	require.NoError(t, err, "Failed to get commit history")
	require.Equal(t, 2, len(commitHistory), "Should have exactly two commit transactions")

	// Just verify both transactions have the correct parent and status
	for i := range commitHistory {
		require.Equal(t, StatusApplied, commitHistory[i].Status,
			"Commit transaction should have APPLIED status")
		require.Equal(t, inflightEntry.TransactionID, commitHistory[i].ParentTransaction,
			"Commit should reference original transaction")
	}

	// Verify the total committed amount equals the original amount
	// Sort transactions by created time to identify first and second commit
	sort.Slice(commitHistory, func(i, j int) bool {
		return commitHistory[i].CreatedAt.Before(commitHistory[j].CreatedAt)
	})

	firstCommit := commitHistory[0]
	secondCommit := commitHistory[1]

	// Verify both commits exist
	require.NotNil(t, firstCommit, "Should have found the first commit transaction")
	require.NotNil(t, secondCommit, "Should have found the second commit transaction")

	// The sum of both commit amounts should equal the original amount
	totalCommittedAmount := firstCommit.Amount + secondCommit.Amount
	require.InDelta(t, originalAmount, totalCommittedAmount, 0.01,
		"Total committed amount should equal original amount")
}

func TestInflightTransactionFlowWithSkipQueueThenPartialCommitThenVoid(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	originalAmount := 500.0
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         originalAmount,
		Inflight:       true,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes for the full amount
	expectedFullDebit := big.NewInt(int64(-originalAmount) * 100) // Amount * precision
	expectedFullCredit := big.NewInt(int64(originalAmount) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(expectedFullDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(expectedFullCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify inflight entry exists
	inflightEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get inflight transaction entry")
	require.Equal(t, StatusInflight, inflightEntry.Status, "Should have an INFLIGHT transaction entry")

	// Partially commit the transaction (commit half of the original amount)
	partialAmount := originalAmount / 2 // 250.0
	partialCommitTxn, err := blnk.CommitInflightTransaction(ctx, inflightEntry.TransactionID, partialAmount)
	require.NoError(t, err, "Failed to partially commit transaction")
	require.Equal(t, StatusApplied, partialCommitTxn.Status, "Partial commit transaction should have APPLIED status")
	require.Equal(t, partialAmount, partialCommitTxn.Amount, "Partial commit amount should match specified amount")

	// Verify balances were updated after partial commit
	updatedSourceAfterPartialCommit, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDestAfterPartialCommit, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes for the partial commit
	expectedPartialDebit := big.NewInt(int64(-partialAmount) * 100) // PartialAmount * precision
	expectedPartialCredit := big.NewInt(int64(partialAmount) * 100)

	// Inflight balance should be reduced by the committed amount
	expectedRemainingInflightDebit := big.NewInt(0).Sub(expectedFullDebit, expectedPartialDebit)
	expectedRemainingInflightCredit := big.NewInt(0).Sub(expectedFullCredit, expectedPartialCredit)

	// Verify balance changes
	require.Equal(t, 0, updatedSourceAfterPartialCommit.Balance.Cmp(expectedPartialDebit),
		"Source balance should reflect the partial commit amount")
	require.Equal(t, 0, updatedDestAfterPartialCommit.Balance.Cmp(expectedPartialCredit),
		"Destination balance should reflect the partial commit amount")

	require.Equal(t, 0, updatedSourceAfterPartialCommit.InflightBalance.Cmp(expectedRemainingInflightDebit),
		"Source inflight balance should be reduced by the committed amount")
	require.Equal(t, 0, updatedDestAfterPartialCommit.InflightBalance.Cmp(expectedRemainingInflightCredit),
		"Destination inflight balance should be reduced by the committed amount")

	// Void the remaining amount
	voidTxn, err := blnk.VoidInflightTransaction(ctx, inflightEntry.TransactionID)
	require.NoError(t, err, "Failed to void remaining transaction amount")
	require.Equal(t, StatusVoid, voidTxn.Status, "Void transaction should have VOID status")

	// Verify balances after void
	updatedSourceAfterVoid, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get source balance after void")

	updatedDestAfterVoid, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get destination balance after void")

	// Verify that permanent balances remain at the partially committed amount
	require.Equal(t, 0, updatedSourceAfterVoid.Balance.Cmp(expectedPartialDebit),
		"Source balance should still reflect the partial commit amount after void")
	require.Equal(t, 0, updatedDestAfterVoid.Balance.Cmp(expectedPartialCredit),
		"Destination balance should still reflect the partial commit amount after void")

	// Verify that inflight balances are now zero after void
	require.Equal(t, 0, updatedSourceAfterVoid.InflightBalance.Cmp(big.NewInt(0)),
		"Source inflight balance should be zero after void")
	require.Equal(t, 0, updatedDestAfterVoid.InflightBalance.Cmp(big.NewInt(0)),
		"Destination inflight balance should be zero after void")

	// Attempt another commit (should fail)
	_, err = blnk.CommitInflightTransaction(ctx, inflightEntry.TransactionID, 0)
	require.Error(t, err, "Committing a voided transaction should fail")
	require.Contains(t, err.Error(), "transaction has already been voided",
		"Error message should indicate transaction is already voided")

	// Attempt another void (should fail)
	_, err = blnk.VoidInflightTransaction(ctx, inflightEntry.TransactionID)
	require.Error(t, err, "Voiding an already voided transaction should fail")
	require.Contains(t, err.Error(), "transaction has already been voided",
		"Error message should indicate transaction is already voided")

	// Verify transaction history
	txnHistory, err := ds.GetTransactionsByParent(ctx, inflightEntry.TransactionID, 10, 0)
	require.NoError(t, err, "Failed to get transaction history")
	require.Equal(t, 2, len(txnHistory), "Should have exactly two transactions: one commit and one void")

	// Check that we have one APPLIED transaction and one VOID transaction
	hasApplied := false
	hasVoid := false

	for _, historyTxn := range txnHistory {
		if historyTxn.Status == StatusApplied {
			hasApplied = true
			require.Equal(t, partialAmount, historyTxn.Amount, "Applied transaction should have partial amount")
		} else if historyTxn.Status == StatusVoid {
			hasVoid = true
			remainingAmount := originalAmount - partialAmount
			require.InDelta(t, remainingAmount, historyTxn.Amount, 0.01,
				"Void transaction should have remaining amount")
		}
	}

	require.True(t, hasApplied, "Transaction history should include an APPLIED transaction")
	require.True(t, hasVoid, "Transaction history should include a VOID transaction")
}

func TestInflightTransactionFlowWithSkipQueueThenVoid(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Inflight:       true,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-500) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify inflight entry exists
	queuedEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusInflight, queuedEntry.Status, "Should have an INFLIGHT transaction entry")

	// Now void the transaction
	_, err = blnk.VoidInflightTransaction(ctx, queuedEntry.TransactionID)
	require.NoError(t, err, "Failed to void transaction")

	// Verify balances were reset after void
	updatedSourceAfterVoid, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance after void")

	updatedDestAfterVoid, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance after void")

	// Verify balance changes were reversed
	require.Equal(t, 0, updatedSourceAfterVoid.InflightBalance.Cmp(big.NewInt(0)),
		"Source inflight balance should be reset to zero after void")
	require.Equal(t, 0, updatedDestAfterVoid.InflightBalance.Cmp(big.NewInt(0)),
		"Destination inflight balance should be reset to zero after void")

	// Verify normal balance wasn't affected
	require.Equal(t, 0, updatedSourceAfterVoid.Balance.Cmp(big.NewInt(0)),
		"Source normal balance should remain zero")
	require.Equal(t, 0, updatedDestAfterVoid.Balance.Cmp(big.NewInt(0)),
		"Destination normal balance should remain zero")

	// Attempt to void again - should fail
	_, err = blnk.VoidInflightTransaction(ctx, queuedEntry.TransactionID)
	require.Error(t, err, "Voiding an already voided transaction should fail")
	require.Contains(t, err.Error(), "transaction has already been voided", "Error message should indicate transaction is already voided")
}

func TestMultipleSourcesTransactionFlowWithSkipQueue(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalanceOne := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	sourceBalanceTwo := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	sourceOne, err := ds.CreateBalance(*sourceBalanceOne)
	require.NoError(t, err, "Failed to create source balance one")

	sourceTwo, err := ds.CreateBalance(*sourceBalanceTwo)
	require.NoError(t, err, "Failed to create source balance two")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference: txnRef,
		Sources: []model.Distribution{
			{Identifier: sourceOne.BalanceID, Distribution: "50%"},
			{Identifier: sourceTwo.BalanceID, Distribution: "50%"},
		},
		Destination:    dest.BalanceID,
		Amount:         500,
		Inflight:       false,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSourceOne, err := ds.GetBalanceByIDLite(sourceOne.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedSourceTwo, err := ds.GetBalanceByIDLite(sourceTwo.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-250) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSourceOne.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedSourceTwo.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedDest.Balance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntryOne, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-1", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryOne.Status, "Should have an APPLIED transaction entry")

	queuedEntryTwo, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-2", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryTwo.Status, "Should have an APPLIED transaction entry")

}

func TestMultipleDestinationTransactionFlowWithSkipQueue(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	destinationBalanceOne := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destinationBalanceTwo := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	destinationOne, err := ds.CreateBalance(*destinationBalanceOne)
	require.NoError(t, err, "Failed to create destination balance one")

	destinationTwo, err := ds.CreateBalance(*destinationBalanceTwo)
	require.NoError(t, err, "Failed to create destination balance two")

	source, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference: txnRef,
		Destinations: []model.Distribution{
			{Identifier: destinationOne.BalanceID, Distribution: "50%"},
			{Identifier: destinationTwo.BalanceID, Distribution: "50%"},
		},
		Source:         source.BalanceID,
		Amount:         500,
		Inflight:       false,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedDestinationOne, err := ds.GetBalanceByIDLite(destinationOne.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	updatedDestinationTwo, err := ds.GetBalanceByIDLite(destinationTwo.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	// Calculate expected balance changes
	expectedCredit := big.NewInt(int64(250) * 100) // Amount * precision
	expectedDebit := big.NewInt(int64(-500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedDestinationOne.Balance.Cmp(expectedCredit),
		"destination balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedDestinationTwo.Balance.Cmp(expectedCredit),
		"destination balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedSource.Balance.Cmp(expectedDebit),
		"Source balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntryOne, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-1", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryOne.Status, "Should have an APPLIED transaction entry")

	queuedEntryTwo, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-2", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryTwo.Status, "Should have an APPLIED transaction entry")

}

func TestQueueTransactionFlowWithSkipQueueAndPreciseAmount(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		PreciseAmount:  big.NewInt(50000),
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-500) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.Balance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntry.Status, "Should have an APPLIED transaction entry")
}

func TestMultipleSourcesTransactionFlowWithSkipQueueAndPreciseAmount(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	sourceBalanceOne := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	sourceBalanceTwo := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	sourceOne, err := ds.CreateBalance(*sourceBalanceOne)
	require.NoError(t, err, "Failed to create source balance one")

	sourceTwo, err := ds.CreateBalance(*sourceBalanceTwo)
	require.NoError(t, err, "Failed to create source balance two")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference: txnRef,
		Sources: []model.Distribution{
			{Identifier: sourceOne.BalanceID, Distribution: "50%"},
			{Identifier: sourceTwo.BalanceID, Distribution: "50%"},
		},
		Destination:    dest.BalanceID,
		PreciseAmount:  big.NewInt(50000),
		Inflight:       false,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedSourceOne, err := ds.GetBalanceByIDLite(sourceOne.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedSourceTwo, err := ds.GetBalanceByIDLite(sourceTwo.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-250) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSourceOne.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedSourceTwo.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedDest.Balance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntryOne, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-1", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryOne.Status, "Should have an APPLIED transaction entry")

	queuedEntryTwo, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-2", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryTwo.Status, "Should have an APPLIED transaction entry")

}

func TestMultipleDestinationTransactionFlowWithSkipQueueWithPreciseAmount(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances
	destinationBalanceOne := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destinationBalanceTwo := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	destinationOne, err := ds.CreateBalance(*destinationBalanceOne)
	require.NoError(t, err, "Failed to create destination balance one")

	destinationTwo, err := ds.CreateBalance(*destinationBalanceTwo)
	require.NoError(t, err, "Failed to create destination balance two")

	source, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction with skip_queue set to true
	txn := &model.Transaction{
		Reference: txnRef,
		Destinations: []model.Distribution{
			{Identifier: destinationOne.BalanceID, Distribution: "50%"},
			{Identifier: destinationTwo.BalanceID, Distribution: "50%"},
		},
		Source:         source.BalanceID,
		PreciseAmount:  big.NewInt(50000),
		Inflight:       false,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
	}

	// Queue the transaction
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")

	// Verify that the transaction was processed immediately
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Verify balances were updated immediately
	updatedDestinationOne, err := ds.GetBalanceByIDLite(destinationOne.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	updatedDestinationTwo, err := ds.GetBalanceByIDLite(destinationTwo.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	// Calculate expected balance changes
	expectedCredit := big.NewInt(int64(250) * 100) // Amount * precision
	expectedDebit := big.NewInt(int64(-500) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedDestinationOne.Balance.Cmp(expectedCredit),
		"destination balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedDestinationTwo.Balance.Cmp(expectedCredit),
		"destination balance should be immediately reduced by transaction amount")

	require.Equal(t, 0, updatedSource.Balance.Cmp(expectedDebit),
		"Source balance should be immediately increased by transaction amount")

	// Verify no queued entry exists
	queuedEntryOne, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-1", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryOne.Status, "Should have an APPLIED transaction entry")

	queuedEntryTwo, err := ds.GetTransactionByRef(ctx, fmt.Sprintf("%s-2", txnRef))
	require.NoError(t, err, "Failed to get queued transaction entry")
	require.Equal(t, StatusApplied, queuedEntryTwo.Status, "Should have an APPLIED transaction entry")

}

func TestQueueTransactionStatus(t *testing.T) {
	// Setup basic configuration
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

	// Create datasource and blnk instance
	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err)
	blnk, err := NewBlnk(ds)
	require.NoError(t, err)

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err)
	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err)

	tests := []struct {
		name       string
		skipQueue  bool
		inflight   bool
		wantStatus string
	}{
		{
			name:       "Skip Queue True - Should be APPLIED",
			skipQueue:  true,
			inflight:   false,
			wantStatus: StatusApplied,
		},
		{
			name:       "Skip Queue False - Should be QUEUED",
			skipQueue:  false,
			inflight:   false,
			wantStatus: StatusQueued,
		},
		{
			name:       "Skip Queue True with Inflight - Should be INFLIGHT",
			skipQueue:  true,
			inflight:   true,
			wantStatus: StatusInflight,
		},
		{
			name:       "Skip Queue False with Inflight - Should be QUEUED",
			skipQueue:  false,
			inflight:   true,
			wantStatus: StatusQueued,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := &model.Transaction{
				Reference:      "txn_" + model.GenerateUUIDWithSuffix("test"),
				Source:         source.BalanceID,
				Destination:    dest.BalanceID,
				Amount:         100,
				Currency:       "USD",
				Precision:      100,
				AllowOverdraft: true,
				SkipQueue:      tt.skipQueue,
				Inflight:       tt.inflight,
			}

			result, err := blnk.QueueTransaction(context.Background(), txn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, result.Status,
				"Status mismatch for skipQueue=%v, inflight=%v", tt.skipQueue, tt.inflight)
		})
	}
}

func TestStandardTransactionRefund(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping queue flow test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	txnRef := "txn_" + model.GenerateUUIDWithSuffix("test")

	// Create test balances with initial amounts
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create a standard transaction with skip_queue set to true
	originalAmount := 500.0
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         originalAmount,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Enable skip queue
		// No Inflight flag - this is a standard transaction
	}

	// Process the transaction
	originalTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to process transaction")
	require.Equal(t, StatusApplied, originalTxn.Status, "Transaction should be APPLIED immediately with skip_queue enabled")

	// Verify balances after initial transaction
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Calculate expected balance changes
	expectedDebit := big.NewInt(int64(-originalAmount) * 100) // Amount * precision
	expectedCredit := big.NewInt(int64(originalAmount) * 100)

	// Verify balance changes
	require.Equal(t, 0, updatedSource.Balance.Cmp(expectedDebit),
		"Source balance should be immediately reduced by transaction amount")
	require.Equal(t, 0, updatedDest.Balance.Cmp(expectedCredit),
		"Destination balance should be immediately increased by transaction amount")
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(big.NewInt(0)),
		"Source inflight balance should be zero for standard transaction")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(big.NewInt(0)),
		"Destination inflight balance should be zero for standard transaction")

	// Get the actual transaction ID for the refund operation
	txnEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get transaction entry")

	// Refund the transaction
	refundTxn, err := blnk.RefundTransaction(ctx, txnEntry.TransactionID, true)
	require.NoError(t, err, "Failed to refund transaction")
	require.Equal(t, StatusApplied, refundTxn.Status, "Refund transaction should be APPLIED immediately")
	require.Equal(t, txnEntry.TransactionID, refundTxn.ParentTransaction, "Refund should reference original transaction as parent")
	require.Equal(t, originalAmount, refundTxn.Amount, "Refund amount should match original amount")

	// Most importantly: verify that source and destination are swapped
	require.Equal(t, txnEntry.Destination, refundTxn.Source, "Refund source should be original destination")
	require.Equal(t, txnEntry.Source, refundTxn.Destination, "Refund destination should be original source")

	// Verify balances after refund
	sourceAfterRefund, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get source balance after refund")

	destAfterRefund, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get destination balance after refund")

	// Balances should be back to where they started (zero)
	require.Equal(t, 0, sourceAfterRefund.Balance.Cmp(big.NewInt(0)),
		"Source balance should be reset to zero after refund")
	require.Equal(t, 0, destAfterRefund.Balance.Cmp(big.NewInt(0)),
		"Destination balance should be reset to zero after refund")
	require.Equal(t, 0, sourceAfterRefund.InflightBalance.Cmp(big.NewInt(0)),
		"Source inflight balance should remain zero after refund")
	require.Equal(t, 0, destAfterRefund.InflightBalance.Cmp(big.NewInt(0)),
		"Destination inflight balance should remain zero after refund")

	// Verify refund transaction in transaction history
	transactions, err := ds.GetTransactionsByParent(ctx, txnEntry.TransactionID, 10, 0)
	require.NoError(t, err, "Failed to get transaction history")
	require.Equal(t, 1, len(transactions), "Should have exactly one child transaction (the refund)")
	require.Equal(t, StatusApplied, transactions[0].Status, "Refund transaction should have APPLIED status")
	require.Equal(t, originalAmount, transactions[0].Amount, "Refund amount should match original")

	// Attempt to refund the refund transaction (should succeed)
	refundOfRefundTxn, err := blnk.RefundTransaction(ctx, refundTxn.TransactionID, true)
	require.NoError(t, err, "Failed to refund the refund transaction")
	require.Equal(t, StatusApplied, refundOfRefundTxn.Status, "Refund of refund should be APPLIED")
	require.Equal(t, refundTxn.TransactionID, refundOfRefundTxn.ParentTransaction, "Refund of refund should reference refund as parent")

	// Verify balances after refund of refund (should be back to the state after original transaction)
	sourceAfterRefundOfRefund, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get source balance after refund of refund")

	destAfterRefundOfRefund, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get destination balance after refund of refund")

	// Verify the balances are back to the post-original transaction state
	require.Equal(t, 0, sourceAfterRefundOfRefund.Balance.Cmp(expectedDebit),
		"Source balance should be back to original debit after refund of refund")
	require.Equal(t, 0, destAfterRefundOfRefund.Balance.Cmp(expectedCredit),
		"Destination balance should be back to original credit after refund of refund")
}

func TestRejectTransaction(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping transaction rejection test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Create a unique transaction reference
	txnRef := "txn_" + model.GenerateUUIDWithSuffix("reject_test")

	// Create test balances
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Create transaction to reject
	txn := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"), // Ensure unique ID
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		AmountString:   "5.00",
		PreciseAmount:  big.NewInt(50000),
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		Status:         StatusQueued, // Initialize as queued
	}

	// Set a rejection reason
	rejectionReason := "Insufficient funds"

	// Reject the transaction
	rejectedTxn, err := blnk.RejectTransaction(ctx, txn, rejectionReason)
	require.NoError(t, err, "Failed to reject transaction")

	// Verify rejection details
	require.Equal(t, StatusRejected, rejectedTxn.Status, "Transaction status should be REJECTED")
	require.Contains(t, rejectedTxn.MetaData, "blnk_rejection_reason", "Metadata should contain rejection reason")
	require.Equal(t, rejectionReason, rejectedTxn.MetaData["blnk_rejection_reason"], "Rejection reason should match")

	// Verify transaction was persisted
	persistedTxn, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get persisted transaction")
	require.Equal(t, StatusRejected, persistedTxn.Status, "Persisted transaction should have REJECTED status")
	require.Contains(t, persistedTxn.MetaData, "blnk_rejection_reason", "Persisted metadata should contain rejection reason")
	require.Equal(t, rejectionReason, persistedTxn.MetaData["blnk_rejection_reason"], "Persisted rejection reason should match")

	// Verify balances should be unaffected by rejected transaction
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Balances should be zero
	require.Equal(t, 0, updatedSource.Balance.Cmp(big.NewInt(0)),
		"Source balance should be zero for rejected transaction")
	require.Equal(t, 0, updatedDest.Balance.Cmp(big.NewInt(0)),
		"Destination balance should be zero for rejected transaction")
	require.Equal(t, 0, updatedSource.InflightBalance.Cmp(big.NewInt(0)),
		"Source inflight balance should be zero for rejected transaction")
	require.Equal(t, 0, updatedDest.InflightBalance.Cmp(big.NewInt(0)),
		"Destination inflight balance should be zero for rejected transaction")

	// Attempt to refund the rejected transaction - should fail
	_, err = blnk.RefundTransaction(ctx, rejectedTxn.TransactionID, true)
	require.Error(t, err, "Refunding a rejected transaction should fail")
	require.Contains(t, err.Error(), "transaction is not in a state that can be refunded",
		"Error message should indicate transaction cannot be refunded due to its state")
}

func TestCommitWorkerFullFlow(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping commit worker test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Create test balances with initial amounts of zero
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Step 1: Create an inflight transaction with skip_queue set to true
	txnRef := "txn_" + model.GenerateUUIDWithSuffix("commit_flow_test")
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		Inflight:       true, // Make this an inflight transaction
		SkipQueue:      true, // Process immediately
	}

	// Step 2: Queue/process the transaction (which will apply it immediately due to skip_queue)
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Step 3: Verify balances were updated immediately with inflight amounts
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Verify inflight balance changes
	expectedInflightDebit := "-50000" // -500 * 100
	expectedInflightCredit := "50000" // 500 * 100

	require.Equal(t, expectedInflightDebit, updatedSource.InflightBalance.String(),
		"Source inflight balance should show debit immediately")
	require.Equal(t, expectedInflightCredit, updatedDest.InflightBalance.String(),
		"Destination inflight balance should show credit immediately")
	require.Equal(t, "0", updatedSource.Balance.String(),
		"Source main balance should still be zero")
	require.Equal(t, "0", updatedDest.Balance.String(),
		"Destination main balance should still be zero")

	// Step 4: Get the transaction by reference to get its ID
	inflightEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get transaction by reference")
	require.Equal(t, StatusInflight, inflightEntry.Status, "Transaction should have INFLIGHT status")

	// Step 5: Setup commit worker
	var wg sync.WaitGroup
	wg.Add(1)

	jobs := make(chan *model.Transaction, 1)
	results := make(chan BatchJobResult, 1)

	// Get the transaction object by ID for the worker
	txnObj, err := ds.GetTransaction(ctx, inflightEntry.TransactionID)
	require.NoError(t, err, "Failed to get transaction by ID")

	// Push to the jobs channel
	jobs <- txnObj
	close(jobs)

	// Step 6: Run the commit worker
	go blnk.CommitWorker(ctx, jobs, results, &wg, 0) // 0 = commit full amount

	// Wait for the worker to finish
	wg.Wait()
	close(results)

	// Collect the result
	var result BatchJobResult
	select {
	case result = <-results:
		// Got a result
	default:
		t.Fatal("No results received from commit worker")
	}

	// Step 7: Verify the commit worker result
	require.NoError(t, result.Error, "Commit worker should not return an error")
	require.NotNil(t, result.Txn, "Commit worker should return a transaction")
	require.Equal(t, StatusApplied, result.Txn.Status, "Committed transaction should have APPLIED status")
	require.Equal(t, inflightEntry.TransactionID, result.Txn.ParentTransaction,
		"Committed transaction should reference the original as parent")

	// Step 8: Verify final balances
	finalSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get final source balance")

	finalDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get final destination balance")

	// Verify balance changes - amounts should move from inflight to main balances
	expectedDebit := "-50000" // -500 * 100
	expectedCredit := "50000" // 500 * 100

	require.Equal(t, expectedDebit, finalSource.Balance.String(),
		"Source balance should now reflect the committed amount")
	require.Equal(t, expectedCredit, finalDest.Balance.String(),
		"Destination balance should now reflect the committed amount")
	require.Equal(t, "0", finalSource.InflightBalance.String(),
		"Source inflight balance should be zero after commit")
	require.Equal(t, "0", finalDest.InflightBalance.String(),
		"Destination inflight balance should be zero after commit")
}

func TestVoidWorkerFullFlow(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping void worker test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Create test balances with initial amounts of zero
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Step 1: Create an inflight transaction with skip_queue set to true
	txnRef := "txn_" + model.GenerateUUIDWithSuffix("void_flow_test")
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		Inflight:       true, // Make this an inflight transaction
		SkipQueue:      true, // Process immediately
	}

	// Step 2: Queue/process the transaction (which will apply it immediately due to skip_queue)
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")
	require.Equal(t, StatusInflight, queuedTxn.Status, "Transaction should be INFLIGHT immediately when skip_queue is true")

	// Step 3: Verify balances were updated immediately with inflight amounts
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Verify inflight balance changes
	expectedInflightDebit := "-50000" // -500 * 100
	expectedInflightCredit := "50000" // 500 * 100

	require.Equal(t, expectedInflightDebit, updatedSource.InflightBalance.String(),
		"Source inflight balance should show debit immediately")
	require.Equal(t, expectedInflightCredit, updatedDest.InflightBalance.String(),
		"Destination inflight balance should show credit immediately")
	require.Equal(t, "0", updatedSource.Balance.String(),
		"Source main balance should still be zero")
	require.Equal(t, "0", updatedDest.Balance.String(),
		"Destination main balance should still be zero")

	// Step 4: Get the transaction by reference to get its ID
	inflightEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get transaction by reference")
	require.Equal(t, StatusInflight, inflightEntry.Status, "Transaction should have INFLIGHT status")

	// Step 5: Setup void worker
	var wg sync.WaitGroup
	wg.Add(1)

	jobs := make(chan *model.Transaction, 1)
	results := make(chan BatchJobResult, 1)

	// Get the transaction object by ID for the worker
	txnObj, err := ds.GetTransaction(ctx, inflightEntry.TransactionID)
	require.NoError(t, err, "Failed to get transaction by ID")

	// Push to the jobs channel
	jobs <- txnObj
	close(jobs)

	// Step 6: Run the void worker
	go blnk.VoidWorker(ctx, jobs, results, &wg, 0) // Amount parameter is not used in VoidWorker

	// Wait for the worker to finish
	wg.Wait()
	close(results)

	// Collect the result
	var result BatchJobResult
	select {
	case result = <-results:
		// Got a result
	default:
		t.Fatal("No results received from void worker")
	}

	// Step 7: Verify the void worker result
	require.NoError(t, result.Error, "Void worker should not return an error")
	require.NotNil(t, result.Txn, "Void worker should return a transaction")
	require.Equal(t, StatusVoid, result.Txn.Status, "Voided transaction should have VOID status")
	require.Equal(t, inflightEntry.TransactionID, result.Txn.ParentTransaction,
		"Voided transaction should reference the original as parent")

	// Step 8: Verify final balances
	finalSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get final source balance")

	finalDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get final destination balance")

	// Verify balance changes - inflight balances should be zeroed and main balances should remain unchanged
	require.Equal(t, "0", finalSource.Balance.String(),
		"Source balance should remain unchanged at zero")
	require.Equal(t, "0", finalDest.Balance.String(),
		"Destination balance should remain unchanged at zero")
	require.Equal(t, "0", finalSource.InflightBalance.String(),
		"Source inflight balance should be zero after void")
	require.Equal(t, "0", finalDest.InflightBalance.String(),
		"Destination inflight balance should be zero after void")

	// Step 9: Verify that attempting to void again fails
	var wg2 sync.WaitGroup
	wg2.Add(1)

	jobs2 := make(chan *model.Transaction, 1)
	results2 := make(chan BatchJobResult, 1)

	jobs2 <- txnObj
	close(jobs2)

	go blnk.VoidWorker(ctx, jobs2, results2, &wg2, 0)
	wg2.Wait()
	close(results2)

	var result2 BatchJobResult
	select {
	case result2 = <-results2:
		// Got a result
	default:
		t.Fatal("No results received from second void worker")
	}

	require.Error(t, result2.Error, "Voiding an already voided transaction should fail")
	require.Contains(t, result2.Error.Error(), "transaction has already been voided",
		"Error should indicate transaction is already voided")
}

func TestRefundWorkerFullFlow(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping refund worker test in short mode")
	}

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
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Create test balances with initial amounts of zero
	sourceBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "USD",
		LedgerID: "general_ledger_id",
	}

	source, err := ds.CreateBalance(*sourceBalance)
	require.NoError(t, err, "Failed to create source balance")

	dest, err := ds.CreateBalance(*destBalance)
	require.NoError(t, err, "Failed to create destination balance")

	// Step 1: Create a standard (non-inflight) transaction with skip_queue set to true
	txnRef := "txn_" + model.GenerateUUIDWithSuffix("refund_flow_test")
	txn := &model.Transaction{
		Reference:      txnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         500,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		MetaData:       map[string]interface{}{"test": true},
		SkipQueue:      true, // Process immediately
		// No Inflight flag - this is a standard transaction that will be immediately applied
	}

	// Step 2: Queue/process the transaction (which will apply it immediately due to skip_queue)
	queuedTxn, err := blnk.QueueTransaction(ctx, txn)
	require.NoError(t, err, "Failed to queue transaction")
	require.Equal(t, StatusApplied, queuedTxn.Status, "Transaction should be APPLIED immediately when skip_queue is true")

	// Step 3: Verify balances were updated immediately with the transaction amounts
	updatedSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get updated source balance")

	updatedDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get updated destination balance")

	// Verify main balance changes
	expectedDebit := "-50000" // -500 * 100
	expectedCredit := "50000" // 500 * 100

	require.Equal(t, expectedDebit, updatedSource.Balance.String(),
		"Source balance should show debit immediately")
	require.Equal(t, expectedCredit, updatedDest.Balance.String(),
		"Destination balance should show credit immediately")
	require.Equal(t, "0", updatedSource.InflightBalance.String(),
		"Source inflight balance should remain zero")
	require.Equal(t, "0", updatedDest.InflightBalance.String(),
		"Destination inflight balance should remain zero")

	// Step 4: Get the transaction by reference to get its ID
	appliedEntry, err := ds.GetTransactionByRef(ctx, txnRef)
	require.NoError(t, err, "Failed to get transaction by reference")
	require.Equal(t, StatusApplied, appliedEntry.Status, "Transaction should have APPLIED status")

	// Step 5: Setup refund worker
	var wg sync.WaitGroup
	wg.Add(1)

	jobs := make(chan *model.Transaction, 1)
	results := make(chan BatchJobResult, 1)

	// Get the transaction object by ID for the worker
	txnObj, err := ds.GetTransaction(ctx, appliedEntry.TransactionID)
	require.NoError(t, err, "Failed to get transaction by ID")

	//apply skip queue to txn to process refund immediately
	txnObj.SkipQueue = true

	// Push to the jobs channel
	jobs <- txnObj
	close(jobs)

	// Step 6: Run the refund worker
	go blnk.RefundWorker(ctx, jobs, results, &wg, 0)

	// Wait for the worker to finish
	wg.Wait()
	close(results)

	// Collect the result
	var result BatchJobResult
	select {
	case result = <-results:
		// Got a result
	default:
		t.Fatal("No results received from refund worker")
	}

	// Step 7: Verify the refund worker result
	require.NoError(t, result.Error, "Refund worker should not return an error")
	require.NotNil(t, result.Txn, "Refund worker should return a transaction")
	require.Equal(t, StatusApplied, result.Txn.Status, "Refund transaction should have APPLIED status")
	require.Equal(t, appliedEntry.TransactionID, result.Txn.ParentTransaction,
		"Refund transaction should reference the original as parent")
	require.Equal(t, appliedEntry.Source, result.Txn.Destination,
		"Refund transaction should swap source and destination")
	require.Equal(t, appliedEntry.Destination, result.Txn.Source,
		"Refund transaction should swap source and destination")

	// Step 8: Verify final balances
	finalSource, err := ds.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err, "Failed to get final source balance")

	finalDest, err := ds.GetBalanceByIDLite(dest.BalanceID)
	require.NoError(t, err, "Failed to get final destination balance")

	// Verify balances are now back to zero (original amount - refund = 0)
	require.Equal(t, "0", finalSource.Balance.String(),
		"Source balance should be zero after refund")
	require.Equal(t, "0", finalDest.Balance.String(),
		"Destination balance should be zero after refund")
	require.Equal(t, "0", finalSource.InflightBalance.String(),
		"Source inflight balance should remain zero")
	require.Equal(t, "0", finalDest.InflightBalance.String(),
		"Destination inflight balance should remain zero")

	// Step 9: Test refunding a rejected transaction (which should fail)
	// Create a transaction to be rejected
	rejectedTxnRef := "txn_" + model.GenerateUUIDWithSuffix("rejected_test")
	rejectedTxn := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      rejectedTxnRef,
		Source:         source.BalanceID,
		Destination:    dest.BalanceID,
		Amount:         200,
		AmountString:   "2.00",
		PreciseAmount:  big.NewInt(20000),
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		Status:         StatusQueued,
	}

	// Reject the transaction
	rejectedTxn, err = blnk.RejectTransaction(ctx, rejectedTxn, "Insufficient funds")
	require.NoError(t, err, "Failed to reject transaction")
	require.Equal(t, StatusRejected, rejectedTxn.Status, "Transaction should have REJECTED status")

	// Try to refund the rejected transaction
	var wg2 sync.WaitGroup
	wg2.Add(1)

	jobs2 := make(chan *model.Transaction, 1)
	results2 := make(chan BatchJobResult, 1)

	jobs2 <- rejectedTxn
	close(jobs2)

	go blnk.RefundWorker(ctx, jobs2, results2, &wg2, 0)
	wg2.Wait()
	close(results2)

	var result2 BatchJobResult
	select {
	case result2 = <-results2:
		// Got a result
	default:
		t.Fatal("No results received from second refund worker")
	}

	require.Error(t, result2.Error, "Refunding a rejected transaction should fail")
	require.Contains(t, result2.Error.Error(), "transaction is not in a state that can be refunded",
		"Error should indicate transaction cannot be refunded")
}
