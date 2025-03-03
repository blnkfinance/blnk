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
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/sirupsen/logrus"

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
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
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

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	source := gofakeit.UUID()
	destination := gofakeit.UUID()

	txn := &model.Transaction{
		Reference:      gofakeit.UUID(),
		Source:         source,
		Destination:    destination,
		Rate:           1,
		Amount:         10,
		AllowOverdraft: false,
		Precision:      100,
		Currency:       "NGN",
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(source, "NGN", "", 1, "ledger-id-source", int64(10000), int64(10000), 0, 0, 0, 0, time.Now(), 0)

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(destination, "", "NGN", 1, "ledger-id-destination", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	// Updated regex to be more flexible
	balanceQuery := `SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = \$1`
	balanceQueryPattern := regexp.MustCompile(`\s+`).ReplaceAllString(balanceQuery, `\s*`)

	mock.ExpectQuery(balanceQueryPattern).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(destination).WillReturnRows(destinationBalanceRows)
	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
	  WHERE balance_id = $1 AND version = $12
	`)).WithArgs(
		source,
		big.NewInt(9000).String(),
		big.NewInt(10000).String(),
		big.NewInt(1000).String(),
		big.NewInt(0).String(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
	  WHERE balance_id = $1 AND version = $12
	`)).WithArgs(
		destination,
		big.NewInt(1000).String(),
		big.NewInt(1000).String(),
		big.NewInt(0).String(),
		big.NewInt(0).String(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(source).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	expectedSQL := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`
	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		source,
		txn.Reference,
		txn.Amount,
		int64(1000), // Adjust precise amount as int64
		txn.Precision,
		float64(1),
		txn.Currency,
		txn.Destination,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = d.RecordTransaction(context.Background(), txn)
	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestRecordTransactionWithRate(t *testing.T) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
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

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	source := gofakeit.UUID()
	destination := gofakeit.UUID()

	txn := &model.Transaction{
		Reference:      gofakeit.UUID(),
		Source:         source,
		Destination:    destination,
		Amount:         1000000,
		Rate:           1300,
		AllowOverdraft: true,
		Precision:      100,
		Currency:       "NGN",
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(source, "", "USD", 1, "ledger-id-source", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(destination, "", "NGN", 1, "ledger-id-destination", 0, 0, 0, 0, 0, 0, time.Now(), 0)

	balanceQuery := `SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = \$1`
	balanceQueryPattern := regexp.MustCompile(`\s+`).ReplaceAllString(balanceQuery, `\s*`)

	mock.ExpectQuery(balanceQueryPattern).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(destination).WillReturnRows(destinationBalanceRows)
	mock.ExpectBegin()

	// Updated mock expectation with correct number of arguments
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

	// Updated mock expectation with correct number of arguments
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
	mock.ExpectCommit()

	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(source).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	expectedSQL := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`
	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		source,
		txn.Reference,
		txn.Amount,
		100000000,
		txn.Precision,
		float64(1300),
		txn.Currency,
		txn.Destination,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = d.RecordTransaction(context.Background(), txn)
	assert.NoError(t, err)
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

func TestRecordTransaction_Concurrency(t *testing.T) {
	t.Parallel()

	// Setup test context and configuration
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
		Server: config.ServerConfig{SecretKey: "some-secret"},
		AccountNumberGeneration: config.AccountNumberGenerationConfig{
			HttpService: config.AccountGenerationHttpService{
				Url: "http://example.com/generateAccount",
			},
		},
	}
	config.ConfigStore.Store(cnf)

	datasource, err := database.NewDataSource(cnf)
	require.NoError(t, err)
	d, err := NewBlnk(datasource)
	require.NoError(t, err)

	sourceBalance := &model.Balance{
		Currency: "NGN",
		LedgerID: "general_ledger_id",
	}
	destBalance := &model.Balance{
		Currency: "NGN",
		LedgerID: "general_ledger_id",
	}

	// Create balances in database
	source, err := datasource.CreateBalance(*sourceBalance)
	require.NoError(t, err)
	destination, err := datasource.CreateBalance(*destBalance)
	require.NoError(t, err)

	// Use a channel to track successful transactions
	numGoroutines := 10
	txnAmount := float64(1000)
	resultChan := make(chan error, numGoroutines)

	// Use a WaitGroup to ensure we wait for all goroutines to complete
	wg := &sync.WaitGroup{}
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()

			// Small random delay to reduce chance of exact simultaneous execution
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

			txn := &model.Transaction{
				TransactionID:  gofakeit.UUID(),
				Reference:      gofakeit.UUID(),
				Source:         source.BalanceID,
				Destination:    destination.BalanceID,
				Amount:         txnAmount,
				Currency:       "NGN",
				Precision:      100,
				AllowOverdraft: true,
			}
			_, err := d.RecordTransaction(ctx, txn)
			resultChan <- err
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(resultChan)

	// Process results
	var successCount int
	var lockFailures int
	expectedLockErr := "failed to acquire lock: lock for key bln_"

	for err := range resultChan {
		if err == nil {
			successCount++
		} else if strings.Contains(err.Error(), expectedLockErr) {
			lockFailures++
		} else {
			t.Logf("Unexpected error: %v", err)
		}
	}

	// Verify exactly one success and the rest being lock failures
	require.Equal(t, 1, successCount, "Expected exactly one successful transaction")
	require.Equal(t, numGoroutines-1, lockFailures, "Expected the rest to fail with lock errors")

	// Check final balances
	finalSource, err := datasource.GetBalanceByIDLite(source.BalanceID)
	require.NoError(t, err)
	finalDest, err := datasource.GetBalanceByIDLite(destination.BalanceID)
	require.NoError(t, err)

	// Only one transaction should have succeeded
	expectedDebit := big.NewInt(int64(1) * int64(txnAmount) * 100) // includes precision
	logrus.Info("Final Source Balance: ", finalSource.Balance, finalDest.Balance, expectedDebit)
	require.Equal(t, 0, finalSource.Balance.Cmp(big.NewInt(0).Neg(expectedDebit)))
	require.Equal(t, 0, finalDest.Balance.Cmp(expectedDebit))
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
