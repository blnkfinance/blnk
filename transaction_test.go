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
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/stretchr/testify/assert"
)

func TestRecordTransaction(t *testing.T) {
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
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
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
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
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
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectQuery(regexp.QuoteMeta(`
    SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
    FROM blnk.balance_monitors WHERE balance_id = $1
`)).WithArgs(source).WillReturnRows(sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}))

	expectedSQL := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`
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
	).WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = d.RecordTransaction(context.Background(), txn)
	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestRecordTransactionWithRate(t *testing.T) {
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

	// Updated regex to be more flexible
	balanceQuery := `SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version FROM blnk.balances WHERE balance_id = \$1`
	balanceQueryPattern := regexp.MustCompile(`\s+`).ReplaceAllString(balanceQuery, `\s*`)

	mock.ExpectQuery(balanceQueryPattern).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQueryPattern).WithArgs(destination).WillReturnRows(destinationBalanceRows)
	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
	`)).WithArgs(
		source,
		big.NewInt(-100000000).String(),
		big.NewInt(0).String(),
		big.NewInt(100000000).String(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		"USD",
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		0,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
	  UPDATE blnk.balances
	  SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
	  WHERE balance_id = $1 AND version = $13
	`)).WithArgs(
		destination,
		big.NewInt(130000000000).String(),
		big.NewInt(130000000000).String(),
		big.NewInt(0).String(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		"NGN",
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

	expectedSQL := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`
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
