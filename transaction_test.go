package blnk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/stretchr/testify/assert"
)

func TestRecordTransaction(t *testing.T) {
	// Test setup
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	txn := model.Transaction{
		Reference: gofakeit.UUID(),
		BalanceID: gofakeit.UUID(),
		Amount:    10000,
		Currency:  "NGN",
		DRCR:      "Credit",
	}
	metaDataJSON, _ := json.Marshal(txn.MetaData)

	//expect reference check
	mock.ExpectQuery("SELECT .* FROM transactions WHERE reference = \\$1").
		WithArgs(txn.Reference).
		WillReturnRows(sqlmock.NewRows(nil))

	//expect balance fetch
	// Create a row with expected data
	balanceRows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data"}).
		AddRow(txn.BalanceID, 100, 50, 50, "NGN", 1, "test-ledger", "test-identity", time.Now(), `{"key":"value"}`)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM balances WHERE balance_id =").
		WithArgs(txn.BalanceID).
		WillReturnRows(balanceRows)
	mock.ExpectCommit()

	// Expect the transaction to be recorded in the database
	mock.ExpectExec("INSERT INTO public.transactions").
		WithArgs(
			sqlmock.AnyArg(), txn.Tag, txn.Reference, txn.Amount, txn.Currency, txn.PaymentMethod, txn.Description, txn.DRCR, sqlmock.AnyArg(), sqlmock.AnyArg(), txn.BalanceID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Add expectation for UPDATE balances query
	mock.ExpectExec("UPDATE balances SET .* WHERE balance_id = \\$1").
		WithArgs(
			txn.BalanceID, 10000, 10050, 50, "NGN", 1, "test-ledger", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	recordedTxn, err := d.RecordTransaction(txn)
	assert.NoError(t, err)
	assert.Equal(t, txn.Reference, recordedTxn.Reference)
	assert.Equal(t, STATUS_APPLIED, recordedTxn.Status)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestRecordTransactionWithWrongCurrency(t *testing.T) {
	// Test setup
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	txn := model.Transaction{
		Reference: gofakeit.UUID(),
		BalanceID: gofakeit.UUID(),
		Amount:    10000,
		Currency:  "NGN",
		DRCR:      "Credit",
	}

	//expect reference check
	mock.ExpectQuery("SELECT .* FROM transactions WHERE reference = \\$1").
		WithArgs(txn.Reference).
		WillReturnRows(sqlmock.NewRows(nil))

	//expect balance fetch
	//create new balance row with USD currency
	balanceRows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data"}).
		AddRow(txn.BalanceID, 100, 50, 50, "USD", 1, "test-ledger", "test-identity", time.Now(), `{"key":"value"}`)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM balances WHERE balance_id =").
		WithArgs(txn.BalanceID).
		WillReturnRows(balanceRows)
	mock.ExpectCommit()

	_, err = d.RecordTransaction(txn)
	assert.ErrorContains(t, err, "transaction currency does not match the balance currency. Please ensure they are consistent")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestRecordTransactionWithBalanceMultiplier(t *testing.T) {
	// Test setup
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	txn := model.Transaction{
		Reference: gofakeit.UUID(),
		BalanceID: gofakeit.UUID(),
		Amount:    100,
		Currency:  "NGN",
		DRCR:      "Credit",
	}
	metaDataJSON, _ := json.Marshal(txn.MetaData)

	//expect reference check
	mock.ExpectQuery("SELECT .* FROM transactions WHERE reference = \\$1").
		WithArgs(txn.Reference).
		WillReturnRows(sqlmock.NewRows(nil))

	//expect balance fetch
	// Create a row with expected data
	balanceRows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data"}).
		AddRow(txn.BalanceID, 10000, 5000, 5000, "NGN", 100, "test-ledger", "test-identity", time.Now(), `{"key":"value"}`)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM balances WHERE balance_id =").
		WithArgs(txn.BalanceID).
		WillReturnRows(balanceRows)
	mock.ExpectCommit()

	// Expect the transaction to be recorded in the database
	mock.ExpectExec("INSERT INTO public.transactions").
		WithArgs(
			sqlmock.AnyArg(), txn.Tag, txn.Reference, txn.Amount*100, txn.Currency, txn.PaymentMethod, txn.Description, txn.DRCR, sqlmock.AnyArg(), sqlmock.AnyArg(), txn.BalanceID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Add expectation for UPDATE balances query
	mock.ExpectExec("UPDATE balances SET .* WHERE balance_id = \\$1").
		WithArgs(
			txn.BalanceID, 10000, 15000, 5000, "NGN", 100, "test-ledger", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	recordedTxn, err := d.RecordTransaction(txn)
	assert.NoError(t, err)
	assert.Equal(t, txn.Reference, recordedTxn.Reference)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
