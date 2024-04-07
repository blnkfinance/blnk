package blnk

import (
	"context"
	"fmt"
	"regexp"
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

	source := gofakeit.UUID()
	destination := gofakeit.UUID()

	fmt.Println(source, destination)
	txn := &model.Transaction{
		Reference:   gofakeit.UUID(),
		Source:      source,
		Destination: destination,
		Amount:      10,
		Currency:    "NGN",
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "created_at"}).
		AddRow(source, "NGN", 1, "ledger-id-source", 100, 50, 50, time.Now())

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "created_at"}).
		AddRow(destination, "NGN", 1, "ledger-id-destination", 200, 100, 100, time.Now())

	balanceQuery := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, created_at FROM blnk.balances WHERE balance_id = $1`)
	balanceQuery2 := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, created_at FROM blnk.balances WHERE balance_id = $1`)

	mock.ExpectQuery(balanceQuery).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQuery2).WithArgs(destination).WillReturnRows(destinationBalanceRows)

	expectedSQL := `INSERT INTO blnk.transactions(transaction_id,source,reference,amount,currency,destination,description,status,created_at,meta_data,scheduled_for,hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`

	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).WithArgs(
		sqlmock.AnyArg(),
		source,
		txn.Reference,
		txn.Amount,
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

func TestApplyBalanceToQueuedTransaction(t *testing.T) {
	// Test setup
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	source := gofakeit.UUID()
	destination := gofakeit.UUID()

	fmt.Println(source, destination)
	txn := &model.Transaction{
		Reference:   gofakeit.UUID(),
		Source:      source,
		Destination: destination,
		Amount:      10,
		Currency:    "NGN",
	}

	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "created_at"}).
		AddRow(source, "NGN", 1, "ledger-id-source", 100, 50, 50, time.Now())

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "created_at"}).
		AddRow(destination, "NGN", 1, "ledger-id-destination", 200, 100, 100, time.Now())

	balanceQuery := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, created_at FROM blnk.balances WHERE balance_id = $1`)
	balanceQuery2 := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, created_at FROM blnk.balances WHERE balance_id = $1`)

	mock.ExpectQuery(balanceQuery).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQuery2).WithArgs(destination).WillReturnRows(destinationBalanceRows)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT transaction_id, source, reference, amount, currency,destination, description, status,created_at, meta_data
		FROM blnk.transactions
		WHERE reference = $1	
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{""}))

	_, err = d.RecordTransaction(context.Background(), txn)
	assert.NoError(t, err)

	// Check if all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
