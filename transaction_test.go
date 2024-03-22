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
		Source:      source,      // Dynamically generated UUID for source
		Destination: destination, // Dynamically generated UUID for destination
		Amount:      10,
		Currency:    "NGN",
	}

	// Adjust your mock expectations to correctly reflect the dynamic nature of Source and Destination
	// Ensure the SELECT query for transaction existence check is correctly mocked
	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `)).WithArgs(txn.Reference).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	// Correctly set up the balance query expectations for both Source and Destination
	// Note: Separate the rows for clarity and correctness
	sourceBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "created_at"}).
		AddRow(source, "NGN", 1, "ledger-id-source", 100, 50, 50, time.Now())

	destinationBalanceRows := sqlmock.NewRows([]string{"balance_id", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "created_at"}).
		AddRow(destination, "NGN", 1, "ledger-id-destination", 200, 100, 100, time.Now())

	balanceQuery := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, created_at FROM blnk.balances WHERE balance_id = $1 FOR UPDATE`)
	balanceQuery2 := regexp.QuoteMeta(`SELECT balance_id, currency, currency_multiplier,ledger_id, balance, credit_balance, debit_balance, created_at FROM blnk.balances WHERE balance_id = $1 FOR UPDATE`)

	// Ensure the mock expectations are correctly set for both the source and destination balances
	mock.ExpectQuery(balanceQuery).WithArgs(source).WillReturnRows(sourceBalanceRows)
	mock.ExpectQuery(balanceQuery2).WithArgs(destination).WillReturnRows(destinationBalanceRows)

	// Correct the expected SQL pattern to match the actual SQL command
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

	// Check if all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
