package blnk

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func getBalanceMock(credit, debit, balance int64) *model.Balance {
	return &model.Balance{CreditBalance: credit, DebitBalance: debit, Balance: balance}
}

func getTransactionMock(amount float64, overdraft bool) model.Transaction {
	transaction := model.Transaction{TransactionID: gofakeit.UUID(), Amount: amount, AllowOverdraft: overdraft, Reference: gofakeit.UUID()}
	return transaction
}

func TestUpdateBalancesWithTransaction(t *testing.T) {
	tests := []struct {
		name               string
		sourceBalance      *model.Balance
		ExpectedError      error
		destinationBalance *model.Balance
		transaction        model.Transaction
		want               struct {
			SourceBalance            int64
			SourceCreditBalance      int64
			SourceDebitBalance       int64
			DestinationBalance       int64
			DestinationCreditBalance int64
			DestinationDebitBalance  int64
		}
	}{{
		name:               "Send 1k from destination to source balance.",
		ExpectedError:      nil,
		sourceBalance:      getBalanceMock(2500, 0, 2500),
		destinationBalance: getBalanceMock(0, 0, 0),
		transaction:        getTransactionMock(1000, false),
		want: struct {
			SourceBalance            int64
			SourceCreditBalance      int64
			SourceDebitBalance       int64
			DestinationBalance       int64
			DestinationCreditBalance int64
			DestinationDebitBalance  int64
		}{SourceBalance: 1500, SourceDebitBalance: 1000, SourceCreditBalance: 2500, DestinationBalance: 1000, DestinationDebitBalance: 0, DestinationCreditBalance: 1000},
	},
		{
			name:               "Debit 900m from source with start balance of 2.5b and debit balance of 500m. And destination start balance of 5k",
			ExpectedError:      nil,
			sourceBalance:      getBalanceMock(2500000000, 500000000, 2500000000),
			destinationBalance: getBalanceMock(5000, 0, 5000),
			transaction:        getTransactionMock(900000000, false),
			want: struct {
				SourceBalance            int64
				SourceCreditBalance      int64
				SourceDebitBalance       int64
				DestinationBalance       int64
				DestinationCreditBalance int64
				DestinationDebitBalance  int64
			}{SourceBalance: 1100000000, SourceDebitBalance: 1400000000, SourceCreditBalance: 2500000000, DestinationBalance: 900005000, DestinationDebitBalance: 0, DestinationCreditBalance: 900005000},
		},
		{
			name:               "Debit 1K from source balance with overdraft on. expect source balance to be -1k.",
			ExpectedError:      nil,
			sourceBalance:      getBalanceMock(0, 0, 0),
			destinationBalance: getBalanceMock(0, 0, 0),
			transaction:        getTransactionMock(1000, true),
			want: struct {
				SourceBalance            int64
				SourceCreditBalance      int64
				SourceDebitBalance       int64
				DestinationBalance       int64
				DestinationCreditBalance int64
				DestinationDebitBalance  int64
			}{SourceBalance: -1000, SourceDebitBalance: 1000, SourceCreditBalance: 0, DestinationBalance: 1000, DestinationDebitBalance: 0, DestinationCreditBalance: 1000},
		},
		{
			name:               "Debit 1K from source balance with overdraft off. expect source balance to be 0, and error returned.",
			ExpectedError:      fmt.Errorf("insufficient funds in source balance"),
			sourceBalance:      getBalanceMock(0, 0, 0),
			destinationBalance: getBalanceMock(0, 0, 0),
			transaction:        getTransactionMock(1000, false),
			want: struct {
				SourceBalance            int64
				SourceCreditBalance      int64
				SourceDebitBalance       int64
				DestinationBalance       int64
				DestinationCreditBalance int64
				DestinationDebitBalance  int64
			}{SourceBalance: 0, SourceDebitBalance: 0, SourceCreditBalance: 0, DestinationBalance: 0, DestinationDebitBalance: 0, DestinationCreditBalance: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transaction := tt.transaction
			err := model.UpdateBalances(&transaction, tt.sourceBalance, tt.destinationBalance)
			assert.Equal(t, tt.ExpectedError, err)
			assert.Equal(t, tt.want.SourceBalance, tt.sourceBalance.Balance, "expected source balances to match")
			assert.Equal(t, tt.want.SourceDebitBalance, tt.sourceBalance.DebitBalance, "expected source debit balances to match")
			assert.Equal(t, tt.want.SourceCreditBalance, tt.sourceBalance.CreditBalance, "expected source credit balances to match")
			assert.Equal(t, tt.want.DestinationBalance, tt.destinationBalance.Balance, "expected destination balances to match")
			assert.Equal(t, tt.want.DestinationDebitBalance, tt.destinationBalance.DebitBalance, "expected destination debit balances to match")
			assert.Equal(t, tt.want.DestinationCreditBalance, tt.destinationBalance.CreditBalance, "expected destination credit balances to match")
		})
	}

}

func TestCreateBalance(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	balance := model.Balance{Balance: 100, CreditBalance: 50, DebitBalance: 50, Currency: "USD", LedgerID: "test-id", IdentityID: "test-id"}

	// Convert metadata to JSON for mocking
	metaDataJSON, _ := json.Marshal(balance.MetaData)
	mock.ExpectExec("INSERT INTO blnk.balances").
		WithArgs(sqlmock.AnyArg(), balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.IdentityID, sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateBalance(balance)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.BalanceID)
	assert.Contains(t, result.BalanceID, "bln_")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetBalanceByID(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	balanceID := gofakeit.UUID()

	mock.ExpectBegin()

	// Adjust the expected SQL to match the actual query structure and fields
	expectedSQL := "SELECT b\\.balance_id, b\\.balance, b\\.credit_balance, b\\.debit_balance, b\\.currency, b\\.currency_multiplier, b\\.ledger_id, COALESCE\\(b\\.identity_id, ''\\) as identity_id, b\\.created_at, b\\.meta_data, b\\.inflight_balance, b\\.inflight_credit_balance, b\\.inflight_debit_balance, b\\.version FROM \\( SELECT \\* FROM blnk\\.balances WHERE balance_id = \\$1 \\) AS b"
	rows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "version"}).
		AddRow(balanceID, 100, 50, 50, "USD", 100, "test-ledger", "test-identity", time.Now(), `{"key":"value"}`, 0, 0, 0, 0)

	mock.ExpectQuery(expectedSQL).
		WithArgs(balanceID).
		WillReturnRows(rows)

	mock.ExpectCommit()

	result, err := d.GetBalanceByID(balanceID, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, balanceID, result.BalanceID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAllBalances(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	rows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "created_at", "meta_data"}).
		AddRow("test-balance", 100, 50, 50, "USD", 1.0, "test-ledger", time.Now(), `{"key":"value"}`)

	mock.ExpectQuery("SELECT balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data FROM blnk.balances LIMIT 20").WillReturnRows(rows)

	result, err := d.GetAllBalances()

	assert.NoError(t, err)
	assert.Len(t, result, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateBalance(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	balance := &model.Balance{
		BalanceID:          "test-balance",
		Balance:            100,
		CreditBalance:      50,
		DebitBalance:       50,
		Currency:           "USD",
		CurrencyMultiplier: 1,
		LedgerID:           gofakeit.UUID(),
		CreatedAt:          time.Time{},              // Assuming CreatedAt is properly initialized elsewhere
		MetaData:           map[string]interface{}{}, // Assuming MetaData is initialized, even if empty
	}

	// Assuming metaDataJSON is what you expect to be passed as the last argument
	metaDataJSON, _ := json.Marshal(balance.MetaData)

	mock.ExpectExec(regexp.QuoteMeta(`
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
        WHERE balance_id = $1`)).WithArgs(balance.BalanceID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON).WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.datasource.UpdateBalance(balance)
	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateBalances(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	sourceBalance := &model.Balance{BalanceID: "source-balance", Balance: 100, CreditBalance: 50, DebitBalance: 50, Currency: "USD"}
	destinationBalance := &model.Balance{BalanceID: "destination-balance", Balance: 150, CreditBalance: 75, DebitBalance: 75, Currency: "USD"}

	// Expect transaction begin
	mock.ExpectBegin()

	// Expect the first update (for sourceBalance)
	mock.ExpectExec("UPDATE blnk.balances").WithArgs(
		sourceBalance.BalanceID, sourceBalance.Balance, sourceBalance.CreditBalance, sourceBalance.DebitBalance, sourceBalance.InflightBalance, sourceBalance.InflightCreditBalance, sourceBalance.InflightDebitBalance, sourceBalance.Currency, sourceBalance.CurrencyMultiplier, sourceBalance.LedgerID, sourceBalance.CreatedAt, sqlmock.AnyArg(), sourceBalance.Version,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect the second update (for destinationBalance)
	mock.ExpectExec("UPDATE blnk.balances").WithArgs(
		destinationBalance.BalanceID, destinationBalance.Balance, destinationBalance.CreditBalance, destinationBalance.DebitBalance, destinationBalance.InflightBalance, destinationBalance.InflightCreditBalance, destinationBalance.InflightDebitBalance, destinationBalance.Currency, destinationBalance.CurrencyMultiplier, destinationBalance.LedgerID, destinationBalance.CreatedAt, sqlmock.AnyArg(), destinationBalance.Version,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect transaction commit
	mock.ExpectCommit()

	err = d.datasource.UpdateBalances(context.Background(), sourceBalance, destinationBalance)
	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateMonitor(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	monitor := model.BalanceMonitor{BalanceID: "test-balance", Description: "Test Monitor", CallBackURL: gofakeit.URL()}

	mock.ExpectExec("INSERT INTO blnk.balance_monitors").WithArgs(sqlmock.AnyArg(), monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Condition.Precision, monitor.Condition.PreciseValue, monitor.Description, monitor.CallBackURL, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateMonitor(monitor)

	assert.NoError(t, err)
	assert.NotEmpty(t, result.MonitorID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetMonitorByID(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	monitorID := "test-monitor"

	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "precision", "precise_value", "description", "call_back_url", "created_at"}).
		AddRow(monitorID, gofakeit.UUID(), "field", "operator", 1000, 100, 100000, "Test Monitor", gofakeit.URL(), time.Now())

	mock.ExpectQuery("SELECT .* FROM blnk.balance_monitors WHERE monitor_id =").WithArgs(monitorID).WillReturnRows(rows)

	result, err := d.GetMonitorByID(monitorID)

	assert.NoError(t, err)
	assert.Equal(t, monitorID, result.MonitorID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAllMonitors(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at"}).
		AddRow("test-monitor", gofakeit.UUID(), "field", "operator", 100, "Test Monitor", gofakeit.URL(), time.Now())

	mock.ExpectQuery("SELECT .* FROM blnk.balance_monitors").WillReturnRows(rows)

	result, err := d.GetAllMonitors()

	assert.NoError(t, err)
	assert.Len(t, result, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetBalanceMonitors(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	balanceID := gofakeit.UUID()
	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at"}).
		AddRow("test-monitor", balanceID, "field", "operator", 100, "Test Monitor", gofakeit.URL(), time.Now())

	mock.ExpectQuery("SELECT .* FROM blnk.balance_monitors WHERE balance_id =").WithArgs(balanceID).WillReturnRows(rows)

	result, err := d.GetBalanceMonitors(balanceID)

	assert.NoError(t, err)
	assert.Len(t, result, 1)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateMonitor(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	monitor := &model.BalanceMonitor{MonitorID: "test-monitor", BalanceID: "test-balance", Description: "Updated Monitor"}

	mock.ExpectExec("UPDATE blnk.balance_monitors").WithArgs(monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CallBackURL).WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.UpdateMonitor(monitor)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDeleteMonitor(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	monitorID := "test-monitor"

	mock.ExpectExec("DELETE FROM blnk.balance_monitors WHERE monitor_id =").WithArgs(monitorID).WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.DeleteMonitor(monitorID)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
