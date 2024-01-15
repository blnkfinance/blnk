package blnk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func getBalanceMock(credit, debit, balance int64) model.Balance {
	return model.Balance{CreditBalance: credit, DebitBalance: debit, Balance: balance}
}

func getTransactionMock(amount int64, currency, DRCR string) model.Transaction {
	transaction := model.Transaction{Amount: amount, Currency: currency, DRCR: DRCR}
	return transaction
}

func TestUpdateBalancesWithTransaction(t *testing.T) {

	tests := []struct {
		name        string
		balance     model.Balance
		transaction model.Transaction
		want        struct {
			Balance       int64
			CreditBalance int64
			DebitBalance  int64
		}
	}{{
		name:        "Credit 1k with starting balance of 0",
		balance:     getBalanceMock(0, 0, 0),
		transaction: getTransactionMock(1000, "NGN", "Credit"),
		want: struct {
			Balance       int64
			CreditBalance int64
			DebitBalance  int64
		}{Balance: 1000, CreditBalance: 1000, DebitBalance: 0},
	},
		{
			name:        "Credit 2k with starting credit balance of 500",
			balance:     getBalanceMock(500, 0, 0),
			transaction: getTransactionMock(2000, "NGN", "Credit"),
			want: struct {
				Balance       int64
				CreditBalance int64
				DebitBalance  int64
			}{Balance: 2500, CreditBalance: 2500, DebitBalance: 0},
		}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			balance := tt.balance
			transaction := tt.transaction

			err := balance.UpdateBalances(&transaction)
			assert.NoError(t, err)
			assert.Equal(t, tt.want.Balance, balance.Balance)
			assert.Equal(t, tt.want.CreditBalance, balance.CreditBalance)
			assert.Equal(t, tt.want.DebitBalance, balance.DebitBalance)
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
	balance := model.Balance{Balance: 100, CreditBalance: 50, DebitBalance: 50, Currency: "USD"}

	// Convert metadata to JSON for mocking
	metaDataJSON, _ := json.Marshal(balance.MetaData)
	mock.ExpectExec("INSERT INTO balances").
		WithArgs(sqlmock.AnyArg(), balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.IdentityID, sqlmock.AnyArg(), metaDataJSON).
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
	balanceID := "test-balance"

	// Expect transaction to begin
	mock.ExpectBegin()

	// Create a row with expected data
	rows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data"}).
		AddRow(balanceID, 100, 50, 50, "USD", 100, "test-ledger", "test-identity", time.Now(), `{"key":"value"}`)

	mock.ExpectQuery("SELECT .* FROM balances WHERE balance_id =").
		WithArgs(balanceID).
		WillReturnRows(rows)

	// Expect transaction to commit
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

	mock.ExpectQuery("SELECT .* FROM balances").WillReturnRows(rows)

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
	balance := &model.Balance{BalanceID: "test-balance", Balance: 100, CreditBalance: 50, DebitBalance: 50, Currency: "USD"}

	metaDataJSON, _ := json.Marshal(balance.MetaData)
	mock.ExpectExec("UPDATE balances").WithArgs(balance.BalanceID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON).WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.updateBalance(*balance)
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
	monitor := model.BalanceMonitor{BalanceID: "test-balance", Description: "Test Monitor"}

	mock.ExpectExec("INSERT INTO balance_monitors").WithArgs(sqlmock.AnyArg(), monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

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

	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "created_at"}).
		AddRow(monitorID, "test-balance", "field", "operator", 1000, "Test Monitor", time.Now())

	mock.ExpectQuery("SELECT .* FROM balance_monitors WHERE monitor_id =").WithArgs(monitorID).WillReturnRows(rows)

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
	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "created_at"}).
		AddRow("test-monitor", "test-balance", "field", "operator", 100, "Test Monitor", time.Now())

	mock.ExpectQuery("SELECT .* FROM balance_monitors").WillReturnRows(rows)

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
	balanceID := "test-balance"
	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "created_at"}).
		AddRow("test-monitor", balanceID, "field", "operator", 100, "Test Monitor", time.Now())

	mock.ExpectQuery("SELECT .* FROM balance_monitors WHERE balance_id=").WithArgs(balanceID).WillReturnRows(rows)

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

	mock.ExpectExec("UPDATE balance_monitors").WithArgs(monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CreatedAt).WillReturnResult(sqlmock.NewResult(1, 1))

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

	mock.ExpectExec("DELETE FROM balance_monitors WHERE monitor_id =").WithArgs(monitorID).WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.DeleteMonitor(monitorID)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
