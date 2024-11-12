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
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

type BigIntString struct {
	*big.Int
}

// Value implements the driver.Valuer interface
func (b BigIntString) Value() (driver.Value, error) {
	if b.Int == nil {
		return nil, nil
	}
	return b.String(), nil
}

func getBalanceMock(credit, debit, balance *big.Int, indicator string) *model.Balance {
	return &model.Balance{CreditBalance: credit, DebitBalance: debit, Balance: balance, Indicator: indicator}
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
			SourceBalance            *big.Int
			SourceCreditBalance      *big.Int
			SourceDebitBalance       *big.Int
			DestinationBalance       *big.Int
			DestinationCreditBalance *big.Int
			DestinationDebitBalance  *big.Int
			SourceIndicator          string
			DestinationIndicator     string
		}
	}{
		{
			name:               "Send 1k from destination to source balance.",
			ExpectedError:      nil,
			sourceBalance:      getBalanceMock(big.NewInt(2500), big.NewInt(0), big.NewInt(2500), "source-indicator"),
			destinationBalance: getBalanceMock(big.NewInt(0), big.NewInt(0), big.NewInt(0), "destination-indicator"),
			transaction:        getTransactionMock(1000, false),
			want: struct {
				SourceBalance            *big.Int
				SourceCreditBalance      *big.Int
				SourceDebitBalance       *big.Int
				DestinationBalance       *big.Int
				DestinationCreditBalance *big.Int
				DestinationDebitBalance  *big.Int
				SourceIndicator          string
				DestinationIndicator     string
			}{
				SourceBalance:            big.NewInt(1500),
				SourceDebitBalance:       big.NewInt(1000),
				SourceCreditBalance:      big.NewInt(2500),
				DestinationBalance:       big.NewInt(1000),
				DestinationDebitBalance:  big.NewInt(0),
				DestinationCreditBalance: big.NewInt(1000),
				SourceIndicator:          "source-indicator",
				DestinationIndicator:     "destination-indicator",
			},
		},
		{
			name:               "Debit 900m from source with start balance of 2.5b and debit balance of 500m. And destination start balance of 5k",
			ExpectedError:      nil,
			sourceBalance:      getBalanceMock(big.NewInt(2500000000), big.NewInt(500000000), big.NewInt(2500000000), "source-indicator"),
			destinationBalance: getBalanceMock(big.NewInt(5000), big.NewInt(0), big.NewInt(5000), "destination-indicator"),
			transaction:        getTransactionMock(900000000, false),
			want: struct {
				SourceBalance            *big.Int
				SourceCreditBalance      *big.Int
				SourceDebitBalance       *big.Int
				DestinationBalance       *big.Int
				DestinationCreditBalance *big.Int
				DestinationDebitBalance  *big.Int
				SourceIndicator          string
				DestinationIndicator     string
			}{
				SourceBalance:            big.NewInt(1100000000),
				SourceDebitBalance:       big.NewInt(1400000000),
				SourceCreditBalance:      big.NewInt(2500000000),
				DestinationBalance:       big.NewInt(900005000),
				DestinationDebitBalance:  big.NewInt(0),
				DestinationCreditBalance: big.NewInt(900005000),
				SourceIndicator:          "source-indicator",
				DestinationIndicator:     "destination-indicator",
			},
		},
		// Add more test cases as needed
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
			assert.Equal(t, tt.want.SourceIndicator, tt.sourceBalance.Indicator, "expected source indicators to match")
			assert.Equal(t, tt.want.DestinationIndicator, tt.destinationBalance.Indicator, "expected destination indicators to match")
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
	balance := model.Balance{Balance: big.NewInt(100), CreditBalance: big.NewInt(50), DebitBalance: big.NewInt(50), Currency: "USD", LedgerID: "test-id", IdentityID: "test-id"}

	// Convert metadata to JSON for mocking
	metaDataJSON, _ := json.Marshal(balance.MetaData)
	mock.ExpectExec("INSERT INTO blnk.balances").
		WithArgs(sqlmock.AnyArg(), balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.IdentityID, sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateBalance(context.Background(), balance)
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

	// Adjust the expected SQL to match the actual SQL output.
	expectedSQL := `SELECT b\.balance_id, b\.balance, b\.credit_balance, b\.debit_balance, b\.currency, b\.currency_multiplier, b\.ledger_id, COALESCE\(b\.identity_id, ''\) as identity_id, b\.created_at, b\.meta_data, b\.inflight_balance, b\.inflight_credit_balance, b\.inflight_debit_balance, b\.version, b\.indicator FROM \( SELECT \* FROM blnk\.balances WHERE balance_id = \$1 \) AS b`
	rows := sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "version", "indicator"}).
		AddRow(balanceID,
			BigIntString{big.NewInt(100)},
			BigIntString{big.NewInt(50)},
			BigIntString{big.NewInt(50)},
			"USD", 100, "test-ledger", "test-identity", time.Now(),
			`{"key":"value"}`,
			BigIntString{big.NewInt(0)},
			BigIntString{big.NewInt(0)},
			BigIntString{big.NewInt(0)},
			0,
			"test-indicator")

	mock.ExpectQuery(expectedSQL).
		WithArgs(balanceID).
		WillReturnRows(rows)

	mock.ExpectCommit()

	result, err := d.GetBalanceByID(context.Background(), balanceID, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, balanceID, result.BalanceID)
	assert.Equal(t, big.NewInt(100), result.Balance)
	assert.Equal(t, big.NewInt(50), result.CreditBalance)
	assert.Equal(t, big.NewInt(50), result.DebitBalance)
	assert.Equal(t, "test-indicator", result.Indicator)

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

	// The column order must match the actual SQL generated.
	rows := sqlmock.NewRows([]string{
		"balance_id", "indicator", "balance", "credit_balance", "debit_balance",
		"currency", "currency_multiplier", "ledger_id", "created_at", "meta_data",
	}).
		AddRow(
			"test-balance", "test-indicator", 100, 50, 50, "USD", 1.0, "test-ledger",
			time.Now(), `{"key":"value"}`,
		)

	mock.ExpectQuery(`SELECT balance_id, indicator, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data FROM blnk.balances ORDER BY created_at DESC LIMIT \$1 OFFSET \$2`).
		WithArgs(1, 1).
		WillReturnRows(rows)

	result, err := d.GetAllBalances(context.Background(), 1, 1)

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
		Balance:            big.NewInt(100),
		CreditBalance:      big.NewInt(50),
		DebitBalance:       big.NewInt(50),
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
        WHERE balance_id = $1`)).WithArgs(balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON).WillReturnResult(sqlmock.NewResult(1, 1))

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
	sourceBalance := &model.Balance{BalanceID: "source-balance", Balance: big.NewInt(100), CreditBalance: big.NewInt(50), DebitBalance: big.NewInt(50), Currency: "USD"}
	destinationBalance := &model.Balance{BalanceID: "destination-balance", Balance: big.NewInt(150), CreditBalance: big.NewInt(75), DebitBalance: big.NewInt(75), Currency: "USD"}

	// Expect transaction begin
	mock.ExpectBegin()

	// Expect the first update (for sourceBalance)
	mock.ExpectExec("UPDATE blnk.balances").WithArgs(
		sourceBalance.BalanceID, sourceBalance.Balance.String(), sourceBalance.CreditBalance.String(), sourceBalance.DebitBalance.String(), sourceBalance.InflightBalance.String(), sourceBalance.InflightCreditBalance.String(), sourceBalance.InflightDebitBalance.String(), sourceBalance.Currency, sourceBalance.CurrencyMultiplier, sourceBalance.LedgerID, sourceBalance.CreatedAt, sqlmock.AnyArg(), sourceBalance.Version,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect the second update (for destinationBalance)
	mock.ExpectExec("UPDATE blnk.balances").WithArgs(
		destinationBalance.BalanceID, destinationBalance.Balance.String(), destinationBalance.CreditBalance.String(), destinationBalance.DebitBalance.String(), destinationBalance.InflightBalance.String(), destinationBalance.InflightCreditBalance.String(), destinationBalance.InflightDebitBalance.String(), destinationBalance.Currency, destinationBalance.CurrencyMultiplier, destinationBalance.LedgerID, destinationBalance.CreatedAt, sqlmock.AnyArg(), destinationBalance.Version,
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
	monitor := model.BalanceMonitor{BalanceID: "test-balance", Description: "Test Monitor", CallBackURL: gofakeit.URL(), Condition: model.AlertCondition{Field: "field", Operator: "operator", Value: 1000, Precision: 100, PreciseValue: big.NewInt(100000)}}

	mock.ExpectExec("INSERT INTO blnk.balance_monitors").WithArgs(sqlmock.AnyArg(), monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Condition.Precision, monitor.Condition.PreciseValue.String(), monitor.Description, monitor.CallBackURL, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateMonitor(context.Background(), monitor)

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

	result, err := d.GetMonitorByID(context.Background(), monitorID)

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

	result, err := d.GetAllMonitors(context.Background())

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
	rows := sqlmock.NewRows([]string{"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value"}).
		AddRow("test-monitor", balanceID, "field", "operator", 100, "Test Monitor", gofakeit.URL(), time.Now(), 100, 100000)

	mock.ExpectQuery("SELECT .* FROM blnk.balance_monitors WHERE balance_id =").WithArgs(balanceID).WillReturnRows(rows)

	result, err := d.GetBalanceMonitors(context.Background(), balanceID)

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

	err = d.UpdateMonitor(context.Background(), monitor)

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

	err = d.DeleteMonitor(context.Background(), monitorID)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
