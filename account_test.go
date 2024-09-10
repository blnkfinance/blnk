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
	"encoding/json"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

func TestCreateAccount(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	if err != nil {
		return
	}
	assert.NoError(t, err)

	account := model.Account{
		Name:       "Test Account",
		Number:     "123456789",
		BankName:   "Test Bank",
		Currency:   "NGN",
		LedgerID:   gofakeit.UUID(),
		IdentityID: "identity_123",
		BalanceID:  gofakeit.UUID(),
		MetaData:   map[string]interface{}{"key": "value"},
	}
	metaDataJSON, _ := json.Marshal(account.MetaData)

	rows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(account.BalanceID, "", "NGN", 1, account.LedgerID, 100, 50, 50, 0, 0, 0, time.Now(), 0)

	mock.ExpectQuery("SELECT .* FROM blnk.balances WHERE balance_id =").
		WithArgs(account.BalanceID).
		WillReturnRows(rows)

	mock.ExpectExec("INSERT INTO blnk.accounts").
		WithArgs(sqlmock.AnyArg(), account.Name, account.Number, account.BankName, account.Currency, account.LedgerID, account.IdentityID, account.BalanceID, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	config.MockConfig(&config.Configuration{Server: config.ServerConfig{SecretKey: "some-secret"}, AccountNumberGeneration: config.AccountNumberGenerationConfig{HttpService: config.AccountGenerationHttpService{}}})

	result, err := d.CreateAccount(account)
	assert.NoError(t, err)
	assert.Equal(t, account.Name, result.Name)
	assert.Equal(t, account.Number, result.Number)
	assert.WithinDuration(t, time.Now(), result.CreatedAt, time.Second)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateAccountWithExternalGenerator(t *testing.T) {
	// Initialize the mock HTTP responder
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	// Mock the external account generator response
	httpmock.RegisterResponder("GET", "http://example.com/generateAccount",
		httpmock.NewStringResponder(200, `{"account_number": "123456789", "bank_name": "Blnk Bank"}`))

	config.MockConfig(&config.Configuration{Server: config.ServerConfig{SecretKey: "some-secret"}, AccountNumberGeneration: config.AccountNumberGenerationConfig{HttpService: config.AccountGenerationHttpService{Url: "http://example.com/generateAccount"}}})

	account := model.Account{
		Name:       "Test Account",
		BankName:   "Blnk Bank",
		Number:     "123456789",
		Currency:   "NGN",
		LedgerID:   "ledger_123",
		IdentityID: gofakeit.UUID(),
		BalanceID:  gofakeit.UUID(),
		MetaData:   map[string]interface{}{"key": "value"},
	}
	metaDataJSON, _ := json.Marshal(account.MetaData)

	rows := sqlmock.NewRows([]string{"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id", "balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version"}).
		AddRow(account.BalanceID, "", "NGN", 1, account.LedgerID, 100, 50, 50, 0, 0, 0, time.Now(), 0)

	mock.ExpectQuery("SELECT .* FROM blnk.balances WHERE balance_id =").
		WithArgs(account.BalanceID).
		WillReturnRows(rows)

	mock.ExpectExec("INSERT INTO blnk.accounts").
		WithArgs(sqlmock.AnyArg(), account.Name, account.Number, account.BankName, account.Currency, account.LedgerID, account.IdentityID, account.BalanceID, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateAccount(account)
	assert.NoError(t, err)
	assert.Equal(t, "123456789", result.Number)
	assert.Equal(t, "Blnk Bank", result.BankName)
	assert.WithinDuration(t, time.Now(), result.CreatedAt, time.Second)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAccountByID(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	testID := "test-account-id"
	account := model.Account{
		Name:       "Test Account",
		Number:     "123456789",
		BankName:   "Test Bank",
		Currency:   "USD",
		LedgerID:   "ledger_123",
		IdentityID: "identity_123",
		BalanceID:  "balance_123",
		MetaData:   map[string]interface{}{"key": "value"},
	}
	metaDataJSON, _ := json.Marshal(account.MetaData)

	rows := sqlmock.NewRows([]string{"account_id", "name", "number", "bank_name", "currency", "ledger_id", "identity_id", "balance_id", "created_at", "meta_data"}).
		AddRow(testID, account.Name, account.Number, account.BankName, account.Currency, account.LedgerID, account.IdentityID, account.BalanceID, time.Now(), metaDataJSON)

	// Expect transaction to begin
	mock.ExpectBegin()

	mock.ExpectQuery("SELECT .* FROM blnk.accounts WHERE account_id =").
		WithArgs(testID).
		WillReturnRows(rows)

	// Expect transaction to commit
	mock.ExpectCommit()
	result, err := d.GetAccount(testID, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testID, result.AccountID)
	assert.Equal(t, account.Name, result.Name)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAllAccounts(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	assert.NoError(t, err)

	d, err := NewBlnk(datasource)
	assert.NoError(t, err)

	account1 := model.Account{
		AccountID: "id1",
		Name:      "Test Account 1",
	}
	metaDataJSON1, _ := json.Marshal(account1.MetaData)

	account2 := model.Account{
		AccountID: "id2",
		Name:      "Test Account 2",
	}
	metaDataJSON2, _ := json.Marshal(account2.MetaData)

	rows := sqlmock.NewRows([]string{"account_id", "name", "number", "bank_name", "currency", "created_at", "meta_data"}).
		AddRow(account1.AccountID, account1.Name, account1.Number, account1.BankName, account1.Currency, time.Now(), metaDataJSON1).
		AddRow(account2.AccountID, account2.Name, account2.Number, account2.BankName, account1.Currency, time.Now(), metaDataJSON2)

	mock.ExpectQuery("SELECT .* FROM blnk.accounts").WillReturnRows(rows)

	result, err := d.GetAllAccounts()
	assert.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, account1.AccountID, result[0].AccountID)
	assert.Equal(t, account2.AccountID, result[1].AccountID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
