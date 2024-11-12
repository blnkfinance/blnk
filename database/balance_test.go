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

package database

import (
	"database/sql"
	"encoding/json"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestCreateBalance_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	balance := model.Balance{
		Balance:            big.NewInt(1000),
		CreditBalance:      big.NewInt(500),
		DebitBalance:       big.NewInt(500),
		Currency:           "USD",
		CurrencyMultiplier: 100,
		LedgerID:           "ldg1",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(balance.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.balances").
		WithArgs(sqlmock.AnyArg(), balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	createdBalance, err := ds.CreateBalance(balance)
	assert.NoError(t, err)
	assert.NotEmpty(t, createdBalance.BalanceID)
	assert.WithinDuration(t, time.Now(), createdBalance.CreatedAt, time.Second)
}

func TestCreateBalance_UniqueViolation(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	balance := model.Balance{
		Balance:            big.NewInt(1000),
		CreditBalance:      big.NewInt(500),
		DebitBalance:       big.NewInt(500),
		Currency:           "USD",
		CurrencyMultiplier: 100,
		LedgerID:           "ldg1",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(balance.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.balances").
		WithArgs(sqlmock.AnyArg(), balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON).
		WillReturnError(&pq.Error{Code: "23505", Message: "unique_violation"})

	_, err = ds.CreateBalance(balance)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)
}

func TestGetBalanceByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	// Mock the transaction Begin call
	mock.ExpectBegin()

	balance := model.Balance{
		BalanceID:          "bln1",
		Balance:            big.NewInt(1000),
		CreditBalance:      big.NewInt(500),
		DebitBalance:       big.NewInt(500),
		Currency:           "USD",
		Indicator:          gofakeit.Name(),
		CurrencyMultiplier: 100,
		LedgerID:           "ldg1",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(balance.MetaData)
	assert.NoError(t, err)

	// Use the exact query in your code and fix the typo for 'indicator'
	query := `
		SELECT b.balance_id, b.balance, b.credit_balance, b.debit_balance, b.currency, b.currency_multiplier, b.ledger_id, COALESCE(b.identity_id, '') as identity_id, b.created_at, b.meta_data, b.inflight_balance, b.inflight_credit_balance, b.inflight_debit_balance, b.version, b.indicator
		FROM ( SELECT * FROM blnk.balances WHERE balance_id = $1 ) AS b
	`

	// Use regexp.QuoteMeta to ensure sqlmock expects this exact query
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln1").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "version", "indicator",
		}).AddRow(balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, "", time.Now(), metaDataJSON, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), 1, balance.Indicator))

	// Mock the transaction commit call
	mock.ExpectCommit()

	retrievedBalance, err := ds.GetBalanceByID("bln1", []string{})
	assert.NoError(t, err)
	assert.Equal(t, balance.BalanceID, retrievedBalance.BalanceID)

	// Ensure all expectations are met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByID_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `SELECT balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data FROM blnk.balances WHERE balance_id = ?`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln1").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.GetBalanceByID("bln1", []string{})
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}
