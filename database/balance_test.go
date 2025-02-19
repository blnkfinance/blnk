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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

	retrievedBalance, err := ds.GetBalanceByID("bln1", []string{}, false)
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

	_, err = ds.GetBalanceByID("bln1", []string{}, false)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestUpdateBalances_Success(t *testing.T) {
	// Setup mock database
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	// Create test balances
	sourceBalance := &model.Balance{
		BalanceID:          "bln1",
		Balance:            big.NewInt(1000),
		CreditBalance:      big.NewInt(500),
		DebitBalance:       big.NewInt(500),
		Version:            1,
		Currency:           "USD",
		CurrencyMultiplier: 100,
	}

	destBalance := &model.Balance{
		BalanceID:          "bln2",
		Balance:            big.NewInt(2000),
		CreditBalance:      big.NewInt(1000),
		DebitBalance:       big.NewInt(1000),
		Version:            1,
		Currency:           "USD",
		CurrencyMultiplier: 100,
	}

	// Set up expectations
	mock.ExpectBegin()

	// Expect source balance update
	mock.ExpectExec(regexp.QuoteMeta(`
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
        WHERE balance_id = $1 AND version = $12
    `)).WithArgs(
		sourceBalance.BalanceID,
		sourceBalance.Balance.String(),
		sourceBalance.CreditBalance.String(),
		sourceBalance.DebitBalance.String(),
		sourceBalance.InflightBalance.String(),
		sourceBalance.InflightCreditBalance.String(),
		sourceBalance.InflightDebitBalance.String(),
		sourceBalance.Currency,
		sourceBalance.CurrencyMultiplier,
		sourceBalance.LedgerID,
		sourceBalance.CreatedAt,
		sourceBalance.Version,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect destination balance update
	mock.ExpectExec(regexp.QuoteMeta(`
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
        WHERE balance_id = $1 AND version = $12
    `)).WithArgs(
		destBalance.BalanceID,
		destBalance.Balance.String(),
		destBalance.CreditBalance.String(),
		destBalance.DebitBalance.String(),
		destBalance.InflightBalance.String(),
		destBalance.InflightCreditBalance.String(),
		destBalance.InflightDebitBalance.String(),
		destBalance.Currency,
		destBalance.CurrencyMultiplier,
		destBalance.LedgerID,
		destBalance.CreatedAt,
		destBalance.Version,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	// Execute the function
	err = ds.UpdateBalances(ctx, sourceBalance, destBalance)
	assert.NoError(t, err)

	// Verify that all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalances_BeginTxError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("begin transaction error"))

	err = ds.UpdateBalances(ctx, &model.Balance{}, &model.Balance{})
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestUpdateBalances_SourceUpdateError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	sourceBalance := &model.Balance{
		BalanceID: "bln1",
		Version:   1,
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE blnk.balances`)).
		WithArgs(
			sourceBalance.BalanceID,
			"0",              // Balance string
			"0",              // CreditBalance string
			"0",              // DebitBalance string
			"0",              // InflightBalance string
			"0",              // InflightCreditBalance string
			"0",              // InflightDebitBalance string
			"",               // Currency
			0,                // CurrencyMultiplier
			"",               // LedgerID
			sqlmock.AnyArg(), // CreatedAt
			sourceBalance.Version,
		).
		WillReturnError(fmt.Errorf("source update error"))
	mock.ExpectRollback()

	err = ds.UpdateBalances(ctx, sourceBalance, &model.Balance{})
	assert.Error(t, err)
}

func TestUpdateBalances_DestUpdateError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	sourceBalance := &model.Balance{
		BalanceID: "bln1",
		Version:   1,
	}
	destBalance := &model.Balance{
		BalanceID: "bln2",
		Version:   1,
	}

	mock.ExpectBegin()
	// Expect successful source balance update
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE blnk.balances`)).
		WithArgs(
			sourceBalance.BalanceID,
			"0",              // Balance string
			"0",              // CreditBalance string
			"0",              // DebitBalance string
			"0",              // InflightBalance string
			"0",              // InflightCreditBalance string
			"0",              // InflightDebitBalance string
			"",               // Currency
			0,                // CurrencyMultiplier
			"",               // LedgerID
			sqlmock.AnyArg(), // CreatedAt
			sourceBalance.Version,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect failed destination balance update
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE blnk.balances`)).
		WithArgs(
			destBalance.BalanceID,
			"0",              // Balance string
			"0",              // CreditBalance string
			"0",              // DebitBalance string
			"0",              // InflightBalance string
			"0",              // InflightCreditBalance string
			"0",              // InflightDebitBalance string
			"",               // Currency
			0,                // CurrencyMultiplier
			"",               // LedgerID
			sqlmock.AnyArg(), // CreatedAt
			destBalance.Version,
		).
		WillReturnError(fmt.Errorf("destination update error"))
	mock.ExpectRollback()

	err = ds.UpdateBalances(ctx, sourceBalance, destBalance)
	assert.Error(t, err)
}

func TestUpdateBalances_CommitError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	sourceBalance := &model.Balance{
		BalanceID: "bln1",
		Version:   1,
	}
	destBalance := &model.Balance{
		BalanceID: "bln2",
		Version:   1,
	}

	mock.ExpectBegin()
	// Expect successful source balance update
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE blnk.balances`)).
		WithArgs(
			sourceBalance.BalanceID,
			"0",              // Balance string
			"0",              // CreditBalance string
			"0",              // DebitBalance string
			"0",              // InflightBalance string
			"0",              // InflightCreditBalance string
			"0",              // InflightDebitBalance string
			"",               // Currency
			0,                // CurrencyMultiplier
			"",               // LedgerID
			sqlmock.AnyArg(), // CreatedAt
			sourceBalance.Version,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect successful destination balance update
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE blnk.balances`)).
		WithArgs(
			destBalance.BalanceID,
			"0",              // Balance string
			"0",              // CreditBalance string
			"0",              // DebitBalance string
			"0",              // InflightBalance string
			"0",              // InflightCreditBalance string
			"0",              // InflightDebitBalance string
			"",               // Currency
			0,                // CurrencyMultiplier
			"",               // LedgerID
			sqlmock.AnyArg(), // CreatedAt
			destBalance.Version,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit().WillReturnError(fmt.Errorf("commit error"))

	err = ds.UpdateBalances(ctx, sourceBalance, destBalance)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}
