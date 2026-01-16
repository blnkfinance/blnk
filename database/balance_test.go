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
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestContains_Found(t *testing.T) {
	slice := []string{"a", "b", "c"}
	assert.True(t, contains(slice, "a"))
	assert.True(t, contains(slice, "b"))
	assert.True(t, contains(slice, "c"))
}

func TestContains_NotFound(t *testing.T) {
	slice := []string{"a", "b", "c"}
	assert.False(t, contains(slice, "d"))
	assert.False(t, contains(slice, ""))
}

func TestContains_EmptySlice(t *testing.T) {
	slice := []string{}
	assert.False(t, contains(slice, "a"))
}

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

func TestGetBalanceByID_WithQueuedTransactions(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectBegin() // Start transaction

	balance := model.Balance{
		BalanceID:          "bln1",
		Balance:            big.NewInt(0),
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

	// First, expect the balance query
	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT b.balance_id, b.balance, b.credit_balance, b.debit_balance, b.currency, b.currency_multiplier, b.ledger_id, COALESCE(b.identity_id, '') as identity_id, b.created_at, b.meta_data, b.inflight_balance, b.inflight_credit_balance, b.inflight_debit_balance, b.version, b.indicator
        FROM ( SELECT * FROM blnk.balances WHERE balance_id = $1 ) AS b
    `)).WithArgs("bln1").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "balance", "credit_balance", "debit_balance", "currency",
			"currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data",
			"inflight_balance", "inflight_credit_balance", "inflight_debit_balance",
			"version", "indicator",
		}).AddRow(
			balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(),
			balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier,
			balance.LedgerID, "", time.Now(), metaDataJSON, balance.Balance.String(),
			balance.CreditBalance.String(), balance.DebitBalance.String(), 1, balance.Indicator,
		))

	// Then expect the queued transactions query with the correct SQL
	queuedTxnQuery := `
        SELECT t.precise_amount, t.source, t.destination 
        FROM blnk.transactions t 
        WHERE (t.source = $1 OR t.destination = $1) 
        AND t.status = 'QUEUED' 
        AND NOT EXISTS (
            SELECT 1 
            FROM blnk.transactions child 
            WHERE child.parent_transaction = t.transaction_id 
            AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
        )
    `

	queuedTxn := model.Transaction{
		TransactionID: "txn1",
		Source:        "bln1",
		Destination:   "bln2",
		Amount:        490,
		PreciseAmount: model.Int64ToBigInt(49000),
		Precision:     100,
		Currency:      "USD",
		Status:        "QUEUED",
		Reference:     "ref1",
		Description:   "Test queued transaction",
		CreatedAt:     time.Now(),
		MetaData:      map[string]interface{}{"key": "value"},
	}

	mock.ExpectQuery(regexp.QuoteMeta(queuedTxnQuery)).
		WithArgs("bln1").
		WillReturnRows(sqlmock.NewRows([]string{
			"precise_amount", "source", "destination",
		}).AddRow(
			queuedTxn.PreciseAmount.String(), queuedTxn.Source, queuedTxn.Destination,
		))

	// Finally, expect the commit
	mock.ExpectCommit()

	// Execute the function
	retrievedBalance, err := ds.GetBalanceByID("bln1", []string{}, true)
	assert.NoError(t, err)
	assert.Equal(t, balance.BalanceID, retrievedBalance.BalanceID)

	// Verify queued transaction impact
	expectedQueuedDebit := big.NewInt(49000)
	if queuedTxn.Source == "bln1" {
		assert.Equal(t, expectedQueuedDebit.String(), retrievedBalance.QueuedDebitBalance.String())
		assert.Equal(t, "0", retrievedBalance.QueuedCreditBalance.String())
	} else {
		assert.Equal(t, expectedQueuedDebit.String(), retrievedBalance.QueuedCreditBalance.String())
		assert.Equal(t, "0", retrievedBalance.QueuedDebitBalance.String())
	}

	// Verify all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByID_WithoutQueuedTransactions(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

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
		// Initialize queued balances
		QueuedDebitBalance:  big.NewInt(0),
		QueuedCreditBalance: big.NewInt(0),
	}

	metaDataJSON, err := json.Marshal(balance.MetaData)
	assert.NoError(t, err)

	// Only expect the balance query, not the queued transactions query
	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT b.balance_id, b.balance, b.credit_balance, b.debit_balance, b.currency, b.currency_multiplier, b.ledger_id, COALESCE(b.identity_id, '') as identity_id, b.created_at, b.meta_data, b.inflight_balance, b.inflight_credit_balance, b.inflight_debit_balance, b.version, b.indicator
        FROM ( SELECT * FROM blnk.balances WHERE balance_id = $1 ) AS b
    `)).WithArgs("bln1").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "balance", "credit_balance", "debit_balance", "currency",
			"currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data",
			"inflight_balance", "inflight_credit_balance", "inflight_debit_balance",
			"version", "indicator",
		}).AddRow(
			balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(),
			balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier,
			balance.LedgerID, "", time.Now(), metaDataJSON, balance.Balance.String(),
			balance.CreditBalance.String(), balance.DebitBalance.String(), 1, balance.Indicator,
		))

	mock.ExpectCommit()

	// Execute GetBalanceByID with withQueued=false
	retrievedBalance, err := ds.GetBalanceByID("bln1", []string{}, false)
	assert.NoError(t, err)
	assert.Equal(t, balance.BalanceID, retrievedBalance.BalanceID)

	// Initialize queued balances if they're nil
	if retrievedBalance.QueuedDebitBalance == nil {
		retrievedBalance.QueuedDebitBalance = big.NewInt(0)
	}
	if retrievedBalance.QueuedCreditBalance == nil {
		retrievedBalance.QueuedCreditBalance = big.NewInt(0)
	}

	// Verify no queued transactions were processed
	assert.Equal(t, "0", retrievedBalance.QueuedDebitBalance.String())
	assert.Equal(t, "0", retrievedBalance.QueuedCreditBalance.String())

	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByID_WithQueuedTransactions_HasAppliedChild(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectBegin()

	balance := model.Balance{
		BalanceID:          "bln1",
		Balance:            big.NewInt(0),
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

	// Expect the balance query
	mock.ExpectQuery(regexp.QuoteMeta(`
        SELECT b.balance_id, b.balance, b.credit_balance, b.debit_balance, b.currency, b.currency_multiplier, b.ledger_id, COALESCE(b.identity_id, '') as identity_id, b.created_at, b.meta_data, b.inflight_balance, b.inflight_credit_balance, b.inflight_debit_balance, b.version, b.indicator
        FROM ( SELECT * FROM blnk.balances WHERE balance_id = $1 ) AS b
    `)).WithArgs("bln1").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "balance", "credit_balance", "debit_balance", "currency",
			"currency_multiplier", "ledger_id", "identity_id", "created_at", "meta_data",
			"inflight_balance", "inflight_credit_balance", "inflight_debit_balance",
			"version", "indicator",
		}).AddRow(
			balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(),
			balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier,
			balance.LedgerID, "", time.Now(), metaDataJSON, balance.Balance.String(),
			balance.CreditBalance.String(), balance.DebitBalance.String(), 1, balance.Indicator,
		))

	// Set up the queued transactions query that checks for applied children
	queuedTxnQuery := `
        SELECT t.precise_amount, t.source, t.destination 
        FROM blnk.transactions t 
        WHERE (t.source = $1 OR t.destination = $1) 
        AND t.status = 'QUEUED' 
        AND NOT EXISTS (
            SELECT 1 
            FROM blnk.transactions child 
            WHERE child.parent_transaction = t.transaction_id 
            AND (child.status = 'APPLIED' OR child.status = 'REJECTED' OR child.status = 'VOID' or child.status = 'INFLIGHT')
        )
    `

	// This should return empty rows because all QUEUED transactions have APPLIED children
	mock.ExpectQuery(regexp.QuoteMeta(queuedTxnQuery)).
		WithArgs("bln1").
		WillReturnRows(sqlmock.NewRows([]string{
			"precise_amount", "source", "destination",
		}))

	mock.ExpectCommit()

	// Execute the function
	retrievedBalance, err := ds.GetBalanceByID("bln1", []string{}, true)
	assert.NoError(t, err)
	assert.Equal(t, balance.BalanceID, retrievedBalance.BalanceID)

	// Verify no queued transactions were included in the balance
	assert.Equal(t, "0", retrievedBalance.QueuedDebitBalance.String())
	assert.Equal(t, "0", retrievedBalance.QueuedCreditBalance.String())

	// Verify all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByIDLite_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
       FROM blnk.balances
       WHERE balance_id = $1
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_123").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id",
			"balance", "credit_balance", "debit_balance",
			"inflight_balance", "inflight_credit_balance", "inflight_debit_balance",
			"created_at", "version",
		}).AddRow(
			"bln_123", "ACC001", "USD", 100, "ldg_001",
			"100000", "60000", "40000",
			"5000", "3000", "2000",
			time.Now(), 1,
		))

	balance, err := ds.GetBalanceByIDLite("bln_123")
	assert.NoError(t, err)
	assert.NotNil(t, balance)
	assert.Equal(t, "bln_123", balance.BalanceID)
	assert.Equal(t, "ACC001", balance.Indicator)
	assert.Equal(t, "USD", balance.Currency)
	assert.Equal(t, "100000", balance.Balance.String())
	assert.Equal(t, "60000", balance.CreditBalance.String())
	assert.Equal(t, "40000", balance.DebitBalance.String())
	assert.Equal(t, "5000", balance.InflightBalance.String())

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByIDLite_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
       FROM blnk.balances
       WHERE balance_id = $1
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_nonexistent").
		WillReturnError(sql.ErrNoRows)

	balance, err := ds.GetBalanceByIDLite("bln_nonexistent")
	assert.Error(t, err)
	assert.Nil(t, balance)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByIDLite_NullIndicator(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
       FROM blnk.balances
       WHERE balance_id = $1
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_123").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id",
			"balance", "credit_balance", "debit_balance",
			"inflight_balance", "inflight_credit_balance", "inflight_debit_balance",
			"created_at", "version",
		}).AddRow(
			"bln_123", nil, "USD", 100, "ldg_001",
			"100000", "60000", "40000",
			"5000", "3000", "2000",
			time.Now(), 1,
		))

	balance, err := ds.GetBalanceByIDLite("bln_123")
	assert.NoError(t, err)
	assert.NotNil(t, balance)
	assert.Equal(t, "", balance.Indicator)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByIndicator_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
       FROM blnk.balances
       WHERE indicator = $1 AND currency = $2
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("ACC001", "USD").
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "indicator", "currency", "currency_multiplier", "ledger_id",
			"balance", "credit_balance", "debit_balance",
			"inflight_balance", "inflight_credit_balance", "inflight_debit_balance",
			"created_at", "version",
		}).AddRow(
			"bln_123", "ACC001", "USD", 100, "ldg_001",
			"100000", "60000", "40000",
			"5000", "3000", "2000",
			time.Now(), 1,
		))

	balance, err := ds.GetBalanceByIndicator("ACC001", "USD")
	assert.NoError(t, err)
	assert.NotNil(t, balance)
	assert.Equal(t, "bln_123", balance.BalanceID)
	assert.Equal(t, "ACC001", balance.Indicator)
	assert.Equal(t, "USD", balance.Currency)
	assert.Equal(t, "100000", balance.Balance.String())

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceByIndicator_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
       FROM blnk.balances
       WHERE indicator = $1 AND currency = $2
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("NONEXISTENT", "USD").
		WillReturnError(sql.ErrNoRows)

	balance, err := ds.GetBalanceByIndicator("NONEXISTENT", "USD")
	assert.Error(t, err)
	assert.NotNil(t, balance)
	assert.Equal(t, "", balance.BalanceID)
	assert.Contains(t, err.Error(), "not found")

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllBalances_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, _ := json.Marshal(metaData)

	query := `
        SELECT balance_id, indicator, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
        FROM blnk.balances
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(10, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "indicator", "balance", "credit_balance", "debit_balance",
			"currency", "currency_multiplier", "ledger_id", "created_at", "meta_data",
		}).
			AddRow("bln_1", "ACC001", "100000", "60000", "40000", "USD", 100, "ldg_001", time.Now(), metaDataJSON).
			AddRow("bln_2", "ACC002", "200000", "120000", "80000", "EUR", 100, "ldg_001", time.Now(), metaDataJSON))

	balances, err := ds.GetAllBalances(10, 0)
	assert.NoError(t, err)
	assert.Len(t, balances, 2)
	assert.Equal(t, "bln_1", balances[0].BalanceID)
	assert.Equal(t, "ACC001", balances[0].Indicator)
	assert.Equal(t, "100000", balances[0].Balance.String())
	assert.Equal(t, "bln_2", balances[1].BalanceID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllBalances_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
        SELECT balance_id, indicator, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
        FROM blnk.balances
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(10, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"balance_id", "indicator", "balance", "credit_balance", "debit_balance",
			"currency", "currency_multiplier", "ledger_id", "created_at", "meta_data",
		}))

	balances, err := ds.GetAllBalances(10, 0)
	assert.NoError(t, err)
	assert.Len(t, balances, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllBalances_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
        SELECT balance_id, indicator, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
        FROM blnk.balances
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
    `

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(10, 0).
		WillReturnError(fmt.Errorf("database connection error"))

	balances, err := ds.GetAllBalances(10, 0)
	assert.Error(t, err)
	assert.Nil(t, balances)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetSourceDestination_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `SELECT blnk.get_balances_by_id($1,$2)`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_source", "bln_dest").
		WillReturnError(fmt.Errorf("stored procedure error"))

	balances, err := ds.GetSourceDestination("bln_source", "bln_dest")
	assert.Error(t, err)
	assert.Nil(t, balances)
	assert.Contains(t, err.Error(), "stored procedure error")

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetSourceDestination_NoResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `SELECT blnk.get_balances_by_id($1,$2)`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_source", "bln_dest").
		WillReturnRows(sqlmock.NewRows([]string{"balance_id", "balance", "credit_balance", "debit_balance", "currency", "currency_multiplier", "ledger_id", "created_at", "meta_data"}))

	balances, err := ds.GetSourceDestination("bln_source", "bln_dest")
	assert.NoError(t, err)
	assert.Len(t, balances, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestCreateMonitor_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	monitor := model.BalanceMonitor{
		BalanceID:   "bln_123",
		Description: "Low balance alert",
		CallBackURL: "https://example.com/webhook",
		Condition: model.AlertCondition{
			Field:        "balance",
			Operator:     "<",
			Value:        1000,
			Precision:    100,
			PreciseValue: big.NewInt(100000),
		},
	}

	mock.ExpectExec(regexp.QuoteMeta(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)).
		WithArgs(sqlmock.AnyArg(), monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Condition.Precision, monitor.Condition.PreciseValue.String(), monitor.Description, monitor.CallBackURL, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	createdMonitor, err := ds.CreateMonitor(monitor)
	assert.NoError(t, err)
	assert.NotEmpty(t, createdMonitor.MonitorID)
	assert.True(t, len(createdMonitor.MonitorID) > 0)
	assert.Equal(t, monitor.BalanceID, createdMonitor.BalanceID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestCreateMonitor_UniqueViolation(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	monitor := model.BalanceMonitor{
		BalanceID: "bln_123",
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: "<",
			Value:    1000,
		},
	}

	mock.ExpectExec(regexp.QuoteMeta(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)).
		WithArgs(sqlmock.AnyArg(), monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(&pq.Error{Code: "23505", Message: "unique_violation"})

	_, err = ds.CreateMonitor(monitor)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestCreateMonitor_ForeignKeyViolation(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	monitor := model.BalanceMonitor{
		BalanceID: "bln_nonexistent",
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: "<",
			Value:    1000,
		},
	}

	mock.ExpectExec(regexp.QuoteMeta(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)).
		WithArgs(sqlmock.AnyArg(), monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(&pq.Error{Code: "23503", Message: "foreign_key_violation"})

	_, err = ds.CreateMonitor(monitor)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrBadRequest, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetMonitorByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		SELECT monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at
		FROM blnk.balance_monitors WHERE monitor_id = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("mon_123").
		WillReturnRows(sqlmock.NewRows([]string{
			"monitor_id", "balance_id", "field", "operator", "value", "precision", "precise_value", "description", "call_back_url", "created_at",
		}).AddRow(
			"mon_123", "bln_123", "balance", "<", 1000.0, 100, int64(100000), "Low balance alert", "https://example.com/webhook", time.Now(),
		))

	monitor, err := ds.GetMonitorByID("mon_123")
	assert.NoError(t, err)
	assert.NotNil(t, monitor)
	assert.Equal(t, "mon_123", monitor.MonitorID)
	assert.Equal(t, "bln_123", monitor.BalanceID)
	assert.Equal(t, "balance", monitor.Condition.Field)
	assert.Equal(t, "<", monitor.Condition.Operator)
	assert.Equal(t, float64(1000), monitor.Condition.Value)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetMonitorByID_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		SELECT monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at
		FROM blnk.balance_monitors WHERE monitor_id = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("mon_nonexistent").
		WillReturnError(sql.ErrNoRows)

	monitor, err := ds.GetMonitorByID("mon_nonexistent")
	assert.Error(t, err)
	assert.Nil(t, monitor)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllMonitors_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at
		FROM blnk.balance_monitors
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WillReturnRows(sqlmock.NewRows([]string{
			"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at",
		}).
			AddRow("mon_1", "bln_1", "balance", "<", 1000.0, "Alert 1", "https://example.com/1", time.Now()).
			AddRow("mon_2", "bln_2", "credit_balance", ">", 5000.0, "Alert 2", "https://example.com/2", time.Now()))

	monitors, err := ds.GetAllMonitors()
	assert.NoError(t, err)
	assert.Len(t, monitors, 2)
	assert.Equal(t, "mon_1", monitors[0].MonitorID)
	assert.Equal(t, "mon_2", monitors[1].MonitorID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllMonitors_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at
		FROM blnk.balance_monitors
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WillReturnRows(sqlmock.NewRows([]string{
			"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at",
		}))

	monitors, err := ds.GetAllMonitors()
	assert.NoError(t, err)
	assert.Len(t, monitors, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceMonitors_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
		FROM blnk.balance_monitors WHERE balance_id = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_123").
		WillReturnRows(sqlmock.NewRows([]string{
			"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value",
		}).
			AddRow("mon_1", "bln_123", "balance", "<", 1000.0, "Low alert", "https://example.com/1", time.Now(), 100, int64(100000)).
			AddRow("mon_2", "bln_123", "debit_balance", ">", 500.0, "High debit", "https://example.com/2", time.Now(), 100, int64(50000)))

	monitors, err := ds.GetBalanceMonitors("bln_123")
	assert.NoError(t, err)
	assert.Len(t, monitors, 2)
	assert.Equal(t, "mon_1", monitors[0].MonitorID)
	assert.Equal(t, "bln_123", monitors[0].BalanceID)
	assert.Equal(t, "mon_2", monitors[1].MonitorID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceMonitors_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
		FROM blnk.balance_monitors WHERE balance_id = $1
	`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_no_monitors").
		WillReturnRows(sqlmock.NewRows([]string{
			"monitor_id", "balance_id", "field", "operator", "value", "description", "call_back_url", "created_at", "precision", "precise_value",
		}))

	monitors, err := ds.GetBalanceMonitors("bln_no_monitors")
	assert.NoError(t, err)
	assert.Len(t, monitors, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateMonitor_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	monitor := &model.BalanceMonitor{
		MonitorID:   "mon_123",
		BalanceID:   "bln_123",
		Description: "Updated alert",
		CallBackURL: "https://example.com/updated",
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: "<=",
			Value:    2000,
		},
	}

	query := `
		UPDATE blnk.balance_monitors
		SET balance_id = $2, field = $3, operator = $4, value = $5, description = $6, call_back_url = $7
		WHERE monitor_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CallBackURL).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.UpdateMonitor(monitor)
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateMonitor_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	monitor := &model.BalanceMonitor{
		MonitorID:   "mon_nonexistent",
		BalanceID:   "bln_123",
		Description: "Updated alert",
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: "<=",
			Value:    2000,
		},
	}

	query := `
		UPDATE blnk.balance_monitors
		SET balance_id = $2, field = $3, operator = $4, value = $5, description = $6, call_back_url = $7
		WHERE monitor_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CallBackURL).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.UpdateMonitor(monitor)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestDeleteMonitor_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		DELETE FROM blnk.balance_monitors WHERE monitor_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("mon_123").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.DeleteMonitor("mon_123")
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestDeleteMonitor_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		DELETE FROM blnk.balance_monitors WHERE monitor_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("mon_nonexistent").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.DeleteMonitor("mon_nonexistent")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalance_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	balance := &model.Balance{
		BalanceID:          "bln_test_123",
		Balance:            big.NewInt(10000),
		CreditBalance:      big.NewInt(15000),
		DebitBalance:       big.NewInt(5000),
		Currency:           "USD",
		CurrencyMultiplier: 100,
		LedgerID:           "ldg_123",
		CreatedAt:          time.Now(),
		MetaData:           map[string]interface{}{"key": "value"},
	}

	metaDataJSON, _ := json.Marshal(balance.MetaData)

	query := `
		UPDATE blnk.balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(
			balance.BalanceID,
			balance.Balance.String(),
			balance.CreditBalance.String(),
			balance.DebitBalance.String(),
			balance.Currency,
			balance.CurrencyMultiplier,
			balance.LedgerID,
			balance.CreatedAt,
			metaDataJSON,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.UpdateBalance(balance)
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalance_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	balance := &model.Balance{
		BalanceID:          "bln_nonexistent",
		Balance:            big.NewInt(10000),
		CreditBalance:      big.NewInt(15000),
		DebitBalance:       big.NewInt(5000),
		Currency:           "USD",
		CurrencyMultiplier: 100,
		LedgerID:           "ldg_123",
		CreatedAt:          time.Now(),
		MetaData:           map[string]interface{}{},
	}

	metaDataJSON, _ := json.Marshal(balance.MetaData)

	query := `
		UPDATE blnk.balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(
			balance.BalanceID,
			balance.Balance.String(),
			balance.CreditBalance.String(),
			balance.DebitBalance.String(),
			balance.Currency,
			balance.CurrencyMultiplier,
			balance.LedgerID,
			balance.CreatedAt,
			metaDataJSON,
		).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.UpdateBalance(balance)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalance_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	balance := &model.Balance{
		BalanceID:          "bln_test_123",
		Balance:            big.NewInt(10000),
		CreditBalance:      big.NewInt(15000),
		DebitBalance:       big.NewInt(5000),
		Currency:           "USD",
		CurrencyMultiplier: 100,
		LedgerID:           "ldg_123",
		CreatedAt:          time.Now(),
		MetaData:           map[string]interface{}{},
	}

	metaDataJSON, _ := json.Marshal(balance.MetaData)

	query := `
		UPDATE blnk.balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(
			balance.BalanceID,
			balance.Balance.String(),
			balance.CreditBalance.String(),
			balance.DebitBalance.String(),
			balance.Currency,
			balance.CurrencyMultiplier,
			balance.LedgerID,
			balance.CreatedAt,
			metaDataJSON,
		).
		WillReturnError(errors.New("database connection error"))

	err = ds.UpdateBalance(balance)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalanceIdentity_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		UPDATE blnk.balances
		SET identity_id = $2
		WHERE balance_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("bln_test_123", "idt_user_456").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.UpdateBalanceIdentity("bln_test_123", "idt_user_456")
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalanceIdentity_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		UPDATE blnk.balances
		SET identity_id = $2
		WHERE balance_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("bln_nonexistent", "idt_user_456").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.UpdateBalanceIdentity("bln_nonexistent", "idt_user_456")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateBalanceIdentity_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	query := `
		UPDATE blnk.balances
		SET identity_id = $2
		WHERE balance_id = $1
	`

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("bln_test_123", "idt_user_456").
		WillReturnError(errors.New("database error"))

	err = ds.UpdateBalanceIdentity("bln_test_123", "idt_user_456")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceAtTime_InvalidBalanceID(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	balance, err := ds.GetBalanceAtTime(ctx, "", time.Now(), false)
	assert.Error(t, err)
	assert.Nil(t, balance)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrBadRequest, apiErr.Code)
}

func TestGetBalanceAtTime_ZeroTime(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	balance, err := ds.GetBalanceAtTime(ctx, "bln_test_123", time.Time{}, false)
	assert.Error(t, err)
	assert.Nil(t, balance)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrBadRequest, apiErr.Code)
}

func TestGetBalanceAtTime_TransactionBeginError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin().WillReturnError(errors.New("connection error"))

	balance, err := ds.GetBalanceAtTime(ctx, "bln_test_123", time.Now().Add(-1*time.Hour), false)
	assert.Error(t, err)
	assert.Nil(t, balance)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetBalanceAtTime_BalanceNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin()

	query := `SELECT currency, created_at FROM blnk.balances WHERE balance_id = $1`
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("bln_nonexistent").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectRollback()

	balance, err := ds.GetBalanceAtTime(ctx, "bln_nonexistent", time.Now().Add(-1*time.Hour), false)
	assert.Error(t, err)
	assert.Nil(t, balance)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestTakeBalanceSnapshots_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT blnk.take_daily_balance_snapshots_batched($1)")).
		WithArgs(100).
		WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(50))

	count, err := ds.TakeBalanceSnapshots(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 50, count)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestTakeBalanceSnapshots_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT blnk.take_daily_balance_snapshots_batched($1)")).
		WithArgs(100).
		WillReturnError(sql.ErrConnDone)

	count, err := ds.TakeBalanceSnapshots(ctx, 100)
	assert.Error(t, err)
	assert.Equal(t, 0, count)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetMostRecentSnapshot_Found(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	balanceID := "bln_test123"
	targetTime := time.Now()
	snapshotTime := targetTime.Add(-1 * time.Hour)

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT 
			balance,
			credit_balance,
			debit_balance,
			snapshot_time
		FROM blnk.balance_snapshots
		WHERE balance_id = $1
		AND snapshot_time <= $2
		ORDER BY snapshot_time DESC
		LIMIT 1`)).
		WithArgs(balanceID, targetTime).
		WillReturnRows(sqlmock.NewRows([]string{"balance", "credit_balance", "debit_balance", "snapshot_time"}).
			AddRow("1000", "500", "500", snapshotTime))

	creditBalance, debitBalance, resultTime, err := ds.getMostRecentSnapshot(ctx, tx, balanceID, targetTime)
	assert.NoError(t, err)
	assert.NotNil(t, creditBalance)
	assert.NotNil(t, debitBalance)
	assert.Equal(t, big.NewInt(500), creditBalance)
	assert.Equal(t, big.NewInt(500), debitBalance)
	assert.Equal(t, snapshotTime, resultTime)
}

func TestGetMostRecentSnapshot_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	balanceID := "bln_test123"
	targetTime := time.Now()

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT 
			balance,
			credit_balance,
			debit_balance,
			snapshot_time
		FROM blnk.balance_snapshots
		WHERE balance_id = $1
		AND snapshot_time <= $2
		ORDER BY snapshot_time DESC
		LIMIT 1`)).
		WithArgs(balanceID, targetTime).
		WillReturnError(sql.ErrNoRows)

	creditBalance, debitBalance, resultTime, err := ds.getMostRecentSnapshot(ctx, tx, balanceID, targetTime)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), creditBalance.Int64())
	assert.Equal(t, int64(0), debitBalance.Int64())
	assert.True(t, resultTime.IsZero())
}

func TestGetMostRecentSnapshot_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()

	balanceID := "bln_test123"
	targetTime := time.Now()

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT 
			balance,
			credit_balance,
			debit_balance,
			snapshot_time
		FROM blnk.balance_snapshots
		WHERE balance_id = $1
		AND snapshot_time <= $2
		ORDER BY snapshot_time DESC
		LIMIT 1`)).
		WithArgs(balanceID, targetTime).
		WillReturnError(sql.ErrConnDone)

	_, _, _, err = ds.getMostRecentSnapshot(ctx, tx, balanceID, targetTime)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestFetchTransactions_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	balanceID := "bln_test123"
	startTime := time.Now().Add(-24 * time.Hour)
	targetTime := time.Now()

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT precise_amount, source, destination, created_at, 
               COALESCE(effective_date, created_at) as effective_date
        FROM blnk.transactions
        WHERE (source = $1 OR destination = $1)
        AND COALESCE(effective_date, created_at) > $2
        AND COALESCE(effective_date, created_at) <= $3
        AND status = 'APPLIED'
        ORDER BY COALESCE(effective_date, created_at) ASC`)).
		WithArgs(balanceID, startTime, targetTime).
		WillReturnRows(sqlmock.NewRows([]string{"precise_amount", "source", "destination", "created_at", "effective_date"}).
			AddRow("1000", balanceID, "dest123", time.Now(), time.Now()))

	rows, err := fetchTransactions(ctx, tx, balanceID, startTime, targetTime)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	_ = rows.Close()
}

func TestFetchTransactions_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	balanceID := "bln_test123"
	startTime := time.Now().Add(-24 * time.Hour)
	targetTime := time.Now()

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT precise_amount, source, destination, created_at, 
               COALESCE(effective_date, created_at) as effective_date
        FROM blnk.transactions
        WHERE (source = $1 OR destination = $1)
        AND COALESCE(effective_date, created_at) > $2
        AND COALESCE(effective_date, created_at) <= $3
        AND status = 'APPLIED'
        ORDER BY COALESCE(effective_date, created_at) ASC`)).
		WithArgs(balanceID, startTime, targetTime).
		WillReturnError(sql.ErrConnDone)

	rows, err := fetchTransactions(ctx, tx, balanceID, startTime, targetTime)
	assert.Error(t, err)
	assert.Nil(t, rows)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestApplyTransaction_Debit(t *testing.T) {
	balanceID := "bln_test123"
	creditBalance := big.NewInt(1000)
	debitBalance := big.NewInt(500)

	txn := struct {
		PreciseAmount string
		Source        string
		Destination   string
		CreatedAt     time.Time
		EffectiveDate time.Time
	}{
		PreciseAmount: "100",
		Source:        balanceID,
		Destination:   "other_balance",
		CreatedAt:     time.Now(),
		EffectiveDate: time.Now(),
	}

	newCredit, newDebit, err := applyTransaction(txn, balanceID, creditBalance, debitBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), newCredit.Int64())
	assert.Equal(t, int64(600), newDebit.Int64())
}

func TestApplyTransaction_Credit(t *testing.T) {
	balanceID := "bln_test123"
	creditBalance := big.NewInt(1000)
	debitBalance := big.NewInt(500)

	txn := struct {
		PreciseAmount string
		Source        string
		Destination   string
		CreatedAt     time.Time
		EffectiveDate time.Time
	}{
		PreciseAmount: "200",
		Source:        "other_balance",
		Destination:   balanceID,
		CreatedAt:     time.Now(),
		EffectiveDate: time.Now(),
	}

	newCredit, newDebit, err := applyTransaction(txn, balanceID, creditBalance, debitBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(1200), newCredit.Int64())
	assert.Equal(t, int64(500), newDebit.Int64())
}

func TestApplyTransaction_InvalidAmount(t *testing.T) {
	balanceID := "bln_test123"
	creditBalance := big.NewInt(1000)
	debitBalance := big.NewInt(500)

	txn := struct {
		PreciseAmount string
		Source        string
		Destination   string
		CreatedAt     time.Time
		EffectiveDate time.Time
	}{
		PreciseAmount: "invalid_amount",
		Source:        balanceID,
		Destination:   "other_balance",
		CreatedAt:     time.Now(),
		EffectiveDate: time.Now(),
	}

	_, _, err := applyTransaction(txn, balanceID, creditBalance, debitBalance)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestCalculateBalanceFromTransactions_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	balanceID := "bln_test123"
	startTime := time.Now().Add(-24 * time.Hour)
	targetTime := time.Now()
	initialCredit := big.NewInt(1000)
	initialDebit := big.NewInt(500)

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT precise_amount, source, destination, created_at, 
               COALESCE(effective_date, created_at) as effective_date
        FROM blnk.transactions
        WHERE (source = $1 OR destination = $1)
        AND COALESCE(effective_date, created_at) > $2
        AND COALESCE(effective_date, created_at) <= $3
        AND status = 'APPLIED'
        ORDER BY COALESCE(effective_date, created_at) ASC`)).
		WithArgs(balanceID, startTime, targetTime).
		WillReturnRows(sqlmock.NewRows([]string{"precise_amount", "source", "destination", "created_at", "effective_date"}).
			AddRow("100", balanceID, "dest123", time.Now(), time.Now()).
			AddRow("200", "src123", balanceID, time.Now(), time.Now()))

	creditBalance, debitBalance, err := ds.calculateBalanceFromTransactions(ctx, tx, balanceID, startTime, targetTime, initialCredit, initialDebit)
	assert.NoError(t, err)
	assert.Equal(t, int64(1200), creditBalance.Int64())
	assert.Equal(t, int64(600), debitBalance.Int64())
}

func TestCalculateBalanceFromTransactions_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}
	ctx := context.Background()
	balanceID := "bln_test123"
	startTime := time.Now().Add(-24 * time.Hour)
	targetTime := time.Now()
	initialCredit := big.NewInt(1000)
	initialDebit := big.NewInt(500)

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT precise_amount, source, destination, created_at, 
               COALESCE(effective_date, created_at) as effective_date
        FROM blnk.transactions
        WHERE (source = $1 OR destination = $1)
        AND COALESCE(effective_date, created_at) > $2
        AND COALESCE(effective_date, created_at) <= $3
        AND status = 'APPLIED'
        ORDER BY COALESCE(effective_date, created_at) ASC`)).
		WithArgs(balanceID, startTime, targetTime).
		WillReturnError(sql.ErrConnDone)

	_, _, err = ds.calculateBalanceFromTransactions(ctx, tx, balanceID, startTime, targetTime, initialCredit, initialDebit)
	assert.Error(t, err)
}
