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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
)

func TestRecordTransaction_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestRecordTransaction")
	defer span.End()

	ds := Datasource{Conn: db}

	transaction := &model.Transaction{
		TransactionID:     "txn123",
		Source:            "src1",
		Reference:         "ref123",
		Amount:            1000,
		Currency:          "USD",
		Destination:       "dest1",
		Description:       "Test Transaction",
		Status:            "PENDING",
		CreatedAt:         time.Now(),
		MetaData:          map[string]interface{}{"key": "value"},
		ScheduledFor:      time.Now(),
		Hash:              "hash123",
		PreciseAmount:     1000,
		Precision:         2,
		Rate:              1,
		ParentTransaction: "parent123",
	}

	metaDataJSON, err := json.Marshal(transaction.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.transactions").
		WithArgs(transaction.TransactionID, transaction.ParentTransaction, transaction.Source, transaction.Reference, transaction.Amount, transaction.PreciseAmount, transaction.Precision, transaction.Rate, transaction.Currency, transaction.Destination, transaction.Description, transaction.Status, transaction.CreatedAt, metaDataJSON, transaction.ScheduledFor, transaction.Hash).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := ds.RecordTransaction(ctx, transaction)
	assert.NoError(t, err)
	assert.Equal(t, transaction, result)
}

func TestRecordTransaction_Failure(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestRecordTransactionFailure")
	defer span.End()

	ds := Datasource{Conn: db}

	transaction := &model.Transaction{
		TransactionID:     "txn123",
		Source:            "src1",
		Reference:         "ref123",
		Amount:            1000,
		Currency:          "USD",
		Destination:       "dest1",
		Description:       "Test Transaction",
		Status:            "PENDING",
		CreatedAt:         time.Now(),
		MetaData:          map[string]interface{}{"key": "value"},
		ScheduledFor:      time.Now(),
		Hash:              "hash123",
		PreciseAmount:     1000,
		Precision:         2,
		Rate:              1,
		ParentTransaction: "parent123",
	}

	metaDataJSON, err := json.Marshal(transaction.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.transactions").
		WithArgs(transaction.TransactionID, transaction.ParentTransaction, transaction.Source, transaction.Reference, transaction.Amount, transaction.PreciseAmount, transaction.Precision, transaction.Rate, transaction.Currency, transaction.Destination, transaction.Description, transaction.Status, transaction.CreatedAt, metaDataJSON, transaction.ScheduledFor, transaction.Hash).
		WillReturnError(errors.New("db error"))

	_, err = ds.RecordTransaction(ctx, transaction)
	assert.Error(t, err)
	assert.IsType(t, apierror.APIError{}, err)
}

func TestGetTransaction_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTransaction")
	defer span.End()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"transaction_id", "source", "reference", "amount", "precise_amount", "precision", "currency", "destination", "description", "status", "created_at", "meta_data"}).
		AddRow("txn123", "src1", "ref123", 1000, 1000, 2, "USD", "dest1", "Test Transaction", "PENDING", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data FROM blnk.transactions WHERE transaction_id = ?").
		WithArgs("txn123").
		WillReturnRows(rows)

	txn, err := ds.GetTransaction(ctx, "txn123")
	assert.NoError(t, err)
	assert.Equal(t, "txn123", txn.TransactionID)
	assert.Equal(t, "src1", txn.Source)
	assert.Equal(t, "dest1", txn.Destination)
}

func TestGetTransaction_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestGetTransactionNotFound")
	defer span.End()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data FROM blnk.transactions WHERE transaction_id = ?").
		WithArgs("txn123").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.GetTransaction(ctx, "txn123")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
}

func TestTransactionExistsByRef_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestTransactionExistsByRef")
	defer span.End()

	ds := Datasource{Conn: db}

	// Modify the expected query to match the actual SQL query placeholder
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM blnk.transactions WHERE reference = \\$1\\)").
		WithArgs("ref123").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	exists, err := ds.TransactionExistsByRef(ctx, "ref123")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestTransactionExistsByRef_Failure(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	tracer := otel.Tracer("transaction.database")
	ctx, span := tracer.Start(context.Background(), "TestTransactionExistsByRefFailure")
	defer span.End()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM blnk.transactions WHERE reference = ?\\)").
		WithArgs("ref123").
		WillReturnError(errors.New("db error"))

	_, err = ds.TransactionExistsByRef(ctx, "ref123")
	assert.Error(t, err)
	assert.IsType(t, apierror.APIError{}, err)
}
