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
	"database/sql"
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/model"

	"github.com/blnkfinance/blnk/config"

	"github.com/blnkfinance/blnk/database"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func newTestDataSource() (database.IDataSource, sqlmock.Sqlmock, error) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue",
			TransactionQueue: "transaction_queue",
			IndexQueue:       "index_queue",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{SecretKey: "some-secret"},
		AccountNumberGeneration: config.AccountNumberGenerationConfig{
			HttpService: config.AccountGenerationHttpService{
				Url: "http://example.com/generateAccount",
			},
		},
	}

	config.ConfigStore.Store(cnf)
	db, mock, err := sqlmock.New()
	if err != nil {
		log.Printf("an error '%s' was not expected when opening a stub database Connection", err)
	}
	newCache, err := cache.NewCache()
	if err != nil {
		log.Printf("an error '%s' was not expected", err)
	}
	return &database.Datasource{Conn: db, Cache: newCache}, mock, nil
}

func TestCreateLedger(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	ledger := model.Ledger{Name: "Test Ledger", MetaData: map[string]interface{}{"key": "value"}}
	metaDataJSON, _ := json.Marshal(ledger.MetaData)

	// Set expectations on mock
	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Execute the test function
	result, err := d.CreateLedger(ledger)
	// Assertions
	assert.NoError(t, err)
	assert.NotEmpty(t, result.LedgerID)
	assert.WithinDuration(t, time.Now(), result.CreatedAt, time.Second)
	assert.Contains(t, result.LedgerID, "ldg_")

	// Check if all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAllLedgers(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	rows := sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
		AddRow("ldg_1234567", "general ledger", time.Now(), `{"key":"value"}`)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers ORDER BY created_at DESC LIMIT \\$1 OFFSET \\$2").
		WithArgs(1, 1).
		WillReturnRows(rows)

	result, err := d.GetAllLedgers(1, 1)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "ldg_1234567", result[0].LedgerID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetLedgerByID(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}
	testID := gofakeit.UUID()
	row := sqlmock.NewRows([]string{gofakeit.UUID(), "name", "created_at", "meta_data"}).
		AddRow(testID, "test-name", time.Now(), `{"key":"value"}`)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id =").
		WithArgs(testID).
		WillReturnRows(row)

	result, err := d.GetLedgerByID(testID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testID, result.LedgerID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateLedger(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "ldg_1234567"
	originalName := "Original Ledger"
	newName := "Updated Ledger"
	testTime := time.Now()

	// Mock the GetLedgerByID call first (called by UpdateLedger)
	row := sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
		AddRow(testID, originalName, testTime, `{"key":"value"}`)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id =").
		WithArgs(testID).
		WillReturnRows(row)

	// Mock the UPDATE query
	mock.ExpectExec("UPDATE blnk.ledgers SET name = \\$1 WHERE ledger_id = \\$2").
		WithArgs(newName, testID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.UpdateLedger(testID, newName)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testID, result.LedgerID)
	assert.Equal(t, newName, result.Name)
	assert.Equal(t, testTime, result.CreatedAt)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateLedgerNotFound(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "ldg_nonexistent"
	newName := "Updated Ledger"

	// Mock the GetLedgerByID call to return no rows (ledger not found)
	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id =").
		WithArgs(testID).
		WillReturnError(sql.ErrNoRows)

	result, err := d.UpdateLedger(testID, newName)

	assert.Error(t, err)
	assert.Nil(t, result)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
