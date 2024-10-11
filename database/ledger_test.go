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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestCreateLedger_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	ledger := model.Ledger{
		Name: "Test Ledger",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(ledger.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	createdLedger, err := ds.CreateLedger(ledger)
	assert.NoError(t, err)
	assert.NotEmpty(t, createdLedger.LedgerID)
	assert.WithinDuration(t, time.Now(), createdLedger.CreatedAt, time.Second)
}

func TestCreateLedger_UniqueViolation(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	ledger := model.Ledger{
		Name: "Test Ledger",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(ledger.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnError(&pq.Error{Code: "23505", Message: "unique_violation"})

	_, err = ds.CreateLedger(ledger)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)
}

func TestGetAllLedgers_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
		AddRow("ldg1", "Ledger 1", time.Now(), metaDataJSON).
		AddRow("ldg2", "Ledger 2", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers ORDER BY created_at DESC LIMIT \\$1 OFFSET \\$2").
		WithArgs(2, 0).
		WillReturnRows(rows)
	ledgers, err := ds.GetAllLedgers(2, 0)
	assert.NoError(t, err)
	assert.Len(t, ledgers, 2)
	assert.Equal(t, "Ledger 1", ledgers[0].Name)
}

func TestGetLedgerByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	row := sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
		AddRow("ldg1", "Ledger 1", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnRows(row)

	ledger, err := ds.GetLedgerByID("ldg1")
	assert.NoError(t, err)
	assert.Equal(t, "Ledger 1", ledger.Name)
}

func TestGetLedgerByID_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.GetLedgerByID("ldg1")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
}
