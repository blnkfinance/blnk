package blnk

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/jerry-enebeli/blnk/cache"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/jerry-enebeli/blnk/database"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func newTestDataSource() (database.IDataSource, sqlmock.Sqlmock, error) {
	config.MockConfig(false, "", "")
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
	mock.ExpectExec("INSERT INTO ledgers").
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

	mock.ExpectQuery("SELECT ledger_id,name, created_at, meta_data FROM ledgers LIMIT 20").WillReturnRows(rows)

	result, err := d.GetAllLedgers()

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

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM ledgers WHERE ledger_id =").
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
