package database

import (
	"sync"
	"testing"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/stretchr/testify/assert"
)

func TestGetDBConnection_Singleton(t *testing.T) {
	// Reset the instance and once for testing purposes
	instance = nil
	once = sync.Once{}

	// Create a mock configuration with a valid DNS string
	mockConfig := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost/blnk?sslmode=disable",
		},
	}

	config.MockConfig(mockConfig)

	// First call to GetDBConnection should initialize the instance
	ds1, err := GetDBConnection(mockConfig)
	assert.NoError(t, err)
	assert.NotNil(t, ds1)

	// Second call should return the same instance
	ds2, err := GetDBConnection(mockConfig)
	assert.NoError(t, err)
	assert.Equal(t, ds1, ds2)
}

func TestGetDBConnection_Failure(t *testing.T) {
	// Reset the instance and once for testing purposes
	instance = nil
	once = sync.Once{}

	// Create a mock configuration with invalid DNS
	mockConfig := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "invalid-dns",
		},
	}

	// Expect error when connecting to DB with invalid DNS
	_, err := GetDBConnection(mockConfig)
	assert.Error(t, err)
}

func TestConnectDB_Success(t *testing.T) {
	// Provide a valid DNS string for your testing database
	dns := "postgres://postgres:password@localhost/blnk?sslmode=disable"

	db, err := ConnectDB(dns)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// Ensure that db is not nil before calling Close
	if db != nil {
		defer db.Close()
	}
}

func TestConnectDB_Failure(t *testing.T) {
	// Provide an invalid DNS string to simulate a failure
	invalidDNS := "invalid-dns"

	db, err := ConnectDB(invalidDNS)
	assert.Error(t, err)
	assert.Nil(t, db)
}
