package pgconn

import (
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
)

func TestConnectDB_InvalidDNS(t *testing.T) {
	dsConfig := config.DataSourceConfig{
		Dns:             "invalid-postgres-url",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
	}

	db, err := ConnectDB(dsConfig)
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestConnectDB_EmptyDNS(t *testing.T) {
	dsConfig := config.DataSourceConfig{
		Dns:             "",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
	}

	db, err := ConnectDB(dsConfig)
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestConnectDB_UnreachableHost(t *testing.T) {
	dsConfig := config.DataSourceConfig{
		Dns:             "postgres://user:password@localhost:9999/nonexistent?sslmode=disable",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
	}

	db, err := ConnectDB(dsConfig)
	// The connection will fail during Ping
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestDatasource_Struct(t *testing.T) {
	ds := &Datasource{
		Conn:  nil,
		Cache: nil,
	}

	assert.NotNil(t, ds)
	assert.Nil(t, ds.Conn)
	assert.Nil(t, ds.Cache)
}
