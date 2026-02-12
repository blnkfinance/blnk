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
	"log"
	"sync"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/cache"
	pgconn "github.com/blnkfinance/blnk/internal/pg-conn"
)

// Declare a package-level variable to hold the singleton instance.
var (
	instance *Datasource
	once     sync.Once
)

type Datasource struct {
	Conn  *sql.DB
	Cache cache.Cache
}

// Close closes the underlying database connection pool.
func (d *Datasource) Close() error {
	if d.Conn != nil {
		return d.Conn.Close()
	}
	return nil
}

// NewDataSource initializes a new database connection.
func NewDataSource(configuration *config.Configuration) (IDataSource, error) {
	con, err := GetDBConnection(configuration)
	if err != nil {
		return nil, err
	}

	// Set the default schema for this connection.
	if _, err := con.Conn.ExecContext(context.Background(), "SET search_path TO blnk"); err != nil {
		return nil, err
	}
	return con, nil
}

// GetDBConnection ensures a single database connection instance.
func GetDBConnection(configuration *config.Configuration) (*Datasource, error) {
	var err error
	once.Do(func() {
		con, errConn := ConnectDB(configuration.DataSource)
		if errConn != nil {
			err = errConn
			return
		}

		cacheInstance, errCache := cache.NewCache()
		if errCache != nil {
			log.Printf("Error creating cache: %v", errCache)
			// Continue without cache instead of failing completely.
		}

		instance = &Datasource{Conn: con, Cache: cacheInstance}
	})
	if err != nil {
		return nil, err
	}
	return instance, nil
}

// ConnectDB establishes a database connection with pooling.
func ConnectDB(dsConfig config.DataSourceConfig) (*sql.DB, error) {
	return pgconn.ConnectDB(dsConfig)
}
