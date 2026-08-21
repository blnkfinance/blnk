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
	"sync"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/cache"
	pgconn "github.com/blnkfinance/blnk/internal/pg-conn"
	"github.com/sirupsen/logrus"
)

// Declare a package-level variable to hold the singleton instance.
//
// instanceMu guards instance. A plain mutex is used rather than sync.Once
// because a Once latches on its first run whether that run succeeded or not:
// if the very first connection attempt in the process failed, the body would
// never run again, instance would stay nil forever, and every later caller
// would be handed that nil back. Connecting is exactly the kind of work that
// deserves a second attempt, so failures leave instance unset and the next
// call tries again.
var (
	instance   *Datasource
	instanceMu sync.Mutex
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
//
// It never returns a nil *Datasource alongside a nil error: either the
// singleton is returned, or the connection error that prevented building it.
func GetDBConnection(configuration *config.Configuration) (*Datasource, error) {
	instanceMu.Lock()
	defer instanceMu.Unlock()

	if instance != nil {
		return instance, nil
	}

	con, err := ConnectDB(configuration.DataSource)
	if err != nil {
		// Leave instance nil so a later call can retry rather than being
		// permanently poisoned by one unreachable-database moment.
		return nil, err
	}

	cacheInstance, errCache := cache.NewCache()
	if errCache != nil {
		logrus.Errorf("Error creating cache: %v", errCache)
		// Continue without cache instead of failing completely.
	}

	instance = &Datasource{Conn: con, Cache: cacheInstance}
	return instance, nil
}

// ConnectDB establishes a database connection with pooling.
func ConnectDB(dsConfig config.DataSourceConfig) (*sql.DB, error) {
	return pgconn.ConnectDB(dsConfig)
}
