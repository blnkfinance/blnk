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
	"log"
	"sync"
	"time"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/internal/cache"
)

// Declare a package-level variable to hold the singleton instance.
var instance *Datasource
var once sync.Once

type Datasource struct {
	Conn  *sql.DB
	Cache cache.Cache
}

// NewDataSource initializes a new database connection.
func NewDataSource(configuration *config.Configuration) (IDataSource, error) {
	con, err := GetDBConnection(configuration)
	if err != nil {
		return nil, err
	}

	// Set the default schema for this connection.
	if _, err := con.Conn.Exec("SET search_path TO blnk"); err != nil {
		return nil, err
	}
	return con, nil
}

// GetDBConnection ensures a single database connection instance.
func GetDBConnection(configuration *config.Configuration) (*Datasource, error) {
	var err error
	once.Do(func() {
		con, errConn := ConnectDB(configuration.DataSource.Dns)
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
func ConnectDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// Apply connection pooling settings
	db.SetMaxOpenConns(25)                  // Maximum number of open connections
	db.SetMaxIdleConns(10)                  // Maximum number of idle connections
	db.SetConnMaxLifetime(30 * time.Minute) // Reuse connections for up to 30 minutes
	db.SetConnMaxIdleTime(5 * time.Minute)  // Close idle connections after 5 minutes

	// Verify connection
	err = db.Ping()
	if err != nil {
		log.Printf("Database connection error ❌: %v", err)
		return nil, err
	}

	log.Println("Database connection established ✅")
	return db, nil
}
