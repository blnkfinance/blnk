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

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/internal/cache"
)

// Declare a package-level variable to hold the singleton instance.
// Ensure the instance is not accessible outside the package.
var instance *Datasource
var once sync.Once

type Datasource struct {
	Conn  *sql.DB
	Cache cache.Cache
}

func NewDataSource(configuration *config.Configuration) (IDataSource, error) {
	con, err := GetDBConnection(configuration)
	if err != nil {
		return nil, err
	}
	// Set the default schema for this connection.
	if _, err := con.Conn.Exec("SET search_path TO blnk"); err != nil {
		log.Fatal(err)
	}
	return con, nil
}

// GetDBConnection provides a global access point to the instance and initializes it if it's not already.
func GetDBConnection(configuration *config.Configuration) (*Datasource, error) {
	var err error
	once.Do(func() {
		con, errConn := ConnectDB(configuration.DataSource.Dns)
		if errConn != nil {
			err = errConn
			return
		}
		cache, err := cache.NewCache()
		if err != nil {
			log.Printf("Error creating cache: %v", err)
			return
		}
		instance = &Datasource{Conn: con, Cache: cache} // or Cache: newCache if cache is used
	})
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func ConnectDB(dns string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		log.Printf("database Connection error ❌: %v", err)
		return nil, err
	}

	return db, nil
}
