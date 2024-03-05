package database

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	"github.com/jerry-enebeli/blnk/cache"

	"github.com/google/uuid"

	"github.com/jerry-enebeli/blnk/config"
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
		instance = &Datasource{Conn: con, Cache: nil} // or Cache: newCache if cache is used
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

func GenerateUUIDWithSuffix(module string) string {

	// Generate a new UUID
	id := uuid.New()

	// Convert the UUID to a string
	uuidStr := id.String()

	// Add the module suffix
	idWithSuffix := fmt.Sprintf("%s_%s", module, uuidStr)

	return idWithSuffix
}
