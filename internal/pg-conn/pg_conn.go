package pgconn

import (
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/internal/cache"
	_ "github.com/lib/pq" // Import the postgres driver
)

// Declare a package-level variable to hold the singleton instance.
var instance *Datasource
var once sync.Once

type Datasource struct {
	Conn  *sql.DB
	Cache cache.Cache
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
