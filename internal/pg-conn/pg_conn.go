package pgconn

import (
	"database/sql"
	"sync"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/cache"
	_ "github.com/lib/pq" // Import the postgres driver
	"github.com/sirupsen/logrus"
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
			logrus.WithError(errCache).Error("Error creating cache")
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
	db, err := sql.Open("postgres", dsConfig.Dns)
	if err != nil {
		return nil, err
	}

	// Apply connection pooling settings from configuration
	db.SetMaxOpenConns(dsConfig.MaxOpenConns)
	db.SetMaxIdleConns(dsConfig.MaxIdleConns)
	db.SetConnMaxLifetime(dsConfig.ConnMaxLifetime)
	db.SetConnMaxIdleTime(dsConfig.ConnMaxIdleTime)

	// Verify connection
	err = db.Ping()
	if err != nil {
		logrus.WithError(err).Error("Database connection error")
		return nil, err
	}

	logrus.Info("database connection established")
	return db, nil
}
