package pgconn

import (
	"database/sql"
	"time"

	"github.com/blnkfinance/blnk/config"
	_ "github.com/lib/pq" // Import the postgres driver
	"github.com/sirupsen/logrus"
)

const (
	maxConnRetries    = 5
	initialRetryDelay = 1 * time.Second
)

// ConnectDB establishes a database connection with pooling.
// It retries the initial ping up to maxConnRetries times with exponential backoff
// to handle transient network issues during startup.
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

	// Verify connection with retry
	delay := initialRetryDelay
	for attempt := 1; attempt <= maxConnRetries; attempt++ {
		err = db.Ping()
		if err == nil {
			logrus.Info("database connection established")
			return db, nil
		}

		logrus.WithError(err).WithField("attempt", attempt).
			Warnf("Database ping failed, retrying in %v...", delay)

		if attempt < maxConnRetries {
			time.Sleep(delay)
			delay *= 2 // exponential backoff
		}
	}

	// All retries exhausted
	_ = db.Close()
	logrus.WithError(err).Error("Database connection failed after retries")
	return nil, err
}
