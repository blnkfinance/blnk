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
	err = createLedgerTable(db)
	if err != nil {
		return nil, err
	}
	err = createIdentityTable(db)
	if err != nil {
		return nil, err
	}
	err = createBalanceTable(db)
	if err != nil {
		return nil, err
	}
	err = createAccountTable(db)
	if err != nil {
		return nil, err
	}
	err = createBalanceMonitorTable(db)
	if err != nil {
		return nil, err
	}
	err = createTransactionTable(db)
	if err != nil {
		return nil, err
	}
	err = createEventMapperTable(db)
	if err != nil {
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

// createTransactionTable creates a PostgreSQL table for the Transaction struct
func createTransactionTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			id SERIAL PRIMARY KEY,
			transaction_id TEXT NOT NULL UNIQUE,
			tag TEXT,
			description TEXT,
			payment_method TEXT,
			reference TEXT,
			amount BIGINT,
			currency TEXT,
			drcr TEXT,
			status TEXT,
			ledger_id TEXT NOT NULL REFERENCES ledgers(ledger_id),
			balance_id TEXT NOT NULL REFERENCES balances(balance_id),
			credit_balance_before BIGINT,
			debit_balance_before BIGINT,
			credit_balance_after BIGINT,
			debit_balance_after BIGINT,
			balance_before BIGINT,
			balance_after BIGINT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		    scheduled_for TIMESTAMP,
			meta_data JSONB
		)
	`)
	log.Println(err)
	return err
}

// createLedgerTable creates a PostgreSQL table for the Ledger struct
func createLedgerTable(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS ledgers (
		id SERIAL PRIMARY KEY,
		name TEXT,
		ledger_id TEXT NOT NULL UNIQUE,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		meta_data JSONB
	)
`)
	log.Println(err)
	return err
}

// createBalanceTable creates a PostgreSQL table for the Balance struct
func createBalanceTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS balances (
			id SERIAL PRIMARY KEY,
			balance_id TEXT NOT NULL UNIQUE,
			balance BIGINT NOT NULL,
			credit_balance BIGINT NOT NULL,
			debit_balance BIGINT NOT NULL,
			currency TEXT NOT NULL,
			currency_multiplier BIGINT NOT NULL,
			ledger_id TEXT NOT NULL REFERENCES ledgers(ledger_id),
			identity_id TEXT REFERENCES identity(identity_id),
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			modification_ref TEXT,
			meta_data JSONB
		)
	`)
	log.Println(err)
	return err
}

// createAccountTable creates a PostgreSQL table for the Account struct
func createAccountTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS accounts (
			id SERIAL PRIMARY KEY,
			account_id TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			number TEXT NOT NULL UNIQUE,
			bank_name TEXT NOT NULL,
			currency TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		    ledger_id TEXT NOT NULL REFERENCES ledgers(ledger_id),
		    identity_id TEXT NOT NULL REFERENCES identity(identity_id),
			balance_id TEXT NOT NULL REFERENCES balances(balance_id),
			meta_data JSONB
		)
	`)
	if err != nil {
		log.Printf("Error creating accounts table: %v", err)
	}
	return err
}

// createBalanceMonitorTable creates a PostgreSQL table for the BalanceMonitor struct
func createBalanceMonitorTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS balance_monitors (
		    id SERIAL PRIMARY KEY,
			monitor_id TEXT NOT NULL UNIQUE,
			balance_id TEXT NOT NULL REFERENCES balances(balance_id),
			field TEXT NOT NULL CHECK (field IN ('debit_balance', 'credit_balance', 'balance')),
			operator TEXT NOT NULL CHECK (operator IN ('>', '<', '>=', '<=', '=')),
			value BIGINT NOT NULL,
			description TEXT,
			call_back_url TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	log.Println(err)
	return err
}

func createIdentityTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS identity (
			id SERIAL PRIMARY KEY,
			identity_id TEXT NOT NULL UNIQUE,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			other_names TEXT,
			gender TEXT,
			dob DATE,
			email_address TEXT,
			phone_number TEXT,
			nationality TEXT,
			street TEXT,
			country TEXT,
			state TEXT,
			organization_name TEXT,
			category TEXT,
			identity_type TEXT,
			post_code TEXT,
			city TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			meta_data JSONB
		)
	`)
	log.Println(err)
	return err
}

// createEventMapperTable creates a PostgreSQL table for the EventMapper struct
func createEventMapperTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS event_mappers (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			mapper_id TEXT NOT NULL UNIQUE,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			mapping_instruction JSONB NOT NULL
		)
	`)
	log.Println(err)
	return err
}
