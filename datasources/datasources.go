package datasources

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/google/uuid"

	blnk "github.com/jerry-enebeli/blnk"

	"github.com/jerry-enebeli/blnk/config"
)

type DataSource interface {
	transaction
	ledger
	balance
}

type transaction interface {
	RecordTransaction(txn blnk.Transaction) (blnk.Transaction, error)
	GetTransaction(id string) (blnk.Transaction, error)
	GetTransactionByRef(reference string) (blnk.Transaction, error)
	UpdateTransactionStatus(id string, status string) error
	GroupTransactionsByCurrency() (map[string]struct {
		TotalAmount int64 `json:"total_amount"`
	}, error)
	GetAllTransactions() ([]blnk.Transaction, error)
	GetScheduledTransactions() ([]blnk.Transaction, error)
}

type ledger interface {
	CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error)
	GetAllLedgers() ([]blnk.Ledger, error)
	GetLedgerByID(id string) (*blnk.Ledger, error)
}

type balance interface {
	CreateBalance(balance blnk.Balance) (blnk.Balance, error)
	GetBalanceByID(id string) (*blnk.Balance, error)
	GetAllBalances() ([]blnk.Balance, error)
	UpdateBalance(balance *blnk.Balance) error
}

type datasource struct {
	conn *sql.DB
}

func NewDataSource(configuration *config.Configuration) (DataSource, error) {
	con, err := connectDB(configuration.DataSource.DNS)
	if err != nil {
		return nil, err
	}
	return &datasource{conn: con}, nil
}

func connectDB(dns string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		log.Printf("database connection error ❌: %v", err)
		return nil, err
	}
	err = createLedgerTable(db)
	if err != nil {
		return nil, err
	}
	err = createBalanceTable(db)
	if err != nil {
		return nil, err
	}
	err = createTransactionTable(db)
	if err != nil {
		return nil, err
	}
	err = createCustomerTable(db)
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
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			modification_ref TEXT,
			meta_data JSONB
		)
	`)
	log.Println(err)
	return err
}

// createCustomerTable creates a PostgreSQL table for the Customer struct
func createCustomerTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS customers (
			id SERIAL PRIMARY KEY,
			customer_id TEXT NOT NULL UNIQUE,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			other_names TEXT,
			gender TEXT,
			dob DATE,
			email_address TEXT,
			phone_number TEXT,
			nationality TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			meta_data JSONB
		)
	`)
	log.Println(err)
	return err
}
