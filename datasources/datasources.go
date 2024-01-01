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
	identity
	balanceMonitor
	eventMapper
	account
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
	GetBalanceByID(id string, include []string) (*blnk.Balance, error)
	GetAllBalances() ([]blnk.Balance, error)
	UpdateBalance(balance *blnk.Balance) error
}

type account interface {
	CreateAccount(account blnk.Account) (blnk.Account, error)
	GetAccountByID(id string) (*blnk.Account, error)
	GetAllAccounts() ([]blnk.Account, error)
	GetAccountByNumber(number string) (*blnk.Account, error)
	UpdateAccount(account *blnk.Account) error
	DeleteAccount(id string) error
}

type balanceMonitor interface {
	CreateMonitor(monitor blnk.BalanceMonitor) (blnk.BalanceMonitor, error)
	GetMonitorByID(id string) (*blnk.BalanceMonitor, error)
	GetAllMonitors() ([]blnk.BalanceMonitor, error)
	GetBalanceMonitors(balanceID string) ([]blnk.BalanceMonitor, error)
	UpdateMonitor(monitor *blnk.BalanceMonitor) error
	DeleteMonitor(id string) error
}

type identity interface {
	CreateIdentity(identity blnk.Identity) (blnk.Identity, error)
	GetIdentityByID(id string) (*blnk.Identity, error)
	GetAllIdentities() ([]blnk.Identity, error)
	UpdateIdentity(identity *blnk.Identity) error
	DeleteIdentity(id string) error
}

type eventMapper interface {
	CreateEventMapper(mapper blnk.EventMapper) (blnk.EventMapper, error)
	GetAllEventMappers() ([]blnk.EventMapper, error)
	GetEventMapperByID(id string) (*blnk.EventMapper, error)
	UpdateEventMapper(mapper blnk.EventMapper) error
	DeleteEventMapper(id string) error
}

type datasource struct {
	conn *sql.DB
}

func NewDataSource(configuration *config.Configuration) (DataSource, error) {
	con, err := connectDB(configuration.DataSource.Dns)
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
