package datasources

import (
	"database/sql"
	"log"

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
}

type ledger interface {
	CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error)
	GetAllLedgers() ([]blnk.Ledger, error)
	GetLedgerByID(id int64) (*blnk.Ledger, error)
}

type balance interface {
	CreateBalance(balance blnk.Balance) (blnk.Balance, error)
	GetBalanceByID(id int64) (*blnk.Balance, error)
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
	return db, nil
}

// createTransactionTable creates a PostgreSQL table for the Transaction struct
func createTransactionTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			id SERIAL PRIMARY KEY,
			tag TEXT,
			reference TEXT,
			amount BIGINT,
			currency TEXT,
			drcr TEXT,
			status TEXT,
			ledger_id INTEGER NOT NULL REFERENCES ledgers(id),
			balance_id INTEGER NOT NULL REFERENCES balances(id),
			credit_balance_before BIGINT,
			debit_balance_before BIGINT,
			credit_balance_after BIGINT,
			debit_balance_after BIGINT,
			balance_before BIGINT,
			balance_after BIGINT,
			created TIMESTAMP NOT NULL DEFAULT NOW(),
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
			created TIMESTAMP NOT NULL DEFAULT NOW(),
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
			balance BIGINT NOT NULL,
			credit_balance BIGINT NOT NULL,
			debit_balance BIGINT NOT NULL,
			currency TEXT NOT NULL,
			currency_multiplier BIGINT NOT NULL,
			ledger_id INTEGER NOT NULL REFERENCES ledgers(id),
			created TIMESTAMP NOT NULL DEFAULT NOW(),
			modification_ref TEXT,
			meta_data JSONB
		)
	`)
	log.Println(err)
	return err
}
