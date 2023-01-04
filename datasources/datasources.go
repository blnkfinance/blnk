package datasources

import (
	"log"

	blnk "github.com/jerry-enebeli/blnk"

	"github.com/jerry-enebeli/blnk/config"
)

type DataSource interface {
	CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error)
	CreateBalance(balance blnk.Balance) (blnk.Balance, error)
	RecordTransaction(transaction blnk.Transaction) (blnk.Transaction, error)
	GetLedger(LedgerID string) (blnk.Ledger, error)
	GetBalance(BalanceID string) (blnk.Balance, error)
	GetTransaction(TransactionID string) (blnk.Transaction, error)
	GetTransactionByRef(reference string) (blnk.Transaction, error)
	UpdateBalance(balanceID string, update blnk.Balance) (blnk.Balance, error)
}

func NewDataSource(configuration *config.Configuration) DataSource {
	switch configuration.DataSource.Name {
	case "MONGO":
		return newMongoDataSource(configuration.DataSource.DNS)
	case "MYSQL":
		return newRelationalDataSource("mysql", configuration.DataSource.DNS)
	case "POSTGRES":
		return newRelationalDataSource("postgres", configuration.DataSource.DNS)
	default:
		log.Println("datasource not supported. Please use either MONGO, POSTGRES, MYSQL or REDIS")
	}
	return nil
}
