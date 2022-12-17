package datasources

import (
	"log"

	"github.com/jerry-enebeli/saifu"

	"github.com/jerry-enebeli/saifu/config"
)

type DataSource interface {
	CreateLedger(ledger saifu.Ledger) (saifu.Ledger, error)
	CreateBalance(balance saifu.Balance) (saifu.Balance, error)
	RecordTransaction(transaction saifu.Transaction) (saifu.Transaction, error)
	GetLedger(LedgerID string) (saifu.Ledger, error)
	GetBalance(BalanceID string) (saifu.Balance, error)
	GetTransaction(TransactionID string) (saifu.Transaction, error)
	GetTransactionByRef(reference string) (saifu.Transaction, error)
	UpdateBalance(balanceID string, update saifu.Balance) (saifu.Balance, error)
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
