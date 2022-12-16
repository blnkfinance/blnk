package datasources

import (
	"log"

	"github.com/jerry-enebeli/saifu/model"

	"github.com/jerry-enebeli/saifu/config"
)

type DataSource interface {
	CreateWallet() (model.Wallet, error)
	GetWalletByIdentifier(identifier model.WalletIdentifier) (model.Wallet, error)
	GetAllWallets(filter model.WalletFilter) ([]model.Wallet, error)
	UpdateWallet(identifier model.WalletIdentifier, wallet model.Wallet) ([]model.Wallet, error)
	DeleteWallet(identifier model.WalletIdentifier) ([]model.Wallet, error)
}

func NewDataSource(configuration *config.Configuration) DataSource {
	switch configuration.DataSource.Name {
	case "MONGO":
		return newMongoDataSource(configuration.DataSource.DNS)
	default:
		log.Println("datasource not supported. Please use either MONGO, POSTGRES, MYSQL or REDIS")
	}
	return nil
}
