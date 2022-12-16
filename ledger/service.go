package ledger

import (
	"github.com/jerry-enebeli/saifu/config"
	"github.com/jerry-enebeli/saifu/datasources"
)

type service struct {
	datasources.DataSource
	config *config.Configuration
}

func NewLedgerService() *service {
	configuration, err := config.Fetch()
	if err != nil {
		panic(err)
		return nil
	}
	dataSource := datasources.NewDataSource(configuration)
	return &service{DataSource: dataSource, config: configuration}
}
