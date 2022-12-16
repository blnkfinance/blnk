package ledger

import (
	"github.com/jerry-enebeli/saifu/config"
	"github.com/jerry-enebeli/saifu/datasources"
)

type Service struct {
	datasources.DataSource
	config *config.Configuration
}

func NewLedgerService() *Service {
	configuration, err := config.Fetch()
	if err != nil {
		panic(err)
		return nil
	}
	dataSource := datasources.NewDataSource(configuration)
	return &Service{DataSource: dataSource, config: configuration}
}
