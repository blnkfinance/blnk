package ledger

import (
	"github.com/jerry-enebeli/saifu/config"
	"github.com/jerry-enebeli/saifu/datasources"
)

type ledger struct {
	datasources.DataSource
	config *config.Configuration
}

func NewLedger() *ledger {
	configuration, err := config.Fetch()
	if err != nil {
		panic(err)
		return nil
	}
	dataSource := datasources.NewDataSource(configuration)
	return &ledger{DataSource: dataSource, config: configuration}
}
