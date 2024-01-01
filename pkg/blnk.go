package pkg

import (
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/datasources"
)

type Blnk struct {
	datasource datasources.DataSource
	config     *config.Configuration
	bt         *BalanceTracker
}

func NewBlnk() (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}
	db, err := datasources.NewDataSource(configuration)
	if err != nil {
		return nil, err
	}

	bt := NewBalanceTracker()
	newBlnk := &Blnk{datasource: db, config: configuration, bt: bt}
	//go newBlnk.ProcessTransactionFromQueue()
	newBlnk.GetScheduledTransaction()
	return newBlnk, nil
}
