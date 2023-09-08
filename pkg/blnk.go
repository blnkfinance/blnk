package pkg

import (
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/datasources"
)

type Blnk struct {
	datasource datasources.DataSource
	config     *config.Configuration
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
	return &Blnk{datasource: db, config: configuration}, nil
}
