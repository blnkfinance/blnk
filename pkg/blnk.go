package pkg

import (
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/datasources"
)

type Blnk struct {
	datasource datasources.DataSource
	config     *config.Configuration
}

func NewBlnk() *Blnk {
	configuration, err := config.Fetch()
	if err != nil {
		panic(err)
	}
	return &Blnk{datasource: datasources.NewDataSource(configuration), config: configuration}
}
