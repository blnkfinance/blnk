package datasources

import (
	"log"

	"github.com/jerry-enebeli/saifu/config"
)

type DataSource interface {
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
