package blnk

import (
	"context"
	"embed"
	"fmt"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	redis_db "github.com/jerry-enebeli/blnk/internal/redis-db"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/redis/go-redis/v9"
)

type Blnk struct {
	queue      *Queue
	search     *TypesenseClient
	redis      redis.UniversalClient
	datasource database.IDataSource
	bt         *model.BalanceTracker
}

const (
	GeneralLedgerID = "general_ledger_id"
)

//go:embed sql/*.sql
var SQLFiles embed.FS

func NewBlnk(db database.IDataSource) (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}
	redis, err := redis_db.NewRedisClient([]string{fmt.Sprintf("redis://%s", configuration.Redis.Dns)})
	if err != nil {
		return nil, err
	}
	bt := NewBalanceTracker()
	newQueue := NewQueue(configuration)
	newSearch := NewTypesenseClient("blnk-api-key", []string{"http://typesense:8108"})
	newBlnk := &Blnk{datasource: db, bt: bt, queue: newQueue, redis: redis.Client(), search: newSearch}
	return newBlnk, nil
}

func (b Blnk) Search(collection string, query map[string]interface{}) (interface{}, error) {
	return b.search.Search(context.Background(), collection, query)
}
