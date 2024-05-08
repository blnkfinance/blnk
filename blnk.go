package blnk

import (
	"context"
	"embed"
	"fmt"

	"github.com/typesense/typesense-go/typesense/api"

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
	redisClient, err := redis_db.NewRedisClient([]string{fmt.Sprintf("redis://%s", configuration.Redis.Dns)})
	if err != nil {
		return nil, err
	}
	bt := NewBalanceTracker()
	newQueue := NewQueue(configuration)

	newSearch := NewTypesenseClient("blnk-api-key", []string{configuration.TypeSense.Dns})
	newBlnk := &Blnk{datasource: db, bt: bt, queue: newQueue, redis: redisClient.Client(), search: newSearch}
	return newBlnk, nil
}

func (l *Blnk) Search(collection string, query *api.SearchCollectionParams) (interface{}, error) {
	return l.search.Search(context.Background(), collection, query)
}
