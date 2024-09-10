/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
