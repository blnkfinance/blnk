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

	"github.com/hibiken/asynq"
	"github.com/typesense/typesense-go/typesense/api"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/internal/hooks"
	redis_db "github.com/jerry-enebeli/blnk/internal/redis-db"
	"github.com/jerry-enebeli/blnk/internal/tokenization"

	"github.com/jerry-enebeli/blnk/model"
	"github.com/redis/go-redis/v9"
)

// Blnk represents the main struct for the Blnk application.
type Blnk struct {
	queue       *Queue
	search      *TypesenseClient
	redis       redis.UniversalClient
	asynqClient *asynq.Client
	datasource  database.IDataSource
	bt          *model.BalanceTracker
	tokenizer   *tokenization.TokenizationService
	Hooks       hooks.HookManager
}

const (
	GeneralLedgerID = "general_ledger_id"
)

//go:embed sql/*.sql
var SQLFiles embed.FS

// NewBlnk initializes a new instance of Blnk with the provided database datasource.
// It fetches the configuration, initializes Redis client, balance tracker, queue, and search client.
//
// Parameters:
// - db database.IDataSource: The datasource for database operations.
//
// Returns:
// - *Blnk: A pointer to the newly created Blnk instance.
// - error: An error if any of the initialization steps fail.
func NewBlnk(db database.IDataSource) (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}
	redisClient, err := redis_db.NewRedisClient([]string{configuration.Redis.Dns})
	if err != nil {
		return nil, err
	}

	redisOption, err := redis_db.ParseRedisURL(configuration.Redis.Dns)
	if err != nil {
		return nil, err
	}
	asynqClient := asynq.NewClient(asynq.RedisClientOpt{
		Addr:      redisOption.Addr,
		Password:  redisOption.Password,
		DB:        redisOption.DB,
		TLSConfig: redisOption.TLSConfig,
	})

	bt := NewBalanceTracker()
	newQueue := NewQueue(configuration)
	newSearch := NewTypesenseClient(configuration.TypeSenseKey, []string{configuration.TypeSense.Dns})
	hookManager := hooks.NewHookManager(redisClient.Client())

	// Use the tokenization secret from configuration
	tokenizationKey := []byte(configuration.TokenizationSecret)
	if len(tokenizationKey) != 32 {
		// Ensure the key is exactly 32 bytes for AES-256
		tokenizationKey = make([]byte, 32)
		copy(tokenizationKey, configuration.TokenizationSecret)
	}

	newBlnk := &Blnk{
		datasource:  db,
		bt:          bt,
		queue:       newQueue,
		redis:       redisClient.Client(),
		asynqClient: asynqClient,
		search:      newSearch,
		tokenizer:   tokenization.NewTokenizationService(tokenizationKey),
		Hooks:       hookManager,
	}
	return newBlnk, nil
}

// Search performs a search on the specified collection using the provided query parameters.
//
// Parameters:
// - collection string: The name of the collection to search.
// - query *api.SearchCollectionParams: The search query parameters.
//
// Returns:
// - interface{}: The search results.
// - error: An error if the search operation fails.
func (l *Blnk) Search(collection string, query *api.SearchCollectionParams) (interface{}, error) {
	return l.search.Search(context.Background(), collection, query)
}

// MultiSearch performs a multi-search operation across collections.
func (l *Blnk) MultiSearch(searchParams *api.MultiSearchSearchesParameter) (*api.MultiSearchResult, error) {
	return l.search.MultiSearch(context.Background(), *searchParams)
}

// Close properly closes all connections and resources used by the Blnk instance.
func (b *Blnk) Close() error {
	if b.asynqClient != nil {
		return b.asynqClient.Close()
	}
	return nil
}
