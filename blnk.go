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
	"net/http"
	"time"

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
	httpClient  *http.Client
	Hooks       hooks.HookManager
}

const (
	GeneralLedgerID = "general_ledger_id"
)

//go:embed sql/*.sql
var SQLFiles embed.FS

// initializeRedisClients sets up both the Redis client and Asynq client
func initializeRedisClients(config *config.Configuration) (redis.UniversalClient, *asynq.Client, error) {
	redisClient, err := redis_db.NewRedisClient([]string{config.Redis.Dns}, config.Redis.SkipTLSVerify)
	if err != nil {
		return nil, nil, err
	}

	redisOption, err := redis_db.ParseRedisURL(config.Redis.Dns)
	if err != nil {
		return nil, nil, err
	}

	asynqClient := asynq.NewClient(asynq.RedisClientOpt{
		Addr:      redisOption.Addr,
		Password:  redisOption.Password,
		DB:        redisOption.DB,
		TLSConfig: redisOption.TLSConfig,
	})

	return redisClient.Client(), asynqClient, nil
}

// initializeTokenizationService creates and configures the tokenization service
func initializeTokenizationService(config *config.Configuration) *tokenization.TokenizationService {
	tokenizationKey := []byte(config.TokenizationSecret)
	if len(tokenizationKey) != 32 {
		tokenizationKey = make([]byte, 32)
		copy(tokenizationKey, config.TokenizationSecret)
	}
	return tokenization.NewTokenizationService(tokenizationKey)
}

// initializeHTTPClient creates and configures the HTTP client for webhook requests
func initializeHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

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

	redisClient, asynqClient, err := initializeRedisClients(configuration)
	if err != nil {
		return nil, err
	}

	bt := NewBalanceTracker()
	newQueue := NewQueue(configuration)
	newSearch := NewTypesenseClient(configuration.TypeSenseKey, []string{configuration.TypeSense.Dns})
	hookManager := hooks.NewHookManager(redisClient)
	tokenizer := initializeTokenizationService(configuration)
	httpClient := initializeHTTPClient()

	return &Blnk{
		datasource:  db,
		bt:          bt,
		queue:       newQueue,
		redis:       redisClient,
		asynqClient: asynqClient,
		search:      newSearch,
		tokenizer:   tokenizer,
		httpClient:  httpClient,
		Hooks:       hookManager,
	}, nil
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
