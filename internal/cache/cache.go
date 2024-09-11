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
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/cache/v9"
	"github.com/jerry-enebeli/blnk/config"
	redis_db "github.com/jerry-enebeli/blnk/internal/redis-db"
)

// Cache interface provides the basic operations for a cache system.
// It includes methods for setting, getting, and deleting cached data.
type Cache interface {
	// Set stores a value in the cache with a specified time-to-live (TTL).
	// Parameters:
	// - ctx: The context for managing the request lifecycle.
	// - key: The cache key under which the value will be stored.
	// - value: The value to be stored in the cache.
	// - ttl: The duration the value should be retained in the cache.
	// Returns an error if the operation fails.
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error

	// Get retrieves a value from the cache using a given key.
	// Parameters:
	// - ctx: The context for managing the request lifecycle.
	// - key: The cache key to fetch the value.
	// - data: The variable to store the fetched data.
	// Returns an error if the key does not exist or retrieval fails.
	Get(ctx context.Context, key string, data interface{}) error

	// Delete removes a value from the cache based on the provided key.
	// Parameters:
	// - ctx: The context for managing the request lifecycle.
	// - key: The cache key to be deleted.
	// Returns an error if the key cannot be deleted.
	Delete(ctx context.Context, key string) error
}

// RedisCache implements the Cache interface, using Redis as the underlying cache store.
// It leverages both Redis and local in-memory caching for efficient lookups.
type RedisCache struct {
	cache *cache.Cache
}

// NewCache creates a new instance of RedisCache by establishing a connection to Redis.
// It fetches the configuration, initializes Redis, and returns a Cache instance.
// Returns an error if the configuration or Redis initialization fails.
func NewCache() (Cache, error) {
	// Fetch configuration settings
	cfg, err := config.Fetch()
	if err != nil {
		return nil, err
	}

	// Initialize Redis cache with the configured Redis DNS
	ca, err := newRedisCache([]string{fmt.Sprintf("redis://%s", cfg.Redis.Dns)})
	if err != nil {
		return nil, err
	}
	return ca, nil
}

// cacheSize defines the size of the local cache (in number of entries) used alongside Redis.
const cacheSize = 128000

// newRedisCache sets up a Redis-backed cache with local caching (TinyLFU).
// Parameters:
// - addresses: The Redis server addresses to connect to.
// Returns a RedisCache instance and an error if the connection fails.
func newRedisCache(addresses []string) (*RedisCache, error) {
	// Initialize the Redis client using the provided addresses
	client, err := redis_db.NewRedisClient(addresses)
	if err != nil {
		return nil, err
	}

	// Create a cache instance with Redis and local cache support
	c := cache.New(&cache.Options{
		Redis:      client.Client(),
		LocalCache: cache.NewTinyLFU(cacheSize, 1*time.Minute), // Local cache uses TinyLFU eviction policy
	})

	return &RedisCache{cache: c}, nil
}

// Set adds a new entry to the cache with a specified key and TTL.
// Parameters:
// - ctx: The context for the operation.
// - key: The cache key under which to store the value.
// - data: The value to be cached.
// - ttl: The time-to-live duration for the cached value.
// Returns an error if the caching operation fails.
func (r *RedisCache) Set(ctx context.Context, key string, data interface{}, ttl time.Duration) error {
	return r.cache.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: data,
		TTL:   ttl,
	})
}

// Get retrieves an entry from the cache based on the provided key.
// Parameters:
// - ctx: The context for the operation.
// - key: The cache key to retrieve.
// - data: The variable to store the retrieved data.
// Returns an error if the key does not exist or if the retrieval fails.
func (r *RedisCache) Get(ctx context.Context, key string, data interface{}) error {
	err := r.cache.Get(ctx, key, &data)
	if errors.Is(err, cache.ErrCacheMiss) {
		return nil // Return nil if the cache key was not found (cache miss)
	}

	return err
}

// Delete removes an entry from the cache based on the provided key.
// Parameters:
// - ctx: The context for the operation.
// - key: The cache key to delete.
// Returns an error if the deletion fails.
func (r *RedisCache) Delete(ctx context.Context, key string) error {
	return r.cache.Delete(ctx, key)
}
