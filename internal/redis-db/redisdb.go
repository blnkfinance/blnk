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

package redis_db

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

// Redis struct holds the Redis client and addresses of Redis instances.
// It supports both single-instance Redis connections and Redis Cluster setups.
type Redis struct {
	addresses []string              // Redis server addresses
	client    redis.UniversalClient // Redis universal client (works for both single and clustered Redis)
}

// NewRedisClient creates a new Redis client connection based on the provided list of addresses.
// It automatically detects if the connection is for a single Redis instance or a Redis Cluster.
//
// Parameters:
// - addresses []string: A list of Redis addresses. For a single Redis instance, provide one address.
//
// Returns:
// - *Redis: A new Redis client wrapper.
// - error: An error if the provided address is invalid or connection setup fails.
func NewRedisClient(addresses []string) (*Redis, error) {
	// Ensure at least one address is provided
	if len(addresses) == 0 {
		return nil, errors.New("redis addresses list cannot be empty")
	}

	var client redis.UniversalClient

	// If a single address is provided, use it to create a standalone Redis client
	if len(addresses) == 1 {
		opts, err := redis.ParseURL(addresses[0])
		if err != nil {
			return nil, err
		}

		client = redis.NewClient(opts)
	} else {
		// For multiple addresses, create a Redis Cluster client
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: addresses,
		})
	}

	return &Redis{addresses: addresses, client: client}, nil
}

// Client returns the Redis universal client.
// It can be used directly for Redis operations like Get, Set, or Publish.
//
// Returns:
// - redis.UniversalClient: The universal Redis client, which supports both standalone and clustered Redis instances.
func (r *Redis) Client() redis.UniversalClient {
	return r.client
}

// MakeRedisClient returns the Redis client interface, allowing compatibility with other packages or tools.
//
// Returns:
// - interface{}: The Redis client interface.
func (r *Redis) MakeRedisClient() interface{} {
	return r.client
}
