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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis struct holds the Redis client and addresses of Redis instances.
// It supports both single-instance Redis connections and Redis Cluster setups.
type Redis struct {
	addresses []string              // Redis server addresses
	client    redis.UniversalClient // Redis universal client (works for both single and clustered Redis)
}

// parseRedisURL parses a Redis URL into Redis options, handling various URL formats
// including Azure Redis Cache URLs with special characters in passwords.
func ParseRedisURL(rawURL string, skipTLSVerify bool) (*redis.Options, error) {
	// Don't modify docker-style addresses (e.g. redis:6379)
	if strings.Count(rawURL, ":") == 1 && !strings.Contains(rawURL, "@") && !strings.Contains(rawURL, "//") {
		return &redis.Options{
			Addr: rawURL,
		}, nil
	}

	// Handle URLs that have redis:// prefix with password but no colon
	if strings.HasPrefix(rawURL, "redis://") && strings.Contains(rawURL, "@") {
		parts := strings.Split(strings.TrimPrefix(rawURL, "redis://"), "@")
		if len(parts) == 2 {
			authParts := strings.Split(parts[0], ":")
			if len(authParts) == 1 {
				// No username, just password - keep original logic
				rawURL = fmt.Sprintf("redis://:%s@%s", parts[0], parts[1])
			}
		}
	}

	// Parse the URL
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		// If ParseURL fails, try manual parsing
		host := rawURL
		var password string

		// Extract password if present
		if strings.Contains(rawURL, "@") {
			parts := strings.Split(rawURL, "@")
			if len(parts) == 2 {
				password = strings.TrimPrefix(parts[0], "redis://")
				host = parts[1]
			}
		}

		opts = &redis.Options{
			Addr:     host,
			Password: password,
			DB:       0,
		}

		// Enable TLS for Azure Redis
		if strings.Contains(host, "redis.cache.windows.net") {
			opts.TLSConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}
	}

	// // Apply TLS skip verify if configured and TLS is enabled
	if opts.TLSConfig != nil && skipTLSVerify {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: skipTLSVerify,
		}
	}

	return opts, nil
}

// NewRedisClient creates a new Redis client connection based on the provided list of addresses.
// It automatically detects if the connection is for a single Redis instance or a Redis Cluster.
//
// Parameters:
// - addresses []string: A list of Redis addresses. For a single Redis instance, provide one address.
// - skipTLSVerify bool: Whether to skip TLS certificate verification
//
// Returns:
// - *Redis: A new Redis client wrapper.
// - error: An error if the provided address is invalid or connection setup fails.
func NewRedisClient(addresses []string, skipTLSVerify bool) (*Redis, error) {
	// Ensure at least one address is provided
	if len(addresses) == 0 {
		return nil, errors.New("redis addresses list cannot be empty")
	}

	var client redis.UniversalClient

	// If a single address is provided, use it to create a standalone Redis client
	if len(addresses) == 1 {
		opts, err := ParseRedisURL(addresses[0], skipTLSVerify)
		if err != nil {
			return nil, err
		}

		client = redis.NewClient(opts)
	} else {
		// For multiple addresses, create a Redis Cluster client
		// Parse each URL for cluster setup
		var clusterAddrs []string
		var password string
		useTLS := false

		for _, addr := range addresses {
			opts, err := ParseRedisURL(addr, skipTLSVerify)
			if err != nil {
				return nil, err
			}
			clusterAddrs = append(clusterAddrs, opts.Addr)

			// Use the password from the first URL that has one
			if password == "" && opts.Password != "" {
				password = opts.Password
			}

			// Enable TLS if any URL requires it
			if opts.TLSConfig != nil {
				useTLS = true
			}
		}
		var tlsConfig *tls.Config
		if useTLS {
			tlsConfig = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: skipTLSVerify,
			}
		}

		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:     clusterAddrs,
			Password:  password,
			TLSConfig: tlsConfig,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
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
