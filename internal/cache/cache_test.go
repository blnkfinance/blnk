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
	"fmt"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testKey gives each test its own key. The cache is a shared Redis, so fixed
// keys made these tests depend on execution order and on what an earlier run
// left behind.
func testKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("cachetest:%s:%d", t.Name(), time.Now().UnixNano())
}

// newTestCache configures the package and returns a cache. Every test needs
// this: config.MockConfig validates before it stores, so configuring with an
// empty Configuration is a no-op, and a test that did that only worked when
// another test had already left a usable config in the global store.
func newTestCache(t *testing.T) Cache {
	t.Helper()

	config.ConfigStore.Store(&config.Configuration{
		Redis:              config.RedisConfig{Dns: "localhost:6379"},
		Queue:              config.QueueConfig{WebhookQueue: "webhook_queue", NumberOfQueues: 1},
		Server:             config.ServerConfig{SecretKey: "some-secret"},
		TokenizationSecret: "12345678901234567890123456789012",
	})

	cache, err := NewCache()
	require.NoError(t, err)
	require.NotNil(t, cache)
	return cache
}

func TestSet(t *testing.T) {
	ctx := context.Background()
	mockCache := newTestCache(t)

	key := testKey(t)
	value := "testValue"

	// Test setting a value
	err := mockCache.Set(ctx, key, value, 10*time.Minute)
	assert.NoError(t, err)

	// Test setting a value with zero TTL (should fail or behave differently)
	err = mockCache.Set(ctx, key, value, 0)
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	mockCache := newTestCache(t)

	key := testKey(t)
	setValue := map[string]string{"hello": "world"}
	err := mockCache.Set(ctx, key, setValue, 10*time.Minute)
	assert.NoError(t, err)

	// Test getting an existing value
	var getValue map[string]string
	err = mockCache.Get(ctx, key, &getValue)
	assert.NoError(t, err)
	assert.Equal(t, setValue, getValue)

	var getValue1 map[string]string
	// Test getting a non-existent key
	err = mockCache.Get(ctx, testKey(t)+":absent", &getValue1)
	assert.NoError(t, err) // Assuming Get returns no error for non-existent keys
	assert.Empty(t, getValue1)
}

func TestGetNonExistentKey(t *testing.T) {
	ctx := context.Background()
	mockCache := newTestCache(t)

	var getValue map[string]string
	err := mockCache.Get(ctx, testKey(t)+":absent", &getValue)
	assert.NoError(t, err) // Assuming Get returns no error for non-existent keys
	assert.Empty(t, getValue)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	mockCache := newTestCache(t)

	key := testKey(t)
	value := "testValue"
	err := mockCache.Set(ctx, key, value, 10*time.Minute)
	if err != nil {
		return
	}

	// Test deleting an existing key
	err = mockCache.Delete(ctx, key)
	assert.NoError(t, err)

	// Verify deletion
	var getValue string
	err = mockCache.Get(ctx, key, &getValue)
	assert.NoError(t, err)
	assert.Empty(t, getValue)

	// Test deleting a non-existent key
	err = mockCache.Delete(ctx, testKey(t)+":absent")
	assert.NoError(t, err)
}
