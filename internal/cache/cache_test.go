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
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	config.MockConfig(&config.Configuration{})
	ctx := context.Background()
	mockCache, err := NewCache()
	assert.NoError(t, err)

	key := "testKey"
	value := "testValue"

	// Test setting a value
	err = mockCache.Set(ctx, key, value, 10*time.Minute)
	assert.NoError(t, err)

	// Test setting a value with zero TTL (should fail or behave differently)
	err = mockCache.Set(ctx, key, value, 0)
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	config.MockConfig(&config.Configuration{})
	ctx := context.Background()
	mockCache, err := NewCache()
	assert.NoError(t, err)

	key := "testKey"
	setValue := map[string]string{"hello": "world"}
	err = mockCache.Set(ctx, key, setValue, 10*time.Minute)
	assert.NoError(t, err)

	// Test getting an existing value
	var getValue map[string]string
	err = mockCache.Get(ctx, key, &getValue)
	assert.NoError(t, err)
	assert.Equal(t, setValue, getValue)

	var getValue1 map[string]string
	// Test getting a non-existent key
	err = mockCache.Get(ctx, "nonExistentKey", &getValue1)
	assert.NoError(t, err) // Assuming Get returns no error for non-existent keys
	assert.Empty(t, getValue1)

}

func TestGetNonExistentKey(t *testing.T) {
	config.MockConfig(&config.Configuration{})
	ctx := context.Background()
	mockCache, err := NewCache()
	assert.NoError(t, err)

	var getValue map[string]string
	err = mockCache.Get(ctx, "nonExistentKey", &getValue)
	assert.NoError(t, err) // Assuming Get returns no error for non-existent keys
	assert.Empty(t, getValue)
}

func TestDelete(t *testing.T) {
	config.MockConfig(&config.Configuration{})
	ctx := context.Background()
	mockCache, err := NewCache()
	assert.NoError(t, err)

	key := "testKey"
	value := "testValue"
	err = mockCache.Set(ctx, key, value, 10*time.Minute)
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
	err = mockCache.Delete(ctx, "nonExistentKey")
	assert.NoError(t, err)
}
