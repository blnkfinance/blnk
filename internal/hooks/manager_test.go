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

package hooks

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListHooks(t *testing.T) {
	newManager := func(t *testing.T) (*redisHookManager, *miniredis.Miniredis) {
		t.Helper()

		server := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() {
			require.NoError(t, client.Close())
		})

		return &redisHookManager{client: client}, server
	}

	registerHooks := func(t *testing.T, manager *redisHookManager) (*Hook, *Hook) {
		t.Helper()

		preHook := &Hook{ID: "pre-hook", Name: "Pre hook", URL: "https://example.com/pre", Type: PreTransaction}
		postHook := &Hook{ID: "post-hook", Name: "Post hook", URL: "https://example.com/post", Type: PostTransaction}
		require.NoError(t, manager.RegisterHook(context.Background(), preHook))
		require.NoError(t, manager.RegisterHook(context.Background(), postHook))

		return preHook, postHook
	}

	t.Run("empty type returns all hooks", func(t *testing.T) {
		manager, _ := newManager(t)
		preHook, postHook := registerHooks(t, manager)

		got, err := manager.ListHooks(context.Background(), HookType(""))

		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.ElementsMatch(t, []string{preHook.ID, postHook.ID}, []string{got[0].ID, got[1].ID})
	})

	t.Run("pre transaction filter", func(t *testing.T) {
		manager, _ := newManager(t)
		preHook, _ := registerHooks(t, manager)

		got, err := manager.ListHooks(context.Background(), PreTransaction)

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, preHook.ID, got[0].ID)
	})

	t.Run("post transaction filter", func(t *testing.T) {
		manager, _ := newManager(t)
		_, postHook := registerHooks(t, manager)

		got, err := manager.ListHooks(context.Background(), PostTransaction)

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, postHook.ID, got[0].ID)
	})

	t.Run("empty registry", func(t *testing.T) {
		manager, _ := newManager(t)

		got, err := manager.ListHooks(context.Background(), HookType(""))

		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("duplicate ID is returned once", func(t *testing.T) {
		manager, server := newManager(t)
		preHook := &Hook{ID: "shared-hook", Name: "Shared hook", URL: "https://example.com/shared", Type: PreTransaction}
		require.NoError(t, manager.RegisterHook(context.Background(), preHook))
		_, err := server.SAdd(postHookKeyPrefix, preHook.ID)
		require.NoError(t, err)

		got, err := manager.ListHooks(context.Background(), HookType(""))

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, preHook.ID, got[0].ID)
	})
}
