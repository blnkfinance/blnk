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

package redlock

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// Locker represents a distributed lock using Redis.
// The lock is identified by a unique key and value, where the value is used
// to ensure that only the lock holder can release or renew the lock.
type Locker struct {
	client redis.UniversalClient // Redis client for interacting with Redis.
	key    string                // The unique key for the lock in Redis.
	value  string                // A unique value to ensure only the holder can release/extend the lock.
}

// MultiLocker manages multiple distributed locks with deterministic ordering.
// It acquires locks in lexicographic order to prevent deadlocks when multiple
// transactions need to lock the same set of keys in different orders.
type MultiLocker struct {
	client  redis.UniversalClient
	lockers []*Locker
	keys    []string
	value   string
}

// NewLocker initializes a new Locker instance with a Redis client, a key, and a value.
// Parameters:
// - client: A Redis universal client to interact with Redis.
// - key: The unique identifier for the lock.
// - value: A unique value to associate with the lock (ensures lock ownership).
// Returns a pointer to a new Locker instance.
func NewLocker(client redis.UniversalClient, key, value string) *Locker {
	return &Locker{
		client: client,
		key:    key,
		value:  value,
	}
}

// Lock attempts to acquire the lock for the specified key with a timeout.
// If the lock is already held, it returns an error indicating the lock is unavailable.
// Parameters:
// - ctx: The context for managing the lock request lifecycle.
// - timeout: The time-to-live (TTL) for the lock.
// Returns an error if the lock is already held or if there is a Redis error.
func (l *Locker) Lock(ctx context.Context, timeout time.Duration) error {
	success, err := l.client.SetNX(ctx, l.key, l.value, timeout).Result()
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("lock for key %s is already held", l.key)
	}
	return nil
}

// Unlock releases the lock if the calling instance is the lock holder (based on the value).
// The operation is atomic, ensuring only the holder of the lock can release it.
// Parameters:
// - ctx: The context for managing the unlock request lifecycle.
// Returns an error if the unlock operation fails, either because the lock expired or
// the caller is not the lock holder.
func (l *Locker) Unlock(ctx context.Context) error {
	// Lua script ensures atomicity: checks the value and deletes the key if the value matches.
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	result, err := l.client.Eval(ctx, script, []string{l.key}, l.value).Result()
	if err != nil {
		return err
	}
	if result == int64(0) {
		return fmt.Errorf("unlock failed, either lock expired or you're not the lock holder for key %s", l.key)
	}
	return nil
}

// ExtendLock extends the TTL of the lock if the calling instance is the lock holder.
// This method ensures that the lock is renewed only by the lock holder.
// Parameters:
// - ctx: The context for managing the extension request.
// - extension: The additional time to extend the lock by.
// Returns an error if the extension fails, either because the lock expired or
// the caller is not the lock holder.
func (l *Locker) ExtendLock(ctx context.Context, extension time.Duration) error {
	// Lua script ensures atomicity: checks the value and extends the TTL if the value matches.
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
	result, err := l.client.Eval(ctx, script, []string{l.key}, l.value, fmt.Sprintf("%d", extension.Milliseconds())).Result()
	if err != nil {
		return err
	}
	if result == int64(0) {
		return fmt.Errorf("lock extension failed for key %s, either lock expired or you're not the holder", l.key)
	}
	return nil
}

// WaitLock tries to acquire the lock within a specified waiting period.
// It will attempt to acquire the lock with exponential backoff if the lock is held by another process.
// Parameters:
// - ctx: The context for managing the wait request lifecycle.
// - lockTimeout: The TTL to set for the lock when acquired.
// - waitTimeout: The maximum time to wait for the lock to become available.
// Returns an error if the lock could not be acquired within the wait timeout.
func (l *Locker) WaitLock(ctx context.Context, lockTimeout, waitTimeout time.Duration) error {
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		err := l.Lock(ctx, lockTimeout)
		if err == nil {
			return nil
		}
		// Implementing exponential backoff to avoid busy-waiting.
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return fmt.Errorf("failed to acquire lock for key %s within the wait timeout", l.key)
}

// NewMultiLocker creates a new MultiLocker instance that manages locks for multiple keys.
// Keys are deduplicated and sorted lexicographically to ensure consistent lock ordering
// across all callers, preventing deadlocks.
// Parameters:
// - client: A Redis universal client to interact with Redis.
// - keys: The keys to lock (duplicates are removed, order is normalized).
// - value: A unique value to associate with all locks (ensures lock ownership).
// Returns a pointer to a new MultiLocker instance.
func NewMultiLocker(client redis.UniversalClient, keys []string, value string) *MultiLocker {
	// Deduplicate keys
	seen := make(map[string]bool)
	uniqueKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if key != "" && !seen[key] {
			seen[key] = true
			uniqueKeys = append(uniqueKeys, key)
		}
	}

	// Sort keys lexicographically to ensure consistent ordering
	sort.Strings(uniqueKeys)

	// Create lockers for each key
	lockers := make([]*Locker, len(uniqueKeys))
	for i, key := range uniqueKeys {
		lockers[i] = NewLocker(client, key, value)
	}

	return &MultiLocker{
		client:  client,
		lockers: lockers,
		keys:    uniqueKeys,
		value:   value,
	}
}

// Lock attempts to acquire all locks in deterministic order.
// If any lock acquisition fails, all previously acquired locks are released (rollback).
// Parameters:
// - ctx: The context for managing the lock request lifecycle.
// - timeout: The time-to-live (TTL) for each lock.
// Returns an error if any lock could not be acquired.
func (m *MultiLocker) Lock(ctx context.Context, timeout time.Duration) error {
	acquiredCount := 0

	for _, locker := range m.lockers {
		err := locker.Lock(ctx, timeout)
		if err != nil {
			// Rollback: release all previously acquired locks in reverse order
			m.rollback(ctx, acquiredCount)
			return fmt.Errorf("failed to acquire lock for key %s: %w", locker.key, err)
		}
		acquiredCount++
	}

	return nil
}

// WaitLock tries to acquire all locks within a specified waiting period.
// It will attempt to acquire locks with exponential backoff if any lock is held.
// If lock acquisition fails after the wait timeout, all acquired locks are released.
// Parameters:
// - ctx: The context for managing the wait request lifecycle.
// - lockTimeout: The TTL to set for each lock when acquired.
// - waitTimeout: The maximum time to wait for all locks to become available.
// Returns an error if all locks could not be acquired within the wait timeout.
func (m *MultiLocker) WaitLock(ctx context.Context, lockTimeout, waitTimeout time.Duration) error {
	deadline := time.Now().Add(waitTimeout)

	for time.Now().Before(deadline) {
		err := m.Lock(ctx, lockTimeout)
		if err == nil {
			return nil
		}
		// Implementing exponential backoff to avoid busy-waiting.
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	return fmt.Errorf("failed to acquire all locks within the wait timeout")
}

// Unlock releases all locks in reverse order of acquisition.
// Releasing in reverse order is a best practice for multi-lock scenarios.
// Parameters:
// - ctx: The context for managing the unlock request lifecycle.
// Returns an error if any unlock operation fails.
func (m *MultiLocker) Unlock(ctx context.Context) error {
	var lastErr error

	// Release locks in reverse order
	for i := len(m.lockers) - 1; i >= 0; i-- {
		if err := m.lockers[i].Unlock(ctx); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// rollback releases locks that were successfully acquired before a failure.
// Parameters:
// - ctx: The context for managing the unlock request lifecycle.
// - count: The number of locks to release (from the beginning of the slice).
func (m *MultiLocker) rollback(ctx context.Context, count int) {
	// Release in reverse order of acquisition
	for i := count - 1; i >= 0; i-- {
		_ = m.lockers[i].Unlock(ctx)
	}
}

// Keys returns the deduplicated and sorted keys that this MultiLocker manages.
func (m *MultiLocker) Keys() []string {
	return m.keys
}
