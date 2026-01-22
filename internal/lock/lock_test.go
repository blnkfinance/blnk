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
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
)

func TestLocker_Lock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	mock.ExpectSetNX("test-key", "test-value", 5*time.Second).SetVal(true)

	err := locker.Lock(context.Background(), 5*time.Second)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_Lock_Failure(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	mock.ExpectSetNX("test-key", "test-value", 5*time.Second).SetVal(false)

	err := locker.Lock(context.Background(), 5*time.Second)
	assert.EqualError(t, err, "lock for key test-key is already held")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_Unlock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	// Simulate a successful unlock
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	mock.ExpectEval(script, []string{"test-key"}, "test-value").SetVal(int64(1))

	err := locker.Unlock(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_Unlock_Failure(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	// Simulate a failed unlock (either lock expired or not the lock holder)
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	mock.ExpectEval(script, []string{"test-key"}, "test-value").SetVal(int64(0))

	err := locker.Unlock(context.Background())
	assert.EqualError(t, err, "unlock failed, either lock expired or you're not the lock holder for key test-key")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_ExtendLock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	// Simulate successful lock extension
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
	mock.ExpectEval(script, []string{"test-key"}, "test-value", "5000").SetVal(int64(1))

	err := locker.ExtendLock(context.Background(), 5*time.Second)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_ExtendLock_Failure(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	// Simulate failed lock extension (either lock expired or not the holder)
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
	mock.ExpectEval(script, []string{"test-key"}, "test-value", "5000").SetVal(int64(0))

	err := locker.ExtendLock(context.Background(), 5*time.Second)
	assert.EqualError(t, err, "lock extension failed for key test-key, either lock expired or you're not the holder")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_WaitLock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	mock.ExpectSetNX("test-key", "test-value", 5*time.Second).SetVal(true)

	err := locker.WaitLock(context.Background(), 5*time.Second, 2*time.Second)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLocker_WaitLock_Failure(t *testing.T) {
	db, mock := redismock.NewClientMock()
	locker := NewLocker(db, "test-key", "test-value")

	// Simulate failure to acquire the lock within the wait timeout
	mock.ExpectSetNX("test-key", "test-value", 5*time.Second).SetVal(false)

	err := locker.WaitLock(context.Background(), 5*time.Second, 500*time.Millisecond)
	assert.EqualError(t, err, "failed to acquire lock for key test-key within the wait timeout")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestNewMultiLocker_SortsKeysLexicographically(t *testing.T) {
	db, _ := redismock.NewClientMock()

	// Keys are provided out of order
	multiLocker := NewMultiLocker(db, []string{"key-c", "key-a", "key-b"}, "test-value")

	// Keys should be sorted lexicographically
	keys := multiLocker.Keys()
	assert.Equal(t, []string{"key-a", "key-b", "key-c"}, keys)
}

func TestNewMultiLocker_DeduplicatesKeys(t *testing.T) {
	db, _ := redismock.NewClientMock()

	// Duplicate keys provided
	multiLocker := NewMultiLocker(db, []string{"key-a", "key-b", "key-a", "key-c", "key-b"}, "test-value")

	// Keys should be deduplicated and sorted
	keys := multiLocker.Keys()
	assert.Equal(t, []string{"key-a", "key-b", "key-c"}, keys)
}

func TestNewMultiLocker_IgnoresEmptyKeys(t *testing.T) {
	db, _ := redismock.NewClientMock()

	// Empty key provided
	multiLocker := NewMultiLocker(db, []string{"key-a", "", "key-b"}, "test-value")

	// Empty keys should be ignored
	keys := multiLocker.Keys()
	assert.Equal(t, []string{"key-a", "key-b"}, keys)
}

func TestNewMultiLocker_SameSourceAndDestination(t *testing.T) {
	db, _ := redismock.NewClientMock()

	// Same key for source and destination (self-transfer scenario)
	multiLocker := NewMultiLocker(db, []string{"balance-123", "balance-123"}, "test-value")

	// Should deduplicate to single key
	keys := multiLocker.Keys()
	assert.Equal(t, []string{"balance-123"}, keys)
	assert.Len(t, multiLocker.lockers, 1)
}

func TestMultiLocker_Lock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()

	// Keys will be sorted to: key-a, key-b
	multiLocker := NewMultiLocker(db, []string{"key-b", "key-a"}, "test-value")

	// Expect locks to be acquired in sorted order
	mock.ExpectSetNX("key-a", "test-value", 5*time.Second).SetVal(true)
	mock.ExpectSetNX("key-b", "test-value", 5*time.Second).SetVal(true)

	err := multiLocker.Lock(context.Background(), 5*time.Second)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiLocker_Lock_RollbackOnPartialFailure(t *testing.T) {
	db, mock := redismock.NewClientMock()

	// Keys will be sorted to: key-a, key-b, key-c
	multiLocker := NewMultiLocker(db, []string{"key-c", "key-a", "key-b"}, "test-value")

	// First two locks succeed, third fails
	mock.ExpectSetNX("key-a", "test-value", 5*time.Second).SetVal(true)
	mock.ExpectSetNX("key-b", "test-value", 5*time.Second).SetVal(true)
	mock.ExpectSetNX("key-c", "test-value", 5*time.Second).SetVal(false)

	// Rollback should release locks in reverse order (key-b, key-a)
	unlockScript := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	mock.ExpectEval(unlockScript, []string{"key-b"}, "test-value").SetVal(int64(1))
	mock.ExpectEval(unlockScript, []string{"key-a"}, "test-value").SetVal(int64(1))

	err := multiLocker.Lock(context.Background(), 5*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acquire lock for key key-c")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiLocker_Unlock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()

	// Keys will be sorted to: key-a, key-b
	multiLocker := NewMultiLocker(db, []string{"key-b", "key-a"}, "test-value")

	// Unlock should release locks in reverse order (key-b, key-a)
	unlockScript := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	mock.ExpectEval(unlockScript, []string{"key-b"}, "test-value").SetVal(int64(1))
	mock.ExpectEval(unlockScript, []string{"key-a"}, "test-value").SetVal(int64(1))

	err := multiLocker.Unlock(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiLocker_Unlock_ReturnsLastError(t *testing.T) {
	db, mock := redismock.NewClientMock()

	multiLocker := NewMultiLocker(db, []string{"key-a", "key-b"}, "test-value")

	// First unlock fails, second succeeds
	unlockScript := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	mock.ExpectEval(unlockScript, []string{"key-b"}, "test-value").SetVal(int64(0)) // Failure
	mock.ExpectEval(unlockScript, []string{"key-a"}, "test-value").SetVal(int64(1)) // Success

	err := multiLocker.Unlock(context.Background())
	// Should return the error from first failed unlock
	assert.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiLocker_WaitLock_Success(t *testing.T) {
	db, mock := redismock.NewClientMock()

	multiLocker := NewMultiLocker(db, []string{"key-b", "key-a"}, "test-value")

	// Expect locks to be acquired in sorted order
	mock.ExpectSetNX("key-a", "test-value", 5*time.Second).SetVal(true)
	mock.ExpectSetNX("key-b", "test-value", 5*time.Second).SetVal(true)

	err := multiLocker.WaitLock(context.Background(), 5*time.Second, 2*time.Second)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMultiLocker_SingleKey(t *testing.T) {
	db, mock := redismock.NewClientMock()

	// Single key scenario
	multiLocker := NewMultiLocker(db, []string{"single-key"}, "test-value")

	assert.Equal(t, []string{"single-key"}, multiLocker.Keys())
	assert.Len(t, multiLocker.lockers, 1)

	// Lock
	mock.ExpectSetNX("single-key", "test-value", 5*time.Second).SetVal(true)

	err := multiLocker.Lock(context.Background(), 5*time.Second)
	assert.NoError(t, err)

	// Unlock
	unlockScript := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	mock.ExpectEval(unlockScript, []string{"single-key"}, "test-value").SetVal(int64(1))

	err = multiLocker.Unlock(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
