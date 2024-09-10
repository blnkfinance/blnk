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
