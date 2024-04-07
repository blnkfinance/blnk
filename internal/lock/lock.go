package redlock

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type Locker struct {
	client redis.UniversalClient
	key    string
	value  string // Used for ensuring that only the lock holder can unlock or renew the lock
}

func NewLocker(client redis.UniversalClient, key, value string) *Locker {
	return &Locker{
		client: client,
		key:    key,
		value:  value,
	}
}

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

func (l *Locker) Unlock(ctx context.Context) error {
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

func (l *Locker) ExtendLock(ctx context.Context, extension time.Duration) error {
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

func (l *Locker) WaitLock(ctx context.Context, lockTimeout, waitTimeout time.Duration) error {
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		err := l.Lock(ctx, lockTimeout)
		if err == nil {
			return nil
		}
		// Implementing exponential backoff
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return fmt.Errorf("failed to acquire lock for key %s within the wait timeout", l.key)
}
