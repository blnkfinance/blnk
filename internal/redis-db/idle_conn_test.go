package redis_db

import (
	"context"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const testRedisAddr = "localhost:6379"

func TestStalePooledConnAfterIdleClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	requireLocalRedis(t)

	serverIdle := 400 * time.Millisecond
	err := evalAfterIdle(t, &redis.Options{
		Addr:         testRedisAddr,
		PoolSize:     1,
		MinIdleConns: 0,
		// go-redis retries 3 times by default (-1 disables, 0 means default).
		// Production hits the same failure once every pooled conn is stale:
		// each retry pops another dead socket until retries are exhausted.
		MaxRetries:      -1,
		ConnMaxIdleTime: -1, // never recycle idle conns: Core before the ConnMaxIdleTime fix
		Dialer:          idleBreakDialer(serverIdle),
	}, serverIdle*2)
	if err == nil {
		t.Fatal("expected broken pipe; pool reused a conn idle longer than the server timeout")
	}
}

func TestPooledConnRecycledBeforeIdleClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	requireLocalRedis(t)

	serverIdle := 400 * time.Millisecond
	err := evalAfterIdle(t, &redis.Options{
		Addr:            testRedisAddr,
		PoolSize:        1,
		MinIdleConns:    0,
		MaxRetries:      -1, // no retries, so success cannot come from redialing
		ConnMaxIdleTime: serverIdle / 2,
		Dialer:          idleBreakDialer(serverIdle),
	}, serverIdle*2)
	if err != nil {
		t.Fatalf("pool should have discarded the idle conn before reuse: %v", err)
	}
}

// TestNewRedisClientSetsConnMaxIdleTime pins the fix at the constructor
// level: every pool Core builds must recycle idle conns before a managed
// Redis/Valkey server can close them (TestStalePooledConnAfterIdleClose
// shows the broken-pipe failure that otherwise results).
func TestNewRedisClientSetsConnMaxIdleTime(t *testing.T) {
	requireLocalRedis(t)

	byDefault, err := NewRedisClient([]string{testRedisAddr}, false)
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	t.Cleanup(func() { _ = byDefault.Client().Close() })
	if got := standaloneOptions(t, byDefault.Client()).ConnMaxIdleTime; got != defaultConnMaxIdleTime {
		t.Fatalf("default ConnMaxIdleTime = %v, want %v", got, defaultConnMaxIdleTime)
	}

	custom, err := NewRedisClient([]string{testRedisAddr}, false, &PoolConfig{ConnMaxIdleTime: 45 * time.Second})
	if err != nil {
		t.Fatalf("NewRedisClient with pool config: %v", err)
	}
	t.Cleanup(func() { _ = custom.Client().Close() })
	if got := standaloneOptions(t, custom.Client()).ConnMaxIdleTime; got != 45*time.Second {
		t.Fatalf("custom ConnMaxIdleTime = %v, want %v", got, 45*time.Second)
	}
}

// TestNewConnOptSetsConnMaxIdleTime guards the asynq path, which previously
// used asynq.RedisClientOpt and could not express ConnMaxIdleTime at all.
func TestNewConnOptSetsConnMaxIdleTime(t *testing.T) {
	connOpt, err := NewConnOpt(testRedisAddr, false)
	if err != nil {
		t.Fatalf("NewConnOpt: %v", err)
	}
	client, ok := connOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		t.Fatalf("MakeRedisClient returned %T; asynq requires redis.UniversalClient", connOpt.MakeRedisClient())
	}
	t.Cleanup(func() { _ = client.Close() })
	if got := standaloneOptions(t, client).ConnMaxIdleTime; got != defaultConnMaxIdleTime {
		t.Fatalf("asynq conn opt ConnMaxIdleTime = %v, want %v", got, defaultConnMaxIdleTime)
	}
}

func standaloneOptions(t *testing.T, client redis.UniversalClient) *redis.Options {
	t.Helper()
	rdb, ok := client.(*redis.Client)
	if !ok {
		t.Fatalf("expected *redis.Client, got %T", client)
	}
	return rdb.Options()
}

func requireLocalRedis(t *testing.T) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", testRedisAddr, err)
	}
}

func evalAfterIdle(t *testing.T, opts *redis.Options, idle time.Duration) error {
	t.Helper()
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("warm ping: %v", err)
	}
	time.Sleep(idle)
	return rdb.Eval(ctx, "return 1", nil).Err()
}

func idleBreakDialer(idle time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &idleBreakConn{Conn: c, idle: idle}, nil
	}
}

type idleBreakConn struct {
	net.Conn
	idle     time.Duration
	mu       sync.Mutex
	lastUsed time.Time
}

func (c *idleBreakConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	stale := !c.lastUsed.IsZero() && time.Since(c.lastUsed) > c.idle
	if !stale {
		c.lastUsed = time.Now()
	}
	c.mu.Unlock()
	if stale {
		return 0, &net.OpError{Op: "write", Net: "tcp", Addr: c.RemoteAddr(), Err: syscall.EPIPE}
	}
	return c.Conn.Write(p)
}

func (c *idleBreakConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.mu.Lock()
		c.lastUsed = time.Now()
		c.mu.Unlock()
	}
	return n, err
}
