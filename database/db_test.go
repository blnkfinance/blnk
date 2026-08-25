package database

import (
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetDBSingleton clears the cached connection so a test can control what the
// next GetDBConnection call sees. Tests that leave the singleton unset must not
// strand later tests, so this only drops the pointer.
func resetDBSingleton() {
	instanceMu.Lock()
	defer instanceMu.Unlock()
	instance.Store(nil)
}

func TestGetDBConnection_Singleton(t *testing.T) {
	// Reset the singleton so this test controls the first connection attempt.
	resetDBSingleton()

	// Create a mock configuration with a valid DNS string
	mockConfig := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost/blnk?sslmode=disable",
		},
	}

	config.ConfigStore.Store(mockConfig)

	// First call to GetDBConnection should initialize the instance
	ds1, err := GetDBConnection(mockConfig)
	assert.NoError(t, err)
	assert.NotNil(t, ds1)

	// Second call should return the same instance
	ds2, err := GetDBConnection(mockConfig)
	assert.NoError(t, err)
	assert.Equal(t, ds1, ds2)
}

func TestGetDBConnection_Failure(t *testing.T) {
	// Reset the singleton so this test controls the first connection attempt.
	resetDBSingleton()

	// Create a mock configuration with invalid DNS
	mockConfig := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "invalid-dns",
		},
	}

	// Expect error when connecting to DB with invalid DNS
	_, err := GetDBConnection(mockConfig)
	assert.Error(t, err)
}

// A failed first attempt must not poison the process. Before the singleton
// retried, sync.Once latched on that failure: instance stayed nil, and because
// the error was a fresh local on every call, later callers were handed
// (nil, nil) — a nil datasource with nothing to check. NewDataSource then
// dereferenced it, so an unreachable database at startup turned every
// subsequent connection into a nil-pointer panic instead of a retry.
func TestGetDBConnection_RetriesAfterFailedAttempt(t *testing.T) {
	resetDBSingleton()
	t.Cleanup(resetDBSingleton)

	failing := &config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "invalid-dns"},
	}
	ds, err := GetDBConnection(failing)
	assert.Error(t, err)
	assert.Nil(t, ds, "a failed attempt must not yield a datasource")

	working := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost/blnk?sslmode=disable",
		},
	}
	ds2, err2 := GetDBConnection(working)
	assert.NoError(t, err2)
	require.NotNil(t, ds2, "a later attempt must reconnect, not return the nil left behind by the failure")
}

// The panic this guards against was previously masked: two integration tests
// recovered from it and reported themselves as skipped, so it never surfaced.
func TestNewDataSource_NoPanicAfterFailedAttempt(t *testing.T) {
	resetDBSingleton()
	t.Cleanup(resetDBSingleton)

	failing := &config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "invalid-dns"},
	}
	_, err := GetDBConnection(failing)
	require.Error(t, err)

	working := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost/blnk?sslmode=disable",
		},
	}
	require.NotPanics(t, func() {
		ds, err := NewDataSource(working)
		assert.NoError(t, err)
		assert.NotNil(t, ds)
	})
}

// Concurrent callers must all receive the same singleton, and exactly one
// connection must be built. GetDBConnection is called per request by the
// health endpoint and by two transaction handlers, so it has to stay safe
// under parallel use.
func TestGetDBConnection_ConcurrentCallersShareOneInstance(t *testing.T) {
	resetDBSingleton()
	t.Cleanup(resetDBSingleton)

	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost/blnk?sslmode=disable",
		},
	}

	const goroutines = 64
	var wg sync.WaitGroup
	results := make([]*Datasource, goroutines)
	errs := make([]error, goroutines)

	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release them together to maximise overlap
			results[i], errs[i] = GetDBConnection(cfg)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		assert.Same(t, results[0], results[i], "every caller must get the same singleton")
	}
}

// The fast path must not take the construction lock. If a future change moves
// the lock back to the top of GetDBConnection, this deadlocks and fails on the
// timeout rather than passing silently.
func TestGetDBConnection_FastPathDoesNotBlockOnTheBuildLock(t *testing.T) {
	resetDBSingleton()
	t.Cleanup(resetDBSingleton)

	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost/blnk?sslmode=disable",
		},
	}
	// Build the singleton first so subsequent calls take the fast path.
	ds, err := GetDBConnection(cfg)
	require.NoError(t, err)
	require.NotNil(t, ds)

	instanceMu.Lock() // hold the construction lock
	defer instanceMu.Unlock()

	done := make(chan *Datasource, 1)
	go func() {
		got, _ := GetDBConnection(cfg)
		done <- got
	}()

	select {
	case got := <-done:
		assert.Same(t, ds, got)
	case <-time.After(5 * time.Second):
		t.Fatal("GetDBConnection blocked on the construction lock: the cached read is not lock-free")
	}
}

func TestConnectDB_Success(t *testing.T) {
	// Provide a valid DNS string for your testing database
	dns := "postgres://postgres:password@localhost/blnk?sslmode=disable"

	db, err := ConnectDB(config.DataSourceConfig{Dns: dns})
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// Ensure that db is not nil before calling Close
	if db != nil {
		defer func() { _ = db.Close() }()
	}
}

func TestConnectDB_Failure(t *testing.T) {
	// Provide an invalid DNS string to simulate a failure
	invalidDNS := "invalid-dns"

	db, err := ConnectDB(config.DataSourceConfig{Dns: invalidDNS})
	assert.Error(t, err)
	assert.Nil(t, db)
}
