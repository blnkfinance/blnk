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

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Genuinely untestable in-process and deliberately not faked here: main(),
// executeCLI's os.Exit path, ACME certificate issuance in serveTLS, and
// delivery of real SIGTERM signals (gracefulShutdown is tested by sending on
// its quit channel instead).

func TestRetryWithBackoff(t *testing.T) {
	t.Run("succeeds after transient failures", func(t *testing.T) {
		calls := 0
		err := retryWithBackoff(context.Background(), 5, time.Millisecond, func() error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 3, calls, "must stop retrying after the first success")
	})

	t.Run("returns last error when all attempts fail", func(t *testing.T) {
		calls := 0
		err := retryWithBackoff(context.Background(), 3, time.Millisecond, func() error {
			calls++
			return errors.New("permanent failure")
		})
		require.Error(t, err)
		assert.Equal(t, 3, calls)
		assert.Contains(t, err.Error(), "after 3 attempts")
		assert.Contains(t, err.Error(), "permanent failure")
	})

	t.Run("stops when context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := retryWithBackoff(ctx, 5, time.Hour, func() error {
			calls++
			return errors.New("fails")
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, calls, "a canceled context must short-circuit the backoff sleep")
	})
}

func TestResolveTLSDomains(t *testing.T) {
	assert.Equal(t, []string{"localhost"}, resolveTLSDomains(config.ServerConfig{}))
	assert.Equal(t, []string{"ledger.example.com"}, resolveTLSDomains(config.ServerConfig{Domain: "ledger.example.com"}))
}

func TestResolveCertStoragePath(t *testing.T) {
	assert.Equal(t, "/var/lib/blnk/certs", resolveCertStoragePath(config.ServerConfig{}))
	assert.Equal(t, "/tmp/certs", resolveCertStoragePath(config.ServerConfig{CertStoragePath: "/tmp/certs"}))
}

func TestGetOrCreateHeartbeatIDAt(t *testing.T) {
	t.Run("persists a stable ID", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "heartbeat.db")

		first := getOrCreateHeartbeatIDAt(path)
		require.NotEmpty(t, first)

		second := getOrCreateHeartbeatIDAt(path)
		assert.Equal(t, first, second, "the heartbeat ID must survive restarts via SQLite")

		_, err := os.Stat(path)
		require.NoError(t, err, "the SQLite file must have been created")
	})

	t.Run("falls back to a fresh ID when storage is unwritable", func(t *testing.T) {
		// A directory path is not a valid SQLite file: storage fails, but the
		// function must still return a usable (non-persistent) UUID.
		id := getOrCreateHeartbeatIDAt(t.TempDir())
		assert.NotEmpty(t, id)
	})
}

func TestNewHTTPServerAndGracefulShutdown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	server := newHTTPServer(router, "0")
	assert.Equal(t, ":0", server.Addr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()

	go func() { _ = server.Serve(ln) }()

	// The server must actually serve requests before shutdown.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/ping")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 50*time.Millisecond)

	quit := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- gracefulShutdown(server, quit, 5*time.Second) }()

	quit <- os.Interrupt

	select {
	case err := <-done:
		require.NoError(t, err, "graceful shutdown must complete cleanly")
	case <-time.After(10 * time.Second):
		t.Fatal("gracefulShutdown did not return after the quit signal")
	}

	// After shutdown the server must refuse new connections.
	_, err = http.Get("http://" + addr + "/ping")
	require.Error(t, err, "server must not accept requests after shutdown")
}

func TestHealthCheckHandler(t *testing.T) {
	cfg := realInfraConfig(t)
	_ = cfg

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health", nil)

	healthCheckHandler(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"UP"`)
}

func TestInitializeTypeSense(t *testing.T) {
	t.Run("disabled when DNS not configured", func(t *testing.T) {
		client, err := initializeTypeSense(context.Background(), &config.Configuration{})
		require.NoError(t, err)
		assert.Nil(t, client, "search must be disabled, not an error, without a TypeSense DNS")
	})

	t.Run("real typesense initializes collections", func(t *testing.T) {
		cfg := &config.Configuration{
			TypeSenseKey: "blnk-api-key",
			TypeSense:    config.TypeSenseConfig{Dns: "http://localhost:8108"},
		}
		client, err := initializeTypeSense(context.Background(), cfg)
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("dead server fails once the context expires", func(t *testing.T) {
		cfg := &config.Configuration{
			TypeSenseKey: "any",
			TypeSense:    config.TypeSenseConfig{Dns: "http://localhost:1"},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		start := time.Now()
		client, err := initializeTypeSense(ctx, cfg)
		require.Error(t, err)
		assert.Nil(t, client)
		assert.Less(t, time.Since(start), 10*time.Second, "context expiry must cut the retry loop short")
	})
}

func TestInitializeRouter(t *testing.T) {
	b := newCmdTestInstance(t)
	router := initializeRouter(b)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"UP"`)
}

func TestSetupBlnk(t *testing.T) {
	cfg := realInfraConfig(t)

	instance, err := setupBlnk(cfg)
	require.NoError(t, err)
	require.NotNil(t, instance)
	require.NotNil(t, instance.GetDataSource(), "a real config must produce a wired Blnk instance")
}
