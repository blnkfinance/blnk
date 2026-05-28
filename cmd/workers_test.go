package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/hibiken/asynq"
)

func TestHasReachedMaxRetryAttempt(t *testing.T) {
	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			MaxRetryAttempts: 5,
		},
	}

	tests := []struct {
		name       string
		retryCount int
		want       bool
	}{
		{name: "below max", retryCount: 4, want: false},
		{name: "at max", retryCount: 5, want: true},
		{name: "above max", retryCount: 6, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasReachedMaxRetryAttempt(cfg, tt.retryCount); got != tt.want {
				t.Fatalf("hasReachedMaxRetryAttempt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldRejectLockContentionImmediately(t *testing.T) {
	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			RejectLockContentionImmediately: true,
		},
	}

	if !shouldRejectLockContentionImmediately(cfg, fmt.Errorf("failed to acquire lock: already held")) {
		t.Fatal("expected lock contention to reject immediately when enabled")
	}

	if shouldRejectLockContentionImmediately(cfg, fmt.Errorf("random transient database error")) {
		t.Fatal("expected non-lock error not to reject immediately")
	}

	cfg.Queue.RejectLockContentionImmediately = false
	if shouldRejectLockContentionImmediately(cfg, fmt.Errorf("failed to acquire lock: already held")) {
		t.Fatal("expected lock contention not to reject immediately when disabled")
	}
}

func mockConfig(t *testing.T) *config.Configuration {
	t.Helper()
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://x:x@localhost/x"},
		Redis:      config.RedisConfig{Dns: "redis://localhost:6379"},
	})
	cfg, err := config.Fetch()
	if err != nil {
		t.Fatalf("config.Fetch: %v", err)
	}
	return cfg
}

// Fix 1: InflightCommitQueue must have a non-empty default so the producer
// doesn't silently enqueue to a "" queue name when the env var is unset.
func TestInflightCommitQueueDefault(t *testing.T) {
	cfg := mockConfig(t)
	if cfg.Queue.InflightCommitQueue == "" {
		t.Fatal("InflightCommitQueue has no default — producer will enqueue to empty queue name and jobs are lost")
	}
}

// Fix 2: initializeQueues must include InflightCommitQueue so the asynq server
// polls it. Without this, tasks move from scheduled→pending and sit there forever.
func TestInflightCommitQueuePolled(t *testing.T) {
	cfg := mockConfig(t)
	queues := initializeQueues()
	if _, ok := queues[cfg.Queue.InflightCommitQueue]; !ok {
		t.Fatalf("initializeQueues() missing %q — asynq server will never dequeue inflight commit jobs", cfg.Queue.InflightCommitQueue)
	}
}

// Fix 3: initializeTaskHandlers must register a handler for InflightCommitQueue.
// Without this, asynq archives every task with "handler not found".
func TestInflightCommitHandlerRegistered(t *testing.T) {
	cfg := mockConfig(t)
	b := &blnkInstance{cnf: cfg}
	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)

	payload, _ := json.Marshal("dummy-txn-id")
	task := asynq.NewTask(cfg.Queue.InflightCommitQueue, payload)
	noHandlerErr := fmt.Sprintf("asynq: no handler found for task %q", cfg.Queue.InflightCommitQueue)

	func() {
		defer func() { recover() }() // nil blnk panics inside handler body — that's fine, it means handler was found
		if err := mux.ProcessTask(context.Background(), task); err != nil && err.Error() == noHandlerErr {
			t.Fatalf("no handler registered for %q — tasks will be archived unprocessed", cfg.Queue.InflightCommitQueue)
		}
	}()
}
