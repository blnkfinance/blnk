package main

import (
	"fmt"
	"testing"

	"github.com/blnkfinance/blnk/config"
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
