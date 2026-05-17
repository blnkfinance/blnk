package hooks

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestValidateHookRejectsUnsupportedURLSchemes(t *testing.T) {
	tests := []string{
		"file:///etc/passwd",
		"gopher://example.com",
		"http://",
		"://example.com",
	}

	for _, hookURL := range tests {
		t.Run(hookURL, func(t *testing.T) {
			hook := &Hook{
				URL:  hookURL,
				Type: PostTransaction,
			}

			if err := validateHook(hook); err == nil {
				t.Fatalf("expected validation error for URL %q", hookURL)
			}
		})
	}
}

func TestValidateHookAllowsHTTPAndHTTPSURLs(t *testing.T) {
	tests := []string{
		"http://example.com/webhook",
		"https://example.com/webhook",
	}

	for _, hookURL := range tests {
		t.Run(hookURL, func(t *testing.T) {
			hook := &Hook{
				URL:  hookURL,
				Type: PostTransaction,
			}

			if err := validateHook(hook); err != nil {
				t.Fatalf("expected URL %q to validate: %v", hookURL, err)
			}
			if hook.Timeout != defaultHookTimeoutSec {
				t.Fatalf("expected default timeout %d, got %d", defaultHookTimeoutSec, hook.Timeout)
			}
		})
	}
}

func TestValidateHookRejectsExcessiveTimeoutAndRetries(t *testing.T) {
	tests := []struct {
		name string
		hook Hook
		want string
	}{
		{
			name: "timeout",
			hook: Hook{
				URL:     "https://example.com/webhook",
				Type:    PostTransaction,
				Timeout: maxHookTimeoutSec + 1,
			},
			want: "timeout",
		},
		{
			name: "retries",
			hook: Hook{
				URL:        "https://example.com/webhook",
				Type:       PostTransaction,
				RetryCount: maxHookRetryCount + 1,
			},
			want: "retry_count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateHook(&tt.hook)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestUpdateHookRejectsInvalidURL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer redisClient.Close()

	manager := &redisHookManager{client: redisClient}
	original := &Hook{
		ID:     "hook_test",
		Name:   "original",
		URL:    "https://example.com/webhook",
		Type:   PostTransaction,
		Active: true,
	}

	if err := manager.RegisterHook(context.Background(), original); err != nil {
		t.Fatalf("register hook: %v", err)
	}

	err = manager.UpdateHook(context.Background(), original.ID, &Hook{
		Name:   "updated",
		URL:    "file:///etc/passwd",
		Type:   PostTransaction,
		Active: true,
	})
	if err == nil {
		t.Fatal("expected invalid update to fail")
	}

	stored, err := manager.GetHook(context.Background(), original.ID)
	if err != nil {
		t.Fatalf("get hook: %v", err)
	}
	if stored.URL != original.URL {
		t.Fatalf("expected stored URL to remain %q, got %q", original.URL, stored.URL)
	}
}
