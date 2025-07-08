package redis_db

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestParseRedisURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected *redis.Options
		wantErr  bool
	}{
		{
			name: "simple docker style",
			url:  "redis:6379",
			expected: &redis.Options{
				Addr: "redis:6379",
			},
			wantErr: false,
		},
		{
			name: "redis url with password",
			url:  "redis://:password123@localhost:6379",
			expected: &redis.Options{
				Addr:     "localhost:6379",
				Password: "password123",
			},
			wantErr: false,
		},
		{
			name: "azure redis url",
			url:  "myinstance.redis.cache.windows.net:6380",
			expected: &redis.Options{
				Addr: "myinstance.redis.cache.windows.net:6380",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRedisURL(tt.url)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected.Addr, got.Addr)
			assert.Equal(t, tt.expected.Password, got.Password)
		})
	}
}

func TestNewRedisClient(t *testing.T) {
	tests := []struct {
		name      string
		addresses []string
		wantErr   bool
	}{
		{
			name:      "empty addresses",
			addresses: []string{},
			wantErr:   true,
		},
		{
			name:      "single address",
			addresses: []string{"localhost:6379"},
			wantErr:   false,
		},
		{
			name:      "multiple addresses",
			addresses: []string{"localhost:6379", "localhost:6380"},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewRedisClient(tt.addresses, false)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, client)
			assert.NotNil(t, client.Client())
		})
	}
}

func TestRedisIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client, err := NewRedisClient([]string{"localhost:6379"}, false)
	if err != nil {
		t.Fatalf("Failed to create Redis client: %v", err)
	}

	ctx := context.Background()
	key := "test_key"
	value := "test_value"

	// Test Set
	err = client.Client().Set(ctx, key, value, time.Minute).Err()
	assert.NoError(t, err)

	// Test Get
	got, err := client.Client().Get(ctx, key).Result()
	assert.NoError(t, err)
	assert.Equal(t, value, got)

	// Test Delete
	err = client.Client().Del(ctx, key).Err()
	assert.NoError(t, err)

	// Verify deletion
	_, err = client.Client().Get(ctx, key).Result()
	assert.Equal(t, redis.Nil, err)
}
