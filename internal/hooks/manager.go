package hooks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/redis/go-redis/v9"
)

const (
	hookKeyPrefix     = "hooks:"
	preHookKeyPrefix  = "hooks:pre"
	postHookKeyPrefix = "hooks:post"
)

type redisHookManager struct {
	client redis.UniversalClient
}

// NewHookManager creates a new Redis-based hook manager
func NewHookManager(redisClient redis.UniversalClient) HookManager {
	return &redisHookManager{
		client: redisClient,
	}
}

// RegisterHook registers a new webhook
func (m *redisHookManager) RegisterHook(ctx context.Context, hook *Hook) error {
	if hook.ID == "" {
		hook.ID = model.GenerateUUIDWithSuffix("hook")
	}
	hook.CreatedAt = time.Now()

	// Validate hook
	if err := validateHook(hook); err != nil {
		return err
	}

	// Store hook in Redis
	key := fmt.Sprintf("%s:%s", hookKeyPrefix, hook.ID)
	data, err := json.Marshal(hook)
	if err != nil {
		return fmt.Errorf("failed to marshal hook: %w", err)
	}

	// Store in main hook registry
	if err := m.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to store hook: %w", err)
	}

	// Add to type-specific set for faster lookups
	typeKey := getTypeKey(hook.Type)
	if err := m.client.SAdd(ctx, typeKey, hook.ID).Err(); err != nil {
		return fmt.Errorf("failed to add hook to type set: %w", err)
	}

	return nil
}

// UpdateHook updates an existing webhook
func (m *redisHookManager) UpdateHook(ctx context.Context, hookID string, hook *Hook) error {
	existing, err := m.GetHook(ctx, hookID)
	if err != nil {
		return fmt.Errorf("hook not found: %s", hookID)
	}

	// Update fields while preserving metadata
	hook.ID = existing.ID
	hook.CreatedAt = existing.CreatedAt
	hook.LastRun = existing.LastRun
	hook.LastSuccess = existing.LastSuccess

	// Handle type change
	if existing.Type != hook.Type {
		// Remove from old type set
		if err := m.client.SRem(ctx, getTypeKey(existing.Type), hookID).Err(); err != nil {
			return err
		}
		// Add to new type set
		if err := m.client.SAdd(ctx, getTypeKey(hook.Type), hookID).Err(); err != nil {
			return err
		}
	}

	// Store updated hook
	data, err := json.Marshal(hook)
	if err != nil {
		return fmt.Errorf("failed to marshal hook: %w", err)
	}

	key := fmt.Sprintf("%s:%s", hookKeyPrefix, hookID)
	return m.client.Set(ctx, key, data, 0).Err()
}

// DeleteHook removes a webhook
func (m *redisHookManager) DeleteHook(ctx context.Context, hookID string) error {
	hook, err := m.GetHook(ctx, hookID)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", hookKeyPrefix, hookID)
	typeKey := getTypeKey(hook.Type)

	// Use pipeline for atomic operations
	pipe := m.client.Pipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, typeKey, hookID)
	_, err = pipe.Exec(ctx)
	return err
}

// GetHook retrieves a webhook by ID
func (m *redisHookManager) GetHook(ctx context.Context, hookID string) (*Hook, error) {
	key := fmt.Sprintf("%s:%s", hookKeyPrefix, hookID)
	data, err := m.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("hook not found: %s", hookID)
		}
		return nil, err
	}

	var hook Hook
	if err := json.Unmarshal(data, &hook); err != nil {
		return nil, fmt.Errorf("failed to unmarshal hook: %w", err)
	}

	return &hook, nil
}

// ListHooks retrieves all hooks of a specific type
func (m *redisHookManager) ListHooks(ctx context.Context, hookType HookType) ([]*Hook, error) {
	typeKey := getTypeKey(hookType)
	hookIDs, err := m.client.SMembers(ctx, typeKey).Result()
	if err != nil {
		return nil, err
	}

	hooks := make([]*Hook, 0, len(hookIDs))
	for _, id := range hookIDs {
		hook, err := m.GetHook(ctx, id)
		if err != nil {
			continue // Skip failed hooks
		}
		hooks = append(hooks, hook)
	}

	return hooks, nil
}

// ExecutePreHooks executes all pre-transaction hooks
func (m *redisHookManager) ExecutePreHooks(ctx context.Context, transactionID string, data interface{}) error {
	hooks, err := m.ListHooks(ctx, PreTransaction)
	if err != nil {
		return err
	}

	return m.executeHooks(ctx, hooks, PreTransaction, transactionID, data)
}

// ExecutePostHooks executes all post-transaction hooks
func (m *redisHookManager) ExecutePostHooks(ctx context.Context, transactionID string, data interface{}) error {
	hooks, err := m.ListHooks(ctx, PostTransaction)
	if err != nil {
		return err
	}

	return m.executeHooks(ctx, hooks, PostTransaction, transactionID, data)
}

// Helper functions

func (m *redisHookManager) executeHooks(ctx context.Context, hooks []*Hook, hookType HookType, transactionID string, data interface{}) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal hook data: %w", err)
	}

	payload := HookPayload{
		TransactionID: transactionID,
		HookType:      hookType,
		Timestamp:     time.Now(),
		Data:          dataBytes,
	}

	for _, hook := range hooks {
		if !hook.Active {
			continue
		}

		go func(h *Hook) {
			// Create background context with timeout
			hookCtx, cancel := context.WithTimeout(context.Background(), time.Duration(h.Timeout)*time.Second)
			defer cancel()

			if err := m.executeHook(hookCtx, h, payload); err != nil {
				notification.NotifyError(fmt.Errorf("hook execution failed for hook %s (type: %s): %w", h.ID, h.Type, err))
			}
		}(hook)
	}

	return nil
}

func validateHook(hook *Hook) error {
	if hook.URL == "" {
		return fmt.Errorf("hook URL is required")
	}
	if hook.Type != PreTransaction && hook.Type != PostTransaction {
		return fmt.Errorf("invalid hook type: %s", hook.Type)
	}
	if hook.Timeout <= 0 {
		hook.Timeout = 30 // Default timeout
	}
	if hook.RetryCount < 0 {
		hook.RetryCount = 3 // Default retry count
	}
	return nil
}

func getTypeKey(hookType HookType) string {
	switch hookType {
	case PreTransaction:
		return preHookKeyPrefix
	case PostTransaction:
		return postHookKeyPrefix
	default:
		return hookKeyPrefix
	}
}
