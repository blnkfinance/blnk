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

package hooks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	hookKeyPrefix     = "hooks:"
	preHookKeyPrefix  = "hooks:pre"
	postHookKeyPrefix = "hooks:post"
)

type redisHookManager struct {
	client redis.UniversalClient
	queue  *asynq.Client
	config *config.Configuration
}

// NewHookManager creates a new Redis-based hook manager.
// It initializes the hook manager with the provided Redis client and Asynq client.
//
// Parameters:
// - redisClient: The Redis client for storing hook configurations.
// - queue: The Asynq client for queuing hook execution tasks.
//
// Returns:
// - HookManager: A new instance of the hook manager.
func NewHookManager(redisClient redis.UniversalClient, queue *asynq.Client) HookManager {
	config, err := config.Fetch()
	if err != nil {
		logrus.Errorf("failed to fetch config: %v", err)
	}
	return &redisHookManager{
		client: redisClient,
		queue:  queue,
		config: config,
	}
}

// RegisterHook registers a new webhook in Redis.
// It assigns a new ID if not provided, validates the hook configuration,
// and stores it in both the main registry and type-specific sets.
//
// Parameters:
// - ctx: The context for the operation.
// - hook: The hook configuration to register.
//
// Returns:
// - error: An error if registration fails.
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

// UpdateHook updates an existing webhook in Redis.
// It retrieves the existing hook, updates its fields while preserving metadata,
// handles type changes by updating sets, and saves the updated hook.
//
// Parameters:
// - ctx: The context for the operation.
// - hookID: The ID of the hook to update.
// - hook: The new hook configuration.
//
// Returns:
// - error: An error if the hook is not found or the update fails.
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

// DeleteHook removes a webhook from Redis.
// It retrieves the hook to determine its type, then deletes it from
// both the main registry and the type-specific set using a transaction.
//
// Parameters:
// - ctx: The context for the operation.
// - hookID: The ID of the hook to delete.
//
// Returns:
// - error: An error if the deletion fails.
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

// GetHook retrieves a webhook by its ID.
// It fetches the hook data from Redis and unmarshals it into a Hook struct.
//
// Parameters:
// - ctx: The context for the operation.
// - hookID: The ID of the hook to retrieve.
//
// Returns:
// - *Hook: The retrieved hook configuration.
// - error: An error if the hook is not found or retrieval fails.
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

// ListHooks retrieves all hooks of a specific type or all hooks if type is empty.
// If hookType is provided, it fetches hooks from that specific set.
// If hookType is empty, it combines hooks from both PreTransaction and PostTransaction sets.
//
// Parameters:
// - ctx: The context for the operation.
// - hookType: The type of hooks to retrieve (PreTransaction or PostTransaction).
//
// Returns:
// - []*Hook: A slice of retrieved hooks.
// - error: An error if retrieval fails.
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

// ExecutePreHooks queues all active pre-transaction hooks for execution.
// It retrieves all PreTransaction hooks and queues them with the transaction data.
//
// Parameters:
// - ctx: The context for the operation.
// - transactionID: The ID of the transaction triggering the hooks.
// - data: The transaction data payload.
//
// Returns:
// - error: An error if hook retrieval or queuing fails.
func (m *redisHookManager) ExecutePreHooks(ctx context.Context, transactionID string, data interface{}) error {
	hooks, err := m.ListHooks(ctx, PreTransaction)
	if err != nil {
		return err
	}

	return m.executeHooks(ctx, hooks, PreTransaction, transactionID, data)
}

// ExecutePostHooks queues all active post-transaction hooks for execution.
// It retrieves all PostTransaction hooks and queues them with the transaction data.
//
// Parameters:
// - ctx: The context for the operation.
// - transactionID: The ID of the transaction triggering the hooks.
// - data: The transaction data payload.
//
// Returns:
// - error: An error if hook retrieval or queuing fails.
func (m *redisHookManager) ExecutePostHooks(ctx context.Context, transactionID string, data interface{}) error {
	hooks, err := m.ListHooks(ctx, PostTransaction)
	if err != nil {
		return err
	}

	return m.executeHooks(ctx, hooks, PostTransaction, transactionID, data)
}

// executeHooks marshals the hook data and queues each active hook for execution.
//
// Parameters:
// - ctx: The context for the operation.
// - hooks: The list of hooks to execute.
// - hookType: The type of the hooks being executed.
// - transactionID: The ID of the transaction.
// - data: The payload data for the hooks.
//
// Returns:
// - error: An error if data marshalling fails.
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

		// Queue hook execution instead of running directly
		err := m.queueHook(ctx, hook, payload)
		if err != nil {
			notification.NotifyError(fmt.Errorf("failed to queue hook execution for hook %s: %w", hook.ID, err))
		}
	}

	return nil
}

// queueHook creates an Asynq task for a hook execution and enqueues it.
//
// Parameters:
// - ctx: The context for the operation.
// - hook: The hook configuration.
// - payload: The payload to send with the hook.
//
// Returns:
// - error: An error if task creation or enqueuing fails.
func (m *redisHookManager) queueHook(ctx context.Context, hook *Hook, payload HookPayload) error {
	taskPayload := HookTaskPayload{
		Hook:    hook,
		Payload: payload,
	}

	data, err := json.Marshal(taskPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal hook task payload: %w", err)
	}

	conf, err := config.Fetch()
	if err != nil {
		return fmt.Errorf("failed to fetch config: %w", err)
	}

	// Use webhook queue from config
	task := asynq.NewTask("new:hook_execution", data, asynq.Queue(conf.Queue.WebhookQueue))

	_, err = m.queue.EnqueueContext(ctx, task, asynq.MaxRetry(hook.RetryCount))
	if err != nil {
		return fmt.Errorf("failed to enqueue hook task: %w", err)
	}

	return nil
}

// validateHook checks if a hook configuration is valid.
// It ensures the URL and type are set correctly, and sets default values for timeout and retry count if missing.
//
// Parameters:
// - hook: The hook to validate.
//
// Returns:
// - error: An error if validation fails.
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

// getTypeKey returns the Redis key for a specific hook type set.
//
// Parameters:
// - hookType: The type of hook.
//
// Returns:
// - string: The Redis key for the hook type set.
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
