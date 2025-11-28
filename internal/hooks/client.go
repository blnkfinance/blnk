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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// ExecuteHook is a public wrapper for processing a hook task.
// It executes the webhook by sending an HTTP POST request to the configured URL.
//
// Parameters:
// - ctx: The context for the operation.
// - hook: The hook configuration containing URL and other settings.
// - payload: The payload to send in the webhook request.
//
// Returns:
// - error: An error if the webhook execution fails.
func (m *redisHookManager) ExecuteHook(ctx context.Context, hook *Hook, payload HookPayload) error {
	return m.executeHook(ctx, hook, payload)
}

// executeHook performs the actual HTTP request for the webhook.
// It handles marshalling the payload, creating the request, and processing the response.
// Retries are handled by the queue system, so this function only performs a single attempt.
func (m *redisHookManager) executeHook(ctx context.Context, hook *Hook, payload HookPayload) error {
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: time.Duration(hook.Timeout) * time.Second,
	}

	// Marshal payload with explicit handling
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Validate JSON before sending
	if !json.Valid(payloadBytes) {
		return fmt.Errorf("invalid JSON payload generated")
	}

	logrus.WithFields(logrus.Fields{
		"hook_id":   hook.ID,
		"hook_name": hook.Name,
		"hook_url":  hook.URL,
		"hook_type": hook.Type,
	}).Info("Executing webhook")

	req, err := http.NewRequestWithContext(ctx, "POST", hook.URL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hook-ID", hook.ID)
	req.Header.Set("X-Hook-Type", string(hook.Type))

	resp, err := client.Do(req)
	if err != nil {
		// Update hook execution status on failure
		_ = m.updateHookStatus(ctx, hook, false)
		if ctx.Err() != nil {
			logrus.WithFields(logrus.Fields{
				"hook_id":   hook.ID,
				"hook_type": hook.Type,
				"error":     err,
			}).Error("Hook execution cancelled due to context timeout")
			return ctx.Err()
		}
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.WithError(err).Error("Failed to close response body")
		}
	}()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = m.updateHookStatus(ctx, hook, false)
		return fmt.Errorf("failed to read response body: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"hook_id":     hook.ID,
		"hook_type":   hook.Type,
		"status_code": resp.StatusCode,
		"response":    string(body),
	}).Debug("Hook response received")

	// Handle empty response
	if len(body) == 0 {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			logrus.WithFields(logrus.Fields{
				"hook_id":     hook.ID,
				"hook_name":   hook.Name,
				"hook_url":    hook.URL,
				"hook_type":   hook.Type,
				"status_code": resp.StatusCode,
			}).Info("Hook executed successfully with empty response")
			_ = m.updateHookStatus(ctx, hook, true)
			return nil
		}
		_ = m.updateHookStatus(ctx, hook, false)
		return fmt.Errorf("hook returned empty response with status %d", resp.StatusCode)
	}

	// Check if response is JSON
	if !json.Valid(body) {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			logrus.WithFields(logrus.Fields{
				"hook_id":     hook.ID,
				"hook_name":   hook.Name,
				"hook_url":    hook.URL,
				"hook_type":   hook.Type,
				"status_code": resp.StatusCode,
				"response":    string(body),
			}).Info("Hook executed successfully with non-JSON response")
			_ = m.updateHookStatus(ctx, hook, true)
			return nil
		}
		if resp.StatusCode >= 400 {
			_ = m.updateHookStatus(ctx, hook, false)
			return fmt.Errorf("hook returned non-JSON error response (status %d): %s", resp.StatusCode, string(body))
		}
		// For non-error status codes, treat non-JSON response as success but log a warning
		logrus.WithFields(logrus.Fields{
			"hook_id":     hook.ID,
			"hook_type":   hook.Type,
			"status_code": resp.StatusCode,
			"response":    string(body),
		}).Warn("Hook returned non-JSON response")
		_ = m.updateHookStatus(ctx, hook, true)
		return nil
	}

	// For JSON responses, try to parse as HookResponse
	var hookResp HookResponse
	if err := json.Unmarshal(body, &hookResp); err != nil {
		if resp.StatusCode >= 400 {
			_ = m.updateHookStatus(ctx, hook, false)
			return fmt.Errorf("invalid JSON error response: %w", err)
		}
		// For non-error status codes, treat invalid JSON as success but log warning
		logrus.WithFields(logrus.Fields{
			"hook_id": hook.ID,
			"error":   err,
		}).Warn("Could not parse hook response as JSON")
		_ = m.updateHookStatus(ctx, hook, true)
		return nil
	}

	if !hookResp.Success {
		_ = m.updateHookStatus(ctx, hook, false)
		return fmt.Errorf("hook execution failed: %s", hookResp.Message)
	}

	logrus.WithFields(logrus.Fields{
		"hook_id":     hook.ID,
		"hook_name":   hook.Name,
		"hook_url":    hook.URL,
		"hook_type":   hook.Type,
		"status_code": resp.StatusCode,
		"message":     hookResp.Message,
	}).Info("Hook executed successfully with JSON response")

	_ = m.updateHookStatus(ctx, hook, true)
	return nil
}

func (m *redisHookManager) updateHookStatus(ctx context.Context, hook *Hook, success bool) error {
	hook.LastRun = time.Now()
	hook.LastSuccess = success

	logrus.WithFields(logrus.Fields{
		"hook_id":   hook.ID,
		"hook_name": hook.Name,
		"hook_type": hook.Type,
		"success":   success,
		"last_run":  hook.LastRun,
	}).Info("Updated hook execution status")

	data, err := json.Marshal(hook)
	if err != nil {
		return fmt.Errorf("failed to marshal hook: %w", err)
	}

	key := fmt.Sprintf("%s:%s", hookKeyPrefix, hook.ID)
	return m.client.Set(ctx, key, data, 0).Err()
}
