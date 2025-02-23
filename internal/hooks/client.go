package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
)

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

	// Create exponential backoff with context
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = time.Duration(hook.Timeout) * time.Second
	b.MaxInterval = 5 * time.Second

	// Execute with retry
	operation := func() error {
		logrus.WithFields(logrus.Fields{
			"hook_id":   hook.ID,
			"hook_name": hook.Name,
			"hook_url":  hook.URL,
			"hook_type": hook.Type,
		}).Info("Executing webhook")

		req, err := http.NewRequestWithContext(ctx, "POST", hook.URL, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return backoff.Permanent(fmt.Errorf("failed to create request: %w", err))
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Hook-ID", hook.ID)
		req.Header.Set("X-Hook-Type", string(hook.Type))

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				logrus.WithFields(logrus.Fields{
					"hook_id":   hook.ID,
					"hook_type": hook.Type,
					"error":     err,
				}).Error("Hook execution cancelled due to context timeout")
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to execute request: %w", err)
		}
		defer resp.Body.Close()

		// Read response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}

		// Log the response for debugging
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
				return nil
			}
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
				return nil
			}
			if resp.StatusCode >= 400 {
				return fmt.Errorf("hook returned non-JSON error response (status %d): %s", resp.StatusCode, string(body))
			}
			// For non-error status codes, treat non-JSON response as success but log a warning
			logrus.WithFields(logrus.Fields{
				"hook_id":     hook.ID,
				"hook_type":   hook.Type,
				"status_code": resp.StatusCode,
				"response":    string(body),
			}).Warn("Hook returned non-JSON response")
			return nil
		}

		// For JSON responses, try to parse as HookResponse
		var hookResp HookResponse
		if err := json.Unmarshal(body, &hookResp); err != nil {
			if resp.StatusCode >= 400 {
				return fmt.Errorf("invalid JSON error response: %w", err)
			}
			// For non-error status codes, treat invalid JSON as success but log warning
			logrus.WithFields(logrus.Fields{
				"hook_id": hook.ID,
				"error":   err,
			}).Warn("Could not parse hook response as JSON")
			return nil
		}

		if !hookResp.Success {
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

		return nil
	}

	// Execute with retries and context
	err = backoff.Retry(operation, backoff.WithContext(backoff.WithMaxRetries(b, uint64(hook.RetryCount)), ctx))

	// Update hook execution status
	updateErr := m.updateHookStatus(ctx, hook, err == nil)
	if updateErr != nil {
		logrus.WithError(updateErr).Error("Failed to update hook status")
	}

	if err != nil {
		return fmt.Errorf("hook execution failed after %d retries: %w", hook.RetryCount, err)
	}

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
