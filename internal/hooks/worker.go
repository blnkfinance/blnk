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

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// ProcessHookTask processes a hook execution task from the queue.
// It unmarshals the task payload, creates a context with timeout based on hook configuration,
// and executes the hook.
//
// Parameters:
// - ctx: The context for the operation.
// - task: The Asynq task containing the hook execution details.
//
// Returns:
// - error: An error if hook execution fails.
func (m *redisHookManager) ProcessHookTask(ctx context.Context, task *asynq.Task) error {
	var taskPayload HookTaskPayload
	if err := json.Unmarshal(task.Payload(), &taskPayload); err != nil {
		return fmt.Errorf("failed to unmarshal hook task payload: %w", err)
	}

	// Add timeout to context based on hook configuration
	hookCtx, cancel := context.WithTimeout(ctx, time.Duration(taskPayload.Hook.Timeout)*time.Second)
	defer cancel()

	logrus.WithFields(logrus.Fields{
		"hook_id":   taskPayload.Hook.ID,
		"hook_type": taskPayload.Hook.Type,
	}).Info("Processing queued hook task")

	return m.executeHook(hookCtx, taskPayload.Hook, taskPayload.Payload)
}
