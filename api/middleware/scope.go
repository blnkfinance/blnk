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

package middleware

import "strings"

// Resource represents a protected API resource that can be accessed via API keys.
// Each resource corresponds to a specific API endpoint category.
type Resource string

// Action represents the allowed actions on a resource.
// Actions include read, write, delete, and wildcard (*).
type Action string

const (
	// Actions
	ActionRead   Action = "read"
	ActionWrite  Action = "write"
	ActionDelete Action = "delete"
	ActionAll    Action = "*"

	// Resources
	ResourceLedgers         Resource = "ledgers"
	ResourceBalances        Resource = "balances"
	ResourceAccounts        Resource = "accounts"
	ResourceIdentities      Resource = "identities"
	ResourceTransactions    Resource = "transactions"
	ResourceBalanceMonitors Resource = "balance-monitors"
	ResourceHooks           Resource = "hooks"
	ResourceAPIKeys         Resource = "api-keys"
	ResourceSearch          Resource = "search"
	ResourceReconciliation  Resource = "reconciliation"
	ResourceMetadata        Resource = "metadata"
	ResourceBackup          Resource = "backup"
	ResourceAll             Resource = "*"
)

// methodToAction maps HTTP methods to actions
var methodToAction = map[string]Action{
	"GET":    ActionRead,
	"HEAD":   ActionRead,
	"POST":   ActionWrite,
	"PUT":    ActionWrite,
	"PATCH":  ActionWrite,
	"DELETE": ActionDelete,
}

// BuildScope creates a scope string from resource and action
func BuildScope(resource Resource, action Action) string {
	return string(resource) + ":" + string(action)
}

// ParseScope parses a scope string into resource and action
func ParseScope(scope string) (Resource, Action) {
	parts := strings.Split(scope, ":")
	if len(parts) != 2 {
		return "", ""
	}
	return Resource(parts[0]), Action(parts[1])
}

// HasPermission checks if a set of scopes has permission for a given resource and HTTP method
func HasPermission(scopes []string, resource Resource, method string) bool {
	action := methodToAction[method]
	if action == "" {
		return false
	}

	for _, scope := range scopes {
		scopeResource, scopeAction := ParseScope(scope)

		// Check for wildcard resource
		if scopeResource == ResourceAll {
			if scopeAction == ActionAll || scopeAction == action {
				return true
			}
			continue
		}

		// Check for exact resource match
		if scopeResource == resource {
			if scopeAction == ActionAll || scopeAction == action {
				return true
			}
		}
	}
	return false
}
