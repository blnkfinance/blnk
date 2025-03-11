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

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
)

const (
	KeyHeader = "X-Blnk-Key"
)

// pathToResource maps URL paths to their corresponding resource types.
// This is used by the authentication middleware to determine the required permissions.
var pathToResource = map[string]Resource{
	"ledgers":          ResourceLedgers,
	"balances":         ResourceBalances,
	"accounts":         ResourceAccounts,
	"identities":       ResourceIdentities,
	"transactions":     ResourceTransactions,
	"balance-monitors": ResourceBalanceMonitors,
	"hooks":            ResourceHooks,
	"api-keys":         ResourceAPIKeys,
	"search":           ResourceSearch,
	"reconciliation":   ResourceReconciliation,
	"metadata":         ResourceMetadata,
	"backup":           ResourceBackup,
}

// AuthMiddleware handles authentication and authorization for API routes.
// It supports both master key and API key authentication using the X-Blnk-Key header.
type AuthMiddleware struct {
	service *blnk.Blnk
}

// NewAuthMiddleware creates a new instance of AuthMiddleware.
//
// Parameters:
// - blnk: The Blnk service used to validate API keys.
//
// Returns:
// - *AuthMiddleware: A new instance of the authentication middleware.
func NewAuthMiddleware(blnk *blnk.Blnk) *AuthMiddleware {
	return &AuthMiddleware{service: blnk}
}

// getResourceFromPath determines the resource type from the URL path.
//
// Parameters:
// - path: The URL path to analyze.
//
// Returns:
// - Resource: The determined resource type, or empty string if not found.
func getResourceFromPath(path string) Resource {
	// Remove leading slash and get first path segment
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}

	// Special case for mocked-account
	if parts[0] == "mocked-account" {
		return ResourceAccounts
	}

	// Special case for multi-search
	if parts[0] == "multi-search" {
		return ResourceSearch
	}

	if parts[0] == "refund-transaction" {
		return ResourceTransactions
	}

	// Check if the path segment maps to a known resource
	if resource, ok := pathToResource[parts[0]]; ok {
		return resource
	}

	return ""
}

// Authenticate returns a middleware function that handles authentication and authorization for all routes.
// It checks for the X-Blnk-Key header and validates it against either the master key or API keys.
// For API keys, it verifies the key's validity and checks permissions based on the resource and HTTP method.
//
// Returns:
// - gin.HandlerFunc: A middleware function that performs the authentication.
//
// Responses:
// - 200 OK: When authentication succeeds.
// - 401 Unauthorized: When the API key is missing or invalid.
// - 403 Forbidden: When the API key lacks sufficient permissions.
func (m *AuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip auth for root path
		if c.Request.URL.Path == "/" {
			c.Next()
			return
		}

		// Check if secure mode is enabled
		conf, err := config.Fetch()
		if err == nil && !conf.Server.Secure {
			// Skip authentication when secure mode is disabled
			c.Next()
			return
		}

		key := extractKey(c)
		if key == "" {
			c.JSON(401, gin.H{"error": "Authentication required. Use X-Blnk-Key header"})
			c.Abort()
			return
		}

		// First check if it's the master key
		if err == nil && conf.Server.SecretKey == key {
			// Master key has all permissions
			c.Set("isMasterKey", true)
			c.Next()
			return
		}

		// If not master key, try API key authentication
		apiKey, err := m.service.GetAPIKeyByKey(c.Request.Context(), key)
		if err != nil {
			c.JSON(401, gin.H{"error": "Invalid API key"})
			c.Abort()
			return
		}

		if !apiKey.IsValid() {
			c.JSON(401, gin.H{"error": "API key is expired or revoked"})
			c.Abort()
			return
		}

		// Determine required resource from path
		resource := getResourceFromPath(c.Request.URL.Path)
		if resource == "" {
			c.JSON(403, gin.H{"error": "Unknown resource type"})
			c.Abort()
			return
		}

		// Check if API key has permission for this resource and method
		if !HasPermission(apiKey.Scopes, resource, c.Request.Method) {
			// Get the required action for this method
			action := methodToAction[c.Request.Method]
			c.JSON(403, gin.H{"error": "Insufficient permissions for " + string(resource) + ":" + string(action)})
			c.Abort()
			return
		}

		// Update last used timestamp in background
		go func() {
			_ = m.service.UpdateLastUsed(c.Request.Context(), apiKey.APIKeyID)
		}()

		c.Set("apiKey", apiKey)
		c.Next()
	}
}

// extractKey retrieves the authentication key from the X-Blnk-Key header.
//
// Parameters:
// - c: The Gin context containing the request headers.
//
// Returns:
// - string: The authentication key, or empty string if not found.
func extractKey(c *gin.Context) string {
	return c.GetHeader(KeyHeader)
}
