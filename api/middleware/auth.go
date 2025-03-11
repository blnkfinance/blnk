package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
)

const (
	APIKeyHeader = "X-Blnk-Key"
)

type AuthMiddleware struct {
	service *blnk.Blnk
}

func NewAuthMiddleware(blnk *blnk.Blnk) *AuthMiddleware {
	return &AuthMiddleware{service: blnk}
}

// Authenticate handles both master key and API key authentication
func (m *AuthMiddleware) Authenticate(requiredScopes ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
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

		// Check required scopes for API key
		for _, scope := range requiredScopes {
			if !apiKey.HasScope(scope) {
				c.JSON(403, gin.H{"error": "Insufficient permissions"})
				c.Abort()
				return
			}
		}

		// Update last used timestamp in background
		go func() {
			_ = m.service.UpdateLastUsed(c.Request.Context(), apiKey.APIKeyID)
		}()

		c.Set("apiKey", apiKey)
		c.Next()
	}
}

// extractKey gets the key from X-Blnk-Key header
func extractKey(c *gin.Context) string {
	return c.GetHeader(APIKeyHeader)
}
