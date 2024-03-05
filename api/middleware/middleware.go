package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/gin-gonic/gin"
)

func SecretKeyAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Retrieve the secret key from Redis
		conf, err := config.Fetch()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Secret key is not configured"})
			return
		}
		secretKey := conf.Server.SecretKey
		// Ensure the secret key is not empty
		if secretKey == "" {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Secret key is not configured"})
			return
		}

		// Get secret key from request headers
		clientSecret := c.GetHeader("X-Secret-Key")

		// Ensure the client secret is not empty
		if clientSecret == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing secret key"})
			return
		}

		// Secure comparison to prevent timing attacks
		if !secureCompare(secretKey, clientSecret) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid secret key"})
			return
		}

		c.Next()
	}
}

func secureCompare(a, b string) bool {
	// Use a constant time compare function to securely compare the strings
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
