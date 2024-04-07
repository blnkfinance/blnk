package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/gin-gonic/gin"
)

func SecretKeyAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		conf, err := config.Fetch()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Secret key is not configured"})
			return
		}
		secretKey := conf.Server.SecretKey
		if secretKey == "" {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Secret key is not configured"})
			return
		}

		clientSecret := c.GetHeader("X-Blnk-Key")

		if clientSecret == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing secret key"})
			return
		}

		if !secureCompare(secretKey, clientSecret) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid secret key"})
			return
		}

		c.Next()
	}
}

func secureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
