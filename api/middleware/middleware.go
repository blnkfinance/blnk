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
	"crypto/subtle"
	"net/http"
	"time"

	"github.com/didip/tollbooth/v7"
	"github.com/didip/tollbooth/v7/limiter"
	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/config"
)

// RateLimitMiddleware creates a middleware for rate limiting using Tollbooth
func RateLimitMiddleware(conf *config.Configuration) gin.HandlerFunc {
	if conf.RateLimit.RequestsPerSecond == nil || conf.RateLimit.Burst == nil {
		// Rate limiting is disabled
		return func(c *gin.Context) {
			c.Next()
		}
	}

	rps := *conf.RateLimit.RequestsPerSecond
	burst := *conf.RateLimit.Burst
	ttl := time.Duration(*conf.RateLimit.CleanupIntervalSec) * time.Second

	lmt := tollbooth.NewLimiter(rps, &limiter.ExpirableOptions{
		DefaultExpirationTTL: ttl,
	})
	lmt.SetBurst(burst)
	return func(c *gin.Context) {
		httpError := tollbooth.LimitByRequest(lmt, c.Writer, c.Request)
		if httpError != nil {
			c.AbortWithStatusJSON(httpError.StatusCode, gin.H{"error": httpError.Message})
			return
		}
		c.Next()
	}
}

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
