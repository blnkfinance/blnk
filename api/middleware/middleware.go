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

// RateLimitMiddleware creates a middleware for rate limiting using Tollbooth.
// It sets up rate limiting based on the configuration parameters and applies it to incoming requests.
//
// Parameters:
// - conf: The configuration object containing rate limit settings.
//
// Returns:
// - gin.HandlerFunc: A middleware function that applies rate limiting to requests.
func RateLimitMiddleware(conf *config.Configuration) gin.HandlerFunc {
	if conf.RateLimit.RequestsPerSecond == nil || conf.RateLimit.Burst == nil {
		// Rate limiting is disabled if RequestsPerSecond or Burst are not set.
		return func(c *gin.Context) {
			c.Next()
		}
	}

	rps := *conf.RateLimit.RequestsPerSecond
	burst := *conf.RateLimit.Burst
	ttl := time.Duration(*conf.RateLimit.CleanupIntervalSec) * time.Second

	// Create a new Tollbooth limiter with the specified rate and expiration time.
	lmt := tollbooth.NewLimiter(rps, &limiter.ExpirableOptions{
		DefaultExpirationTTL: ttl,
	})
	lmt.SetBurst(burst)

	// Middleware function that applies rate limiting to requests.
	return func(c *gin.Context) {
		httpError := tollbooth.LimitByRequest(lmt, c.Writer, c.Request)
		if httpError != nil {
			// Respond with an error if the request exceeds the rate limit.
			c.AbortWithStatusJSON(httpError.StatusCode, gin.H{"error": httpError.Message})
			return
		}
		// Continue to the next handler if the request is within the limit.
		c.Next()
	}
}

// SecretKeyAuthMiddleware creates a middleware for validating secret keys.
// It checks the request header for a valid secret key and aborts the request if the key is missing or invalid.
//
// Returns:
// - gin.HandlerFunc: A middleware function that validates the secret key in the request.
func SecretKeyAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		conf, err := config.Fetch()
		if err != nil {
			// Respond with an error if fetching the configuration fails.
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Secret key is not configured"})
			return
		}
		secretKey := conf.Server.SecretKey
		if secretKey == "" {
			// Respond with an error if the secret key is not configured.
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Secret key is not configured"})
			return
		}

		clientSecret := c.GetHeader("X-Blnk-Key")

		if clientSecret == "" {
			// Respond with an error if the client secret key is missing.
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing secret key"})
			return
		}

		if !secureCompare(secretKey, clientSecret) {
			// Respond with an error if the client secret key is invalid.
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid secret key"})
			return
		}

		// Continue to the next handler if the secret key is valid.
		c.Next()
	}
}

// secureCompare performs a constant-time comparison of two strings to prevent timing attacks.
// It compares the two strings and returns true if they are equal, false otherwise.
//
// Parameters:
// - a: The first string to compare.
// - b: The second string to compare.
//
// Returns:
// - bool: True if the strings are equal, false otherwise.
func secureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
