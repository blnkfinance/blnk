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


