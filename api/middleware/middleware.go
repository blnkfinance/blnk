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
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/didip/tollbooth/v7"
	"github.com/didip/tollbooth/v7/limiter"
	"github.com/gin-gonic/gin"
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
		c.Next()
	}
}

// SecurityHeaders sets security headers to the response.
// It sets the following headers:
// - X-Content-Type-Options: nosniff
// - X-Frame-Options: DENY
// - Referrer-Policy: strict-origin-when-cross-origin
// - Content-Security-Policy: default-src 'none'; frame-ancestors 'none'
// - Cache-Control: no-store
// - Strict-Transport-Security: max-age=31536000; includeSubDomains
//
// Returns:
// - gin.HandlerFunc: A middleware function that sets security headers to the response.
func SecurityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		c.Header("Cache-Control", "no-store")
		isTLS := c.Request.TLS != nil
		if !isTLS {
			xfp := strings.ToLower(c.GetHeader("X-Forwarded-Proto"))
			if xfp == "https" {
				isTLS = true
			}
		}
		if isTLS {
			c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}

		c.Next()
	}
}
