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

// MetricsAuth returns a middleware that controls access to the /metrics endpoint.
//
// Behavior based on secure mode and token configuration:
//   - Secure mode OFF, no token: open access (no auth required)
//   - Secure mode OFF, token set: require bearer token
//   - Secure mode ON, token set: require bearer token
//   - Secure mode ON, no token: block all access (misconfiguration)
//
// When authentication is required, requests must include "Authorization: Bearer <token>".
// This uses the standard Authorization header that Prometheus natively supports via
// its scrape_configs authorization block.
func MetricsAuth(secure bool, token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if secure && token == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "Metrics endpoint unavailable: metrics_bearer_token must be configured when secure mode is enabled",
			})
			return
		}

		if token == "" {
			c.Next()
			return
		}

		auth := c.GetHeader("Authorization")
		if auth == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization required for metrics endpoint"})
			return
		}

		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header must use Bearer scheme"})
			return
		}

		provided := auth[len(prefix):]
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid bearer token"})
			return
		}

		c.Next()
	}
}

// MetricsAuthHandler wraps an http.Handler with bearer token authentication.
// This is the non-Gin equivalent of MetricsAuth, used for the worker monitoring server
// which uses a standard http.ServeMux instead of Gin.
// Same secure mode logic as MetricsAuth: blocks access when secure=true and token is empty.
func MetricsAuthHandler(secure bool, token string, next http.Handler) http.Handler {
	if secure && token == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"Metrics endpoint unavailable: metrics_bearer_token must be configured when secure mode is enabled"}`))
		})
	}

	if token == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			http.Error(w, `{"error":"Authorization required for metrics endpoint"}`, http.StatusUnauthorized)
			return
		}

		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			http.Error(w, `{"error":"Authorization header must use Bearer scheme"}`, http.StatusUnauthorized)
			return
		}

		provided := auth[len(prefix):]
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			http.Error(w, `{"error":"Invalid bearer token"}`, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
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
