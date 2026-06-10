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
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMetricsRouter builds a gin router protected by MetricsAuth. The handler
// flips reached so tests can assert the protected handler is never executed
// on a rejected request.
func newMetricsRouter(secure bool, token string, reached *bool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/metrics", MetricsAuth(secure, token), func(c *gin.Context) {
		*reached = true
		c.String(http.StatusOK, "metrics-ok")
	})
	return router
}

func TestMetricsAuth_TokenMatrix(t *testing.T) {
	const configuredToken = "s3cr3t-metrics-token"

	tests := []struct {
		name          string
		secure        bool
		token         string
		authHeader    string
		setHeader     bool
		expectedCode  int
		expectedBody  string
		handlerCalled bool
	}{
		// --- secure mode ON, token missing: hard lockdown (misconfiguration) ---
		{
			name:          "secure on, no token configured, no header -> 403",
			secure:        true,
			token:         "",
			expectedCode:  http.StatusForbidden,
			expectedBody:  "metrics_bearer_token must be configured",
			handlerCalled: false,
		},
		{
			name:          "secure on, no token configured, attacker supplies a bearer token anyway -> still 403",
			secure:        true,
			token:         "",
			setHeader:     true,
			authHeader:    "Bearer anything",
			expectedCode:  http.StatusForbidden,
			handlerCalled: false,
		},
		// --- open access: secure OFF and no token ---
		{
			name:          "secure off, no token, no header -> open access",
			secure:        false,
			token:         "",
			expectedCode:  http.StatusOK,
			handlerCalled: true,
		},
		{
			name:          "secure off, no token, garbage header is ignored -> open access",
			secure:        false,
			token:         "",
			setHeader:     true,
			authHeader:    "Bearer junk",
			expectedCode:  http.StatusOK,
			handlerCalled: true,
		},
		// --- token configured (secure ON): full adversarial header matrix ---
		{
			name:          "missing Authorization header -> 401",
			secure:        true,
			token:         configuredToken,
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Authorization required",
			handlerCalled: false,
		},
		{
			name:          "empty Authorization header value -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "",
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "bare 'Bearer' with no token -> 401 (scheme rejected, prefix needs trailing space)",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer",
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Bearer scheme",
			handlerCalled: false,
		},
		{
			name:          "'Bearer ' with empty token -> 401 invalid token",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer ",
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Invalid bearer token",
			handlerCalled: false,
		},
		{
			name:          "lowercase 'bearer' scheme -> 401 (scheme is case-sensitive here)",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "bearer " + configuredToken,
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Bearer scheme",
			handlerCalled: false,
		},
		{
			name:          "uppercase 'BEARER' scheme -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "BEARER " + configuredToken,
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "Basic scheme with the right token inside -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Basic " + configuredToken,
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Bearer scheme",
			handlerCalled: false,
		},
		{
			name:          "double space between scheme and token -> 401 (leading whitespace is part of token)",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer  " + configuredToken,
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Invalid bearer token",
			handlerCalled: false,
		},
		{
			name:          "trailing whitespace after token -> 401 (no trimming, exact match only)",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer " + configuredToken + " ",
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Invalid bearer token",
			handlerCalled: false,
		},
		{
			name:          "wrong token -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer totally-wrong",
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Invalid bearer token",
			handlerCalled: false,
		},
		{
			name:          "prefix of the real token -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer s3cr3t",
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "real token plus suffix -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer " + configuredToken + "x",
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "correct bearer token -> 200",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer " + configuredToken,
			expectedCode:  http.StatusOK,
			handlerCalled: true,
		},
		// --- token configured but secure OFF: token must still be enforced ---
		{
			name:          "secure off but token configured, no header -> 401 (token wins over insecure mode)",
			secure:        false,
			token:         configuredToken,
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "secure off but token configured, correct token -> 200",
			secure:        false,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer " + configuredToken,
			expectedCode:  http.StatusOK,
			handlerCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reached := false
			router := newMetricsRouter(tt.secure, tt.token, &reached)

			req, err := http.NewRequest(http.MethodGet, "/metrics", nil)
			require.NoError(t, err)
			if tt.setHeader {
				req.Header.Set("Authorization", tt.authHeader)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedCode, w.Code)
			assert.Equal(t, tt.handlerCalled, reached, "protected handler invocation mismatch")
			if tt.expectedBody != "" {
				assert.Contains(t, w.Body.String(), tt.expectedBody)
			}
		})
	}
}

func TestMetricsAuth_TokenInQueryStringIsNotAccepted(t *testing.T) {
	// Tokens must never be accepted via the URL: they end up in access logs.
	reached := false
	router := newMetricsRouter(true, "qtoken", &reached)

	req, err := http.NewRequest(http.MethodGet, "/metrics?access_token=qtoken&token=qtoken", nil)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.False(t, reached)
}

func TestMetricsAuthHandler_TokenMatrix(t *testing.T) {
	const configuredToken = "worker-metrics-token"

	tests := []struct {
		name          string
		secure        bool
		token         string
		authHeader    string
		setHeader     bool
		expectedCode  int
		expectedBody  string
		handlerCalled bool
	}{
		{
			name:          "secure on, no token -> 403 lockdown",
			secure:        true,
			token:         "",
			expectedCode:  http.StatusForbidden,
			expectedBody:  "metrics_bearer_token must be configured",
			handlerCalled: false,
		},
		{
			name:          "secure off, no token -> open access (next handler returned untouched)",
			secure:        false,
			token:         "",
			expectedCode:  http.StatusOK,
			handlerCalled: true,
		},
		{
			name:          "token set, no header -> 401",
			secure:        true,
			token:         configuredToken,
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Authorization required",
			handlerCalled: false,
		},
		{
			name:          "token set, bare Bearer -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer",
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Bearer scheme",
			handlerCalled: false,
		},
		{
			name:          "token set, lowercase bearer -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "bearer " + configuredToken,
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "token set, wrong token -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer nope",
			expectedCode:  http.StatusUnauthorized,
			expectedBody:  "Invalid bearer token",
			handlerCalled: false,
		},
		{
			name:          "token set, extra interior whitespace -> 401",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer  " + configuredToken,
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
		{
			name:          "token set, correct token -> 200",
			secure:        true,
			token:         configuredToken,
			setHeader:     true,
			authHeader:    "Bearer " + configuredToken,
			expectedCode:  http.StatusOK,
			handlerCalled: true,
		},
		{
			name:          "secure off, token set, missing header -> still 401",
			secure:        false,
			token:         configuredToken,
			expectedCode:  http.StatusUnauthorized,
			handlerCalled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reached := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("metrics-ok"))
			})

			handler := MetricsAuthHandler(tt.secure, tt.token, next)

			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if tt.setHeader {
				req.Header.Set("Authorization", tt.authHeader)
			}

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedCode, w.Code)
			assert.Equal(t, tt.handlerCalled, reached, "next handler invocation mismatch")
			if tt.expectedBody != "" {
				assert.Contains(t, w.Body.String(), tt.expectedBody)
			}
		})
	}
}

func TestMetricsAuthHandler_LockdownResponseIsJSON(t *testing.T) {
	handler := MetricsAuthHandler(true, "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler must not be reached in lockdown mode")
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.JSONEq(t,
		`{
			"error": "Metrics endpoint unavailable: metrics_bearer_token must be configured when secure mode is enabled",
			"error_detail": {
				"code": "AUTH_METRICS_DISABLED",
				"message": "Metrics endpoint unavailable: metrics_bearer_token must be configured when secure mode is enabled"
			}
		}`,
		w.Body.String())
}

func TestSecurityHeaders_AllHeadersOnPlainHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(SecurityHeaders())
	router.GET("/anything", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	headers := w.Header()
	assert.Equal(t, "nosniff", headers.Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", headers.Get("X-Frame-Options"))
	assert.Equal(t, "strict-origin-when-cross-origin", headers.Get("Referrer-Policy"))
	assert.Equal(t, "default-src 'none'; frame-ancestors 'none'", headers.Get("Content-Security-Policy"))
	assert.Equal(t, "no-store", headers.Get("Cache-Control"))
	// HSTS must NOT be sent over plain HTTP: browsers ignore it on http://
	// and sending it would falsely advertise TLS support.
	assert.Empty(t, headers.Get("Strict-Transport-Security"),
		"HSTS should not be set on a plain-HTTP request with no forwarded proto")
}

func TestSecurityHeaders_HSTSGating(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name            string
		tls             bool
		forwardedProto  string
		expectHSTS      bool
		expectHSTSValue string
	}{
		{
			name:            "direct TLS connection -> HSTS set",
			tls:             true,
			expectHSTS:      true,
			expectHSTSValue: "max-age=31536000; includeSubDomains",
		},
		{
			name:            "behind proxy with X-Forwarded-Proto https -> HSTS set",
			forwardedProto:  "https",
			expectHSTS:      true,
			expectHSTSValue: "max-age=31536000; includeSubDomains",
		},
		{
			name:            "X-Forwarded-Proto HTTPS uppercase -> HSTS set (case-insensitive)",
			forwardedProto:  "HTTPS",
			expectHSTS:      true,
			expectHSTSValue: "max-age=31536000; includeSubDomains",
		},
		{
			name:           "X-Forwarded-Proto http -> no HSTS",
			forwardedProto: "http",
			expectHSTS:     false,
		},
		{
			name:           "spoofable garbage forwarded proto -> no HSTS",
			forwardedProto: "https,",
			expectHSTS:     false,
		},
		{
			name:       "no TLS, no forwarded proto -> no HSTS",
			expectHSTS: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.Use(SecurityHeaders())
			router.GET("/h", func(c *gin.Context) { c.Status(http.StatusOK) })

			req := httptest.NewRequest(http.MethodGet, "/h", nil)
			if tt.tls {
				req.TLS = &tls.ConnectionState{}
			}
			if tt.forwardedProto != "" {
				req.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			hsts := w.Header().Get("Strict-Transport-Security")
			if tt.expectHSTS {
				assert.Equal(t, tt.expectHSTSValue, hsts)
			} else {
				assert.Empty(t, hsts)
			}
		})
	}
}

func TestSecurityHeaders_PresentOnErrorResponses(t *testing.T) {
	// Security headers are set before c.Next(), so they must be present
	// even when the downstream handler fails or the route 404s.
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(SecurityHeaders())
	router.GET("/boom", func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "boom"})
	})

	t.Run("500 from handler", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/boom", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
		assert.Equal(t, "DENY", w.Header().Get("X-Frame-Options"))
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	})

	t.Run("404 unknown route", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
		assert.Equal(t, "default-src 'none'; frame-ancestors 'none'", w.Header().Get("Content-Security-Policy"))
	})
}

func TestRateLimitMiddleware_ExceededReturns429(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rps := 1.0
	burst := 1
	cleanup := 60
	conf := &config.Configuration{
		RateLimit: config.RateLimitConfig{
			RequestsPerSecond:  &rps,
			Burst:              &burst,
			CleanupIntervalSec: &cleanup,
		},
	}

	router := gin.New()
	router.Use(RateLimitMiddleware(conf))
	router.GET("/limited", func(c *gin.Context) { c.Status(http.StatusOK) })

	codes := map[int]int{}
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/limited", nil)
		req.RemoteAddr = "203.0.113.7:4242" // same client for every request
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		codes[w.Code]++
		if w.Code == http.StatusTooManyRequests {
			assert.Contains(t, w.Body.String(), "error", "429 response should carry an error body")
		}
	}

	assert.GreaterOrEqual(t, codes[http.StatusOK], 1, "at least the first request should pass")
	assert.GreaterOrEqual(t, codes[http.StatusTooManyRequests], 1,
		"burst of 1 at 1 rps must reject some of 5 immediate requests")
}

func TestRateLimitMiddleware_DisabledWhenBurstMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rps := 1.0
	conf := &config.Configuration{
		RateLimit: config.RateLimitConfig{
			RequestsPerSecond: &rps,
			Burst:             nil, // burst missing -> limiter disabled entirely
		},
	}

	router := gin.New()
	router.Use(RateLimitMiddleware(conf))
	router.GET("/open", func(c *gin.Context) { c.Status(http.StatusOK) })

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/open", nil)
		req.RemoteAddr = "203.0.113.8:4242"
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	}
}

func TestRateLimitMiddleware_NilCleanupIntervalDoesNotPanic(t *testing.T) {
	// Regression: RateLimitMiddleware used to dereference CleanupIntervalSec
	// without a nil check; it now falls back to config.DEFAULT_CLEANUP_SEC.
	gin.SetMode(gin.TestMode)
	rps := 10.0
	burst := 5
	conf := &config.Configuration{
		RateLimit: config.RateLimitConfig{
			RequestsPerSecond:  &rps,
			Burst:              &burst,
			CleanupIntervalSec: nil,
		},
	}

	assert.NotPanics(t, func() {
		router := gin.New()
		router.Use(RateLimitMiddleware(conf))
		router.GET("/t", func(c *gin.Context) { c.Status(http.StatusOK) })
		req := httptest.NewRequest(http.MethodGet, "/t", nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	})
}
