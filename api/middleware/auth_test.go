package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/stretchr/testify/assert"
)

func setupBlnk() (*blnk.Blnk, error) {
	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})
	cnf, err := config.Fetch()
	if err != nil {
		return nil, err
	}
	db, err := database.NewDataSource(cnf)
	if err != nil {
		return nil, err
	}

	return blnk.NewBlnk(db)
}

func TestAuthMiddleware_Authenticate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup blnk service
	blnkService, err := setupBlnk()
	assert.NoError(t, err)

	// Create test API keys
	validKey, err := blnkService.CreateAPIKey(context.Background(), "valid-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	insufficientKey, err := blnkService.CreateAPIKey(context.Background(), "insufficient-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	expiredKey, err := blnkService.CreateAPIKey(context.Background(), "expired-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(-24*time.Hour))
	assert.NoError(t, err)

	revokedKey, err := blnkService.CreateAPIKey(context.Background(), "revoked-key", "test-owner", []string{"read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	err = blnkService.RevokeAPIKey(context.Background(), revokedKey.APIKeyID, revokedKey.OwnerID)
	assert.NoError(t, err)

	allPermissionsScopes := []string{
		"ledgers:read", "ledgers:write", "ledgers:delete",
		"balances:read", "balances:write", "balances:delete",
		"accounts:read", "accounts:write", "accounts:delete",
		"identities:read", "identities:write", "identities:delete",
		"transactions:read", "transactions:write", "transactions:delete",
		"balance-monitors:read", "balance-monitors:write", "balance-monitors:delete",
		"hooks:read", "hooks:write", "hooks:delete",
		"api-keys:read", "api-keys:write", "api-keys:delete",
		"search:read", "search:write", "search:delete",
		"reconciliation:read", "reconciliation:write", "reconciliation:delete",
		"metadata:read", "metadata:write", "metadata:delete",
		"backup:read", "backup:write", "backup:delete",
	}

	comprehensiveKey, err := blnkService.CreateAPIKey(context.Background(), "comprehensive-key", "test-owner", allPermissionsScopes, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	tests := []struct {
		name          string
		path          string
		method        string
		apiKey        string
		expectedCode  int
		expectedError string
		setupConfig   func() *config.Configuration
	}{
		{
			name:   "Valid master key",
			path:   "/ledgers",
			method: "GET",
			apiKey: "master-key",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure:    true,
						SecretKey: "master-key",
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Root path",
			path:   "/",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Server health endpoint",
			path:   "/health",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Insufficient permissions",
			path:   "/ledgers",
			method: "POST",
			apiKey: insufficientKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for ledgers:write",
		},
		{
			name:   "Secure mode disabled",
			path:   "/ledgers",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: false,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Unknown resource type",
			path:   "/fake-resouce/dd",
			method: "POST",
			apiKey: validKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusForbidden,
		},
		{
			name:   "Valid API key with read permission",
			path:   "/ledgers",
			method: "GET",
			apiKey: validKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Invalid API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: "invalid-key",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Invalid API key",
		},
		{
			name:   "Expired API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: expiredKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "API key is expired or revoked",
		},
		{
			name:   "Revoked API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: revokedKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "API key is expired or revoked",
		},
		{
			name:   "Missing API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: "",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Authentication required. Use X-Blnk-Key header",
		},
		{
			name:   "Comprehensive key for GET /ledgers",
			path:   "/ledgers",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /ledgers",
			path:   "/ledgers",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for DELETE /ledgers",
			path:   "/ledgers",
			method: "DELETE",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /accounts",
			path:   "/accounts",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /accounts",
			path:   "/accounts",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /transactions",
			path:   "/transactions",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /transactions",
			path:   "/transactions",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /refund-transaction",
			path:   "/refund-transaction/:id",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /identities",
			path:   "/identities",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /identities",
			path:   "/identities",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /balances",
			path:   "/balances",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /balance-monitors",
			path:   "/balance-monitors",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /hooks",
			path:   "/hooks",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /api-keys",
			path:   "/api-keys",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /search",
			path:   "/search",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /multi-search",
			path:   "/multi-search",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /reconciliation",
			path:   "/reconciliation",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /metadata",
			path:   "/metadata",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for PATCH /metadata",
			path:   "/metadata",
			method: "PATCH",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /backup",
			path:   "/backup",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blnk, err := setupBlnk()
			if err != nil {
				t.Fatalf("Failed to setup blnk: %v", err)
			}

			// Create a new router and middleware
			router := gin.New()
			authMiddleware := NewAuthMiddleware(blnk)

			// Store test configuration
			if tt.setupConfig != nil {
				config.ConfigStore.Store(tt.setupConfig())
			}

			// Add test route with middleware
			router.Any(tt.path, authMiddleware.Authenticate(), func(c *gin.Context) {
				c.Status(http.StatusOK)
			})

			// Create test request
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(tt.method, tt.path, nil)
			if tt.apiKey != "" {
				req.Header.Set(KeyHeader, tt.apiKey)
			}

			// Serve the request
			router.ServeHTTP(w, req)

			// Assert response
			assert.Equal(t, tt.expectedCode, w.Code)
			if tt.expectedError != "" {
				assert.Contains(t, w.Body.String(), tt.expectedError)
			}
		})
	}
}

func TestGetResourceFromPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected Resource
	}{
		{
			name:     "Valid ledger path",
			path:     "/ledgers",
			expected: ResourceLedgers,
		},
		{
			name:     "Valid accounts path with ID",
			path:     "/accounts/123",
			expected: ResourceAccounts,
		},
		{
			name:     "Valid mocked account path",
			path:     "/mocked-account",
			expected: ResourceAccounts,
		},
		{
			name:     "Valid transactions path",
			path:     "/transactions",
			expected: ResourceTransactions,
		},
		{
			name:     "Valid identities path with ID",
			path:     "/identities/xyz",
			expected: ResourceIdentities,
		},
		{
			name:     "Valid balances path",
			path:     "/balances",
			expected: ResourceBalances,
		},
		{
			name:     "Valid balance-monitors path",
			path:     "/balance-monitors",
			expected: ResourceBalanceMonitors,
		},
		{
			name:     "Valid hooks path",
			path:     "/hooks",
			expected: ResourceHooks,
		},
		{
			name:     "Valid api-keys path",
			path:     "/api-keys",
			expected: ResourceAPIKeys,
		},
		{
			name:     "Valid search path",
			path:     "/search",
			expected: ResourceSearch,
		},
		{
			name:     "Valid reconciliation path",
			path:     "/reconciliation",
			expected: ResourceReconciliation,
		},
		{
			name:     "Valid metadata path",
			path:     "/metadata",
			expected: ResourceMetadata,
		},
		{
			name:     "Valid backup path",
			path:     "/backup",
			expected: ResourceBackup,
		},
		{
			name:     "Unknown resource",
			path:     "/unknown",
			expected: "",
		},
		{
			name:     "Empty path",
			path:     "",
			expected: "",
		},
		{
			name:     "Root path",
			path:     "/",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getResourceFromPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractKey(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{
			name:     "Valid key",
			header:   "test-key",
			expected: "test-key",
		},
		{
			name:     "Empty key",
			header:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("GET", "/", nil)
			if tt.header != "" {
				c.Request.Header.Set(KeyHeader, tt.header)
			}

			result := extractKey(c)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasPermission(t *testing.T) {
	tests := []struct {
		name     string
		scopes   []string
		resource Resource
		method   string
		expected bool
	}{
		{
			name:     "Exact resource and action match",
			scopes:   []string{"ledgers:read"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: true,
		},
		{
			name:     "Exact resource but wrong action",
			scopes:   []string{"ledgers:read"},
			resource: ResourceLedgers,
			method:   "POST",
			expected: false,
		},
		{
			name:     "Multiple explicit permissions - one matches",
			scopes:   []string{"ledgers:read", "accounts:write", "transactions:delete"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: true,
		},
		{
			name:     "Multiple explicit permissions - none match",
			scopes:   []string{"ledgers:write", "accounts:write", "transactions:read"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: false,
		},
		{
			name:     "Multiple explicit permissions - method match",
			scopes:   []string{"ledgers:write", "accounts:write", "transactions:read"},
			resource: ResourceLedgers,
			method:   "POST",
			expected: true,
		},
		{
			name:     "Unsupported HTTP method",
			scopes:   []string{"ledgers:read", "ledgers:write", "ledgers:delete"},
			resource: ResourceLedgers,
			method:   "CUSTOM",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasPermission(tt.scopes, tt.resource, tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}
