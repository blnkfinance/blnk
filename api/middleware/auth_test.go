package middleware

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/gin-gonic/gin"
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

	metadataWriterKey, err := blnkService.CreateAPIKey(context.Background(), "metadata-writer", "test-owner", []string{"metadata:write"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	wildcardAllKey, err := blnkService.CreateAPIKey(context.Background(), "wildcard-all", "test-owner", []string{"*:*"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	wildcardWriteKey, err := blnkService.CreateAPIKey(context.Background(), "wildcard-write", "test-owner", []string{"*:write"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	metadataWildcardActionKey, err := blnkService.CreateAPIKey(context.Background(), "metadata-wildcard-action", "test-owner", []string{"metadata:*"}, time.Now().Add(24*time.Hour))
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
		{
			name:   "Near-miss master key is rejected",
			path:   "/ledgers",
			method: "GET",
			apiKey: "master-keX", // differs from SecretKey by one char
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure:    true,
						SecretKey: "master-key",
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Invalid API key",
		},
		{
			name:   "Empty configured secret key does not grant master access",
			path:   "/ledgers",
			method: "GET",
			apiKey: "anything", // no SecretKey configured -> must not be treated as master
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure:    true,
						SecretKey: "",
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Invalid API key",
		},
		{
			name:   "metadata:write scoped key allows POST /:id/metadata",
			path:   "/:id/metadata",
			method: "POST",
			apiKey: metadataWriterKey.Key,
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
			name:   "scoped key without metadata:write gets insufficient permissions not unknown resource",
			path:   "/:id/metadata",
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
			expectedError: "Insufficient permissions for metadata:write",
		},
		{
			name:   "*:* wildcard scoped key allows POST /:id/metadata",
			path:   "/:id/metadata",
			method: "POST",
			apiKey: wildcardAllKey.Key,
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
			name:   "*:write wildcard scoped key allows POST /:id/metadata",
			path:   "/:id/metadata",
			method: "POST",
			apiKey: wildcardWriteKey.Key,
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
			name:   "metadata:* wildcard scoped key allows POST /:id/metadata",
			path:   "/:id/metadata",
			method: "POST",
			apiKey: metadataWildcardActionKey.Key,
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
			name:   "*:* wildcard scoped key allows POST /ldg_123/metadata",
			path:   "/ldg_123/metadata",
			method: "POST",
			apiKey: wildcardAllKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
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

func TestAuthMiddleware_ReadScopedKeysCanPOSTQueryRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service, err := setupBlnk()
	assert.NoError(t, err)

	tests := []struct {
		name          string
		path          string
		body          string
		scopes        []string
		expectedCode  int
		expectedError string
	}{
		{
			name:         "ledgers filter accepts ledgers:read",
			path:         "/ledgers/filter",
			scopes:       []string{"ledgers:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:         "balances filter accepts balances:read",
			path:         "/balances/filter",
			scopes:       []string{"balances:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:         "accounts filter accepts accounts:read",
			path:         "/accounts/filter",
			scopes:       []string{"accounts:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:         "identities filter accepts identities:read",
			path:         "/identities/filter",
			scopes:       []string{"identities:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:         "transactions filter accepts transactions:read",
			path:         "/transactions/filter",
			scopes:       []string{"transactions:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:          "search collection rejects search:read without the resource scope",
			path:          "/search/transactions",
			scopes:        []string{"search:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for transactions:read",
		},
		{
			name:         "search collection accepts search:read plus the resource read scope",
			path:         "/search/transactions",
			body:         `{"q":"*","query_by":"reference"}`,
			scopes:       []string{"search:read", "transactions:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:          "identity search rejects search:read without identities:read",
			path:          "/search/identities",
			body:          `{"q":"*","query_by":"email_address"}`,
			scopes:        []string{"search:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for identities:read",
		},
		{
			name:         "identity search accepts identities:read",
			path:         "/search/identities",
			body:         `{"q":"*","query_by":"email_address"}`,
			scopes:       []string{"search:read", "identities:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:          "identity search still requires search:read",
			path:          "/search/identities",
			scopes:        []string{"identities:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for search:read",
		},
		{
			name:          "resource write scope does not permit search",
			path:          "/search/identities",
			scopes:        []string{"search:read", "identities:write"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for identities:read",
		},
		{
			name:          "unknown search collection is rejected",
			path:          "/search/accounts",
			scopes:        []string{"search:read", "accounts:read", "*:*"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for this search collection",
		},
		{
			name:         "reconciliations search uses reconciliation:read",
			path:         "/search/reconciliations",
			scopes:       []string{"search:read", "reconciliation:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:          "multi-search rejects search:read without each collection scope",
			path:          "/multi-search",
			body:          `{"searches":[{"collection":"identities","q":"*","query_by":"email_address"}]}`,
			scopes:        []string{"search:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for identities:read",
		},
		{
			name:         "multi-search accepts a batch the key can read",
			path:         "/multi-search",
			body:         `{"searches":[{"collection":"identities","q":"*","query_by":"email_address"},{"collection":"ledgers","q":"*","query_by":"name"}]}`,
			scopes:       []string{"search:read", "identities:read", "ledgers:read"},
			expectedCode: http.StatusOK,
		},
		{
			name:          "multi-search rejects the batch when any collection is forbidden",
			path:          "/multi-search",
			body:          `{"searches":[{"collection":"ledgers","q":"*","query_by":"name"},{"collection":"transactions","q":"*","query_by":"reference"}]}`,
			scopes:        []string{"search:read", "ledgers:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for transactions:read",
		},
		{
			name:          "multi-search rejects an unknown collection in the batch",
			path:          "/multi-search",
			body:          `{"searches":[{"collection":"ledgers","q":"*","query_by":"name"},{"collection":"secrets","q":"*"}]}`,
			scopes:        []string{"search:read", "ledgers:read", "*:*"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for this search collection",
		},
		{
			name:          "multi-search rejects a malformed batch",
			path:          "/multi-search",
			body:          `{bad`,
			scopes:        []string{"search:read", "*:*"},
			expectedCode:  http.StatusBadRequest,
			expectedError: "Invalid multi-search request",
		},
		{
			name:         "ledgers filter still accepts ledgers:write",
			path:         "/ledgers/filter",
			scopes:       []string{"ledgers:write"},
			expectedCode: http.StatusOK,
		},
		{
			name:         "transactions filter still accepts transactions:write",
			path:         "/transactions/filter",
			scopes:       []string{"transactions:write"},
			expectedCode: http.StatusOK,
		},
		{
			name:          "search write scope does not bypass the collection read scope",
			path:          "/search/transactions",
			scopes:        []string{"search:write"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for transactions:read",
		},
		{
			name:          "creating a transaction still requires transactions:write",
			path:          "/transactions",
			scopes:        []string{"transactions:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for transactions:write",
		},
		{
			name:          "refund with id filter still requires transactions:write",
			path:          "/refund-transaction/filter",
			scopes:        []string{"transactions:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for transactions:write",
		},
		{
			name:          "search reindex still requires search:write",
			path:          "/search/reindex",
			scopes:        []string{"search:read"},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for search:write",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := service.CreateAPIKey(context.Background(), t.Name(), "test-owner", tt.scopes, time.Now().Add(time.Hour))
			assert.NoError(t, err)

			config.ConfigStore.Store(&config.Configuration{
				Server: config.ServerConfig{Secure: true},
			})

			router := gin.New()
			router.Any(tt.path, NewAuthMiddleware(service).Authenticate(), func(c *gin.Context) {
				if tt.body != "" {
					raw, err := io.ReadAll(c.Request.Body)
					if err != nil || !strings.Contains(string(raw), `"q"`) {
						c.Status(http.StatusTeapot)
						return
					}
				}
				c.Status(http.StatusOK)
			})

			recorder := httptest.NewRecorder()
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			req := httptest.NewRequest(http.MethodPost, tt.path, body)
			req.Header.Set(KeyHeader, key.Key)
			router.ServeHTTP(recorder, req)

			assert.Equal(t, tt.expectedCode, recorder.Code)
			if tt.expectedError != "" {
				assert.Contains(t, recorder.Body.String(), tt.expectedError)
			}
		})
	}
}

func TestAuthorizeSearchCollections(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		scopes     []string
		wantNil    bool
		wantCode   apierror.ErrorCode
		wantPhrase string
	}{
		{
			name:       "identities require identities:read",
			method:     http.MethodPost,
			path:       "/search/identities",
			scopes:     []string{"search:read"},
			wantCode:   apierror.ErrAuthInsufficientPermissions,
			wantPhrase: "identities:read",
		},
		{
			name:    "identities:read allows the identities collection",
			method:  http.MethodPost,
			path:    "/search/identities",
			scopes:  []string{"identities:read"},
			wantNil: true,
		},
		{
			name:       "get is held to the same collection scope",
			method:     http.MethodGet,
			path:       "/search/balances",
			scopes:     []string{"search:read"},
			wantCode:   apierror.ErrAuthInsufficientPermissions,
			wantPhrase: "balances:read",
		},
		{
			name:    "wildcard read covers an allowlisted collection",
			method:  http.MethodPost,
			path:    "/search/ledgers",
			scopes:  []string{"*:read"},
			wantNil: true,
		},
		{
			name:    "resource wildcard covers that collection",
			method:  http.MethodPost,
			path:    "/search/transactions",
			scopes:  []string{"transactions:*"},
			wantNil: true,
		},
		{
			name:       "write does not grant collection read",
			method:     http.MethodPost,
			path:       "/search/transactions",
			scopes:     []string{"transactions:write"},
			wantCode:   apierror.ErrAuthInsufficientPermissions,
			wantPhrase: "transactions:read",
		},
		{
			name:       "full access still cannot leave the allowlist",
			method:     http.MethodPost,
			path:       "/search/custom_pii",
			scopes:     []string{"*:*"},
			wantCode:   apierror.ErrAuthInsufficientPermissions,
			wantPhrase: "this search collection",
		},
		{
			name:    "reconciliations map to reconciliation:read",
			method:  http.MethodPost,
			path:    "/search/reconciliations",
			scopes:  []string{"reconciliation:read"},
			wantNil: true,
		},
		{
			name:    "reindex is not a collection search",
			method:  http.MethodPost,
			path:    "/search/reindex",
			scopes:  []string{"search:read"},
			wantNil: true,
		},
		{
			name:    "multi-search authorizes every search",
			method:  http.MethodPost,
			path:    "/multi-search",
			body:    `{"searches":[{"collection":"balances","q":"*"},{"collection":"identities","q":"*"}]}`,
			scopes:  []string{"balances:read", "identities:*"},
			wantNil: true,
		},
		{
			name:       "multi-search stops on the first forbidden collection",
			method:     http.MethodPost,
			path:       "/multi-search",
			body:       `{"searches":[{"collection":"balances","q":"*"},{"collection":"identities","q":"*"}]}`,
			scopes:     []string{"balances:read"},
			wantCode:   apierror.ErrAuthInsufficientPermissions,
			wantPhrase: "identities:read",
		},
		{
			name:       "multi-search rejects a collection outside the allowlist",
			method:     http.MethodPost,
			path:       "/multi-search",
			body:       `{"searches":[{"collection":"balances","q":"*"},{"collection":"accounts","q":"*"}]}`,
			scopes:     []string{"*:*"},
			wantCode:   apierror.ErrAuthInsufficientPermissions,
			wantPhrase: "this search collection",
		},
		{
			name:     "multi-search rejects an empty batch",
			method:   http.MethodPost,
			path:     "/multi-search",
			body:     `{"searches":[]}`,
			scopes:   []string{"*:*"},
			wantCode: apierror.ErrGenMalformedRequest,
		},
		{
			name:     "multi-search rejects malformed json",
			method:   http.MethodPost,
			path:     "/multi-search",
			body:     `{bad`,
			scopes:   []string{"*:*"},
			wantCode: apierror.ErrGenMalformedRequest,
		},
		{
			name:    "other resources are unchanged",
			method:  http.MethodPost,
			path:    "/ledgers/filter",
			scopes:  []string{"ledgers:read"},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			c.Request = httptest.NewRequest(tt.method, tt.path, body)

			failure := authorizeSearchCollections(c, tt.scopes)
			if tt.wantNil {
				assert.Nil(t, failure)
				if tt.body != "" {
					raw, err := io.ReadAll(c.Request.Body)
					assert.NoError(t, err)
					assert.JSONEq(t, tt.body, string(raw))
				}
				return
			}

			if assert.NotNil(t, failure) {
				assert.Equal(t, tt.wantCode, failure.code)
				if tt.wantPhrase != "" {
					assert.Contains(t, failure.message, tt.wantPhrase)
				}
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
			name:     "/:id/metadata path",
			path:     "/:id/metadata",
			expected: ResourceMetadata,
		},
		{
			name:     "/ldg_123/metadata entity path",
			path:     "/ldg_123/metadata",
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
		{
			name:     "Wildcard resource with matching action",
			scopes:   []string{"*:read"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: true,
		},
		{
			name:     "Wildcard resource with all actions",
			scopes:   []string{"*:*"},
			resource: ResourceTransactions,
			method:   "DELETE",
			expected: true,
		},
		{
			name:     "Wildcard resource with wrong action",
			scopes:   []string{"*:read"},
			resource: ResourceLedgers,
			method:   "DELETE",
			expected: false,
		},
		{
			name:     "Resource with wildcard action",
			scopes:   []string{"ledgers:*"},
			resource: ResourceLedgers,
			method:   "DELETE",
			expected: true,
		},
		{
			name:     "HEAD method maps to read",
			scopes:   []string{"ledgers:read"},
			resource: ResourceLedgers,
			method:   "HEAD",
			expected: true,
		},
		{
			name:     "PUT method maps to write",
			scopes:   []string{"accounts:write"},
			resource: ResourceAccounts,
			method:   "PUT",
			expected: true,
		},
		{
			name:     "PATCH method maps to write",
			scopes:   []string{"accounts:write"},
			resource: ResourceAccounts,
			method:   "PATCH",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasPermission(tt.scopes, tt.resource, tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildScope(t *testing.T) {
	tests := []struct {
		name     string
		resource Resource
		action   Action
		expected string
	}{
		{
			name:     "ledgers read",
			resource: ResourceLedgers,
			action:   ActionRead,
			expected: "ledgers:read",
		},
		{
			name:     "accounts write",
			resource: ResourceAccounts,
			action:   ActionWrite,
			expected: "accounts:write",
		},
		{
			name:     "transactions delete",
			resource: ResourceTransactions,
			action:   ActionDelete,
			expected: "transactions:delete",
		},
		{
			name:     "wildcard resource with all actions",
			resource: ResourceAll,
			action:   ActionAll,
			expected: "*:*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildScope(tt.resource, tt.action)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseScope(t *testing.T) {
	tests := []struct {
		name             string
		scope            string
		expectedResource Resource
		expectedAction   Action
	}{
		{
			name:             "Valid scope ledgers:read",
			scope:            "ledgers:read",
			expectedResource: ResourceLedgers,
			expectedAction:   ActionRead,
		},
		{
			name:             "Valid scope accounts:write",
			scope:            "accounts:write",
			expectedResource: ResourceAccounts,
			expectedAction:   ActionWrite,
		},
		{
			name:             "Invalid scope - no colon",
			scope:            "ledgersread",
			expectedResource: "",
			expectedAction:   "",
		},
		{
			name:             "Invalid scope - too many parts",
			scope:            "ledgers:read:extra",
			expectedResource: "",
			expectedAction:   "",
		},
		{
			name:             "Empty scope",
			scope:            "",
			expectedResource: "",
			expectedAction:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource, action := ParseScope(tt.scope)
			assert.Equal(t, tt.expectedResource, resource)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestScopeCovers(t *testing.T) {
	tests := []struct {
		name           string
		grantedScope   string
		requestedScope string
		expected       bool
	}{
		{
			name:           "Exact scope match",
			grantedScope:   "ledgers:read",
			requestedScope: "ledgers:read",
			expected:       true,
		},
		{
			name:           "Resource wildcard covers exact scope",
			grantedScope:   "*:write",
			requestedScope: "accounts:write",
			expected:       true,
		},
		{
			name:           "Action wildcard covers resource action",
			grantedScope:   "ledgers:*",
			requestedScope: "ledgers:delete",
			expected:       true,
		},
		{
			name:           "Exact resource scope does not cover wildcard request",
			grantedScope:   "ledgers:read",
			requestedScope: "ledgers:*",
			expected:       false,
		},
		{
			name:           "Different action is denied",
			grantedScope:   "ledgers:read",
			requestedScope: "ledgers:write",
			expected:       false,
		},
		{
			name:           "Invalid requested scope is denied",
			grantedScope:   "ledgers:*",
			requestedScope: "not-a-scope",
			expected:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ScopeCovers(tt.grantedScope, tt.requestedScope)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCanGrantScopes(t *testing.T) {
	tests := []struct {
		name            string
		callerScopes    []string
		requestedScopes []string
		expected        bool
	}{
		{
			name:            "All requested scopes are covered",
			callerScopes:    []string{"api-keys:read", "api-keys:write", "ledgers:*"},
			requestedScopes: []string{"api-keys:read", "ledgers:delete"},
			expected:        true,
		},
		{
			name:            "Wildcard caller can grant wildcard request",
			callerScopes:    []string{"*:*"},
			requestedScopes: []string{"accounts:delete"},
			expected:        true,
		},
		{
			name:            "Broader requested scope is denied",
			callerScopes:    []string{"api-keys:write"},
			requestedScopes: []string{"*:*"},
			expected:        false,
		},
		{
			name:            "Different resource is denied",
			callerScopes:    []string{"ledgers:*"},
			requestedScopes: []string{"accounts:read"},
			expected:        false,
		},
		{
			name:            "Invalid requested scope is denied",
			callerScopes:    []string{"ledgers:*"},
			requestedScopes: []string{"invalid"},
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanGrantScopes(tt.callerScopes, tt.requestedScopes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("Rate limiting disabled when config is nil", func(t *testing.T) {
		conf := &config.Configuration{
			RateLimit: config.RateLimitConfig{
				RequestsPerSecond: nil,
				Burst:             nil,
			},
		}

		router := gin.New()
		router.Use(RateLimitMiddleware(conf))
		router.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/test", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("Rate limiting enabled", func(t *testing.T) {
		rps := 100.0
		burst := 10
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
		router.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/test", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestInjectAPIKeyToMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("Non-POST request skips injection", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("GET", "/", nil)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})

	t.Run("Nil body skips injection", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/", nil)
		c.Request.Body = nil

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})

	t.Run("Invalid JSON returns error", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := strings.NewReader("not valid json")
		c.Request = httptest.NewRequest("POST", "/", body)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.Error(t, err)
	})

	t.Run("Valid JSON without meta_data creates it", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := strings.NewReader(`{"name": "test"}`)
		c.Request = httptest.NewRequest("POST", "/", body)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})

	t.Run("Valid JSON with existing meta_data updates it", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := strings.NewReader(`{"name": "test", "meta_data": {"existing": "value"}}`)
		c.Request = httptest.NewRequest("POST", "/", body)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})
}
