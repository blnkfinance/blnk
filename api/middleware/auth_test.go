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

	expiredKey, err := blnkService.CreateAPIKey(context.Background(), "expired-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(-24*time.Hour))
	assert.NoError(t, err)

	revokedKey, err := blnkService.CreateAPIKey(context.Background(), "revoked-key", "test-owner", []string{"read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	err = blnkService.RevokeAPIKey(context.Background(), revokedKey.APIKeyID, revokedKey.OwnerID)
	assert.NoError(t, err)

	testKey, err := blnkService.CreateAPIKey(context.Background(), "test-key", "test-owner", []string{"ledgers:read", "ledgers:write"}, time.Now().Add(24*time.Hour))
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
						Secure:    true,
						SecretKey: "master-key",
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
						Secure:    true,
						SecretKey: "master-key",
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
						Secure:    true,
						SecretKey: "master-key",
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Authentication required. Use X-Blnk-Key header",
		},
		{
			name:   "Valid API key with correct permissions",
			path:   "/ledgers",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       testKey.Key,
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
