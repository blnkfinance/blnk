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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/gin-gonic/gin"
)

func installAuthConfig(t *testing.T, secure bool, secret string) {
	t.Helper()
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://localhost/blnk"},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		Server:     config.ServerConfig{Secure: secure, SecretKey: secret},
	})
	loaded, err := config.Fetch()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Server.Secure != secure || loaded.Server.SecretKey != secret {
		t.Fatal("auth config was not stored")
	}
}

func TestSecureModeBlocksUnauthenticatedLedgerRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	installAuthConfig(t, true, "master-secret")
	middleware := NewAuthMiddleware(nil)

	protected := []string{"/transactions", "/identities", "/backup", "/hooks"}
	for _, path := range protected {
		t.Run(path+" without a key", func(t *testing.T) {
			router := gin.New()
			router.POST(path, middleware.Authenticate(), func(c *gin.Context) {
				c.Status(http.StatusOK)
			})
			request := httptest.NewRequest(http.MethodPost, path, nil)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
		})

		t.Run(path+" with the master key", func(t *testing.T) {
			router := gin.New()
			router.POST(path, middleware.Authenticate(), func(c *gin.Context) {
				c.Status(http.StatusOK)
			})
			request := httptest.NewRequest(http.MethodPost, path, nil)
			request.Header.Set("X-Blnk-Key", "master-secret")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
		})
	}

	t.Run("health stays open", func(t *testing.T) {
		router := gin.New()
		router.GET("/health", middleware.Authenticate(), func(c *gin.Context) {
			c.Status(http.StatusOK)
		})
		request := httptest.NewRequest(http.MethodGet, "/health", nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("health status = %d", recorder.Code)
		}
	})
}

func TestInsecureModeStillSkipsAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	installAuthConfig(t, false, "")
	router := gin.New()
	router.POST("/transactions", NewAuthMiddleware(nil).Authenticate(), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	request := httptest.NewRequest(http.MethodPost, "/transactions", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("insecure mode status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}
