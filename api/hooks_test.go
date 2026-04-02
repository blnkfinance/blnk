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

package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blnkfinance/blnk/internal/hooks"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/stretchr/testify/assert"
)

func TestRegisterHook(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/hooks", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Valid hook registration", func(t *testing.T) {
		hook := hooks.Hook{
			Name:    "Test Hook",
			URL:     "https://example.com/webhook",
			Type:    hooks.PreTransaction,
			Active:  true,
			Timeout: 30,
		}
		payloadBytes, _ := request.ToJsonReq(&hook)
		req := httptest.NewRequest("POST", "/hooks", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusCreated, resp.Code)
	})
}

func TestUpdateHook(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/hooks/hk_test123", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetHook(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Hook not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/hooks/hk_nonexistent", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestListHooks(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("List all hooks", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/hooks", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("List hooks by type", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/hooks?type=PRE_TRANSACTION", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)
	})
}

func TestDeleteHook(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Delete non-existent hook", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/hooks/hk_nonexistent", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}
