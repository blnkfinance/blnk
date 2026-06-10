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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiKeyRequestBody(t *testing.T, owner string, scopes []string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"name":       gofakeit.Name(),
		"scopes":     scopes,
		"owner":      owner,
		"expires_at": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
	})
	require.NoError(t, err)
	return body
}

func doJSONRequest(router *gin.Engine, method, route string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, route, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, route, nil)
	}
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

func TestCreateAPIKeyHTTP(t *testing.T) {
	t.Run("Master key creates for any owner", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, true, nil)
		owner := "owner_" + gofakeit.UUID()

		resp := doJSONRequest(router, "POST", "/api-keys", apiKeyRequestBody(t, owner, []string{"ledgers:read"}))
		assert.Equal(t, http.StatusCreated, resp.Code)

		var created model.APIKey
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &created))
		assert.NotEmpty(t, created.APIKeyID)
		assert.NotEmpty(t, created.Key)
		assert.Equal(t, owner, created.OwnerID)
	})

	t.Run("Missing required fields", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, true, nil)
		resp := doJSONRequest(router, "POST", "/api-keys", []byte(`{"name": "incomplete"}`))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("No principal and not master", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, false, nil)
		resp := doJSONRequest(router, "POST", "/api-keys", apiKeyRequestBody(t, "owner_x", []string{"ledgers:read"}))
		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})

	t.Run("Non-master cannot escalate scopes", func(t *testing.T) {
		principal := &model.APIKey{
			APIKeyID: "api_key_test",
			OwnerID:  "owner_principal",
			Scopes:   []string{"ledgers:read"},
		}
		router, _ := setupAuthedRouter(t, false, principal)
		resp := doJSONRequest(router, "POST", "/api-keys", apiKeyRequestBody(t, principal.OwnerID, []string{"transactions:write"}))
		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("Non-master cannot create for another owner", func(t *testing.T) {
		principal := &model.APIKey{
			APIKeyID: "api_key_test",
			OwnerID:  "owner_principal",
			Scopes:   []string{"ledgers:read"},
		}
		router, _ := setupAuthedRouter(t, false, principal)
		resp := doJSONRequest(router, "POST", "/api-keys", apiKeyRequestBody(t, "owner_other", []string{"ledgers:read"}))
		assert.Equal(t, http.StatusForbidden, resp.Code)
	})
}

func TestListAPIKeysHTTP(t *testing.T) {
	t.Run("Master lists keys for owner", func(t *testing.T) {
		router, b := setupAuthedRouter(t, true, nil)
		owner := "owner_" + gofakeit.UUID()
		_, err := b.CreateAPIKey(t.Context(), gofakeit.Name(), owner, []string{"ledgers:read"}, time.Now().Add(time.Hour))
		require.NoError(t, err)

		resp := doJSONRequest(router, "GET", fmt.Sprintf("/api-keys?owner=%s", owner), nil)
		assert.Equal(t, http.StatusOK, resp.Code)

		var keys []model.APIKey
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &keys))
		assert.Equal(t, 1, len(keys))
		assert.Equal(t, owner, keys[0].OwnerID)
	})

	t.Run("Master without owner is rejected", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, true, nil)
		resp := doJSONRequest(router, "GET", "/api-keys", nil)
		// Owner-required is payload validation, not authentication: 400.
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Non-master lists own keys", func(t *testing.T) {
		owner := "owner_" + gofakeit.UUID()
		principal := &model.APIKey{APIKeyID: "api_key_test", OwnerID: owner, Scopes: []string{"ledgers:read"}}
		router, b := setupAuthedRouter(t, false, principal)
		_, err := b.CreateAPIKey(t.Context(), gofakeit.Name(), owner, []string{"ledgers:read"}, time.Now().Add(time.Hour))
		require.NoError(t, err)

		resp := doJSONRequest(router, "GET", "/api-keys", nil)
		assert.Equal(t, http.StatusOK, resp.Code)

		var keys []model.APIKey
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &keys))
		assert.Equal(t, 1, len(keys))
	})

	t.Run("Non-master cannot list another owner", func(t *testing.T) {
		principal := &model.APIKey{APIKeyID: "api_key_test", OwnerID: "owner_principal", Scopes: []string{"ledgers:read"}}
		router, _ := setupAuthedRouter(t, false, principal)
		resp := doJSONRequest(router, "GET", "/api-keys?owner=owner_other", nil)
		assert.Equal(t, http.StatusForbidden, resp.Code)
	})
}

func TestRevokeAPIKeyHTTP(t *testing.T) {
	t.Run("Master revokes existing key", func(t *testing.T) {
		router, b := setupAuthedRouter(t, true, nil)
		owner := "owner_" + gofakeit.UUID()
		key, err := b.CreateAPIKey(t.Context(), gofakeit.Name(), owner, []string{"ledgers:read"}, time.Now().Add(time.Hour))
		require.NoError(t, err)

		resp := doJSONRequest(router, "DELETE", fmt.Sprintf("/api-keys/%s?owner=%s", key.APIKeyID, owner), nil)
		assert.Equal(t, http.StatusNoContent, resp.Code)

		keys, err := b.ListAPIKeys(t.Context(), owner)
		require.NoError(t, err)
		require.Equal(t, 1, len(keys))
		assert.True(t, keys[0].IsRevoked)
	})

	t.Run("Nonexistent key", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, true, nil)
		resp := doJSONRequest(router, "DELETE", "/api-keys/api_key_nonexistent?owner=owner_x", nil)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Non-master cannot revoke another owner's key", func(t *testing.T) {
		ownerA := "owner_" + gofakeit.UUID()
		principal := &model.APIKey{APIKeyID: "api_key_test", OwnerID: "owner_" + gofakeit.UUID(), Scopes: []string{"ledgers:read"}}
		router, b := setupAuthedRouter(t, false, principal)
		key, err := b.CreateAPIKey(t.Context(), gofakeit.Name(), ownerA, []string{"ledgers:read"}, time.Now().Add(time.Hour))
		require.NoError(t, err)

		resp := doJSONRequest(router, "DELETE", fmt.Sprintf("/api-keys/%s", key.APIKeyID), nil)
		// The key belongs to ownerA, the principal owns nothing with this ID
		assert.Contains(t, []int{http.StatusForbidden, http.StatusNotFound}, resp.Code)

		keys, err := b.ListAPIKeys(t.Context(), ownerA)
		require.NoError(t, err)
		require.Equal(t, 1, len(keys))
		assert.False(t, keys[0].IsRevoked, "key must not be revoked by a different owner")
	})
}
