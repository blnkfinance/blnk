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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteIdentityAPI(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Delete existing identity", func(t *testing.T) {
		identity := createTestIdentity(t, b)
		resp := doJSONRequest(router, "DELETE", fmt.Sprintf("/identities/%s", identity.IdentityID), nil)
		assert.Equal(t, http.StatusOK, resp.Code)

		_, err := b.GetIdentity(identity.IdentityID)
		assert.Error(t, err, "identity should no longer exist")
	})

	t.Run("Delete nonexistent identity", func(t *testing.T) {
		resp := doJSONRequest(router, "DELETE", "/identities/idt_nonexistent", nil)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestUpdateMetadataNotFound(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Nonexistent entity returns 404", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/ldg_nonexistent/metadata", []byte(`{"meta_data": {"k": "v"}}`))
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Invalid entity prefix returns 400", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/bogus_123/metadata", []byte(`{"meta_data": {"k": "v"}}`))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Missing meta_data body returns 400", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/ldg_nonexistent/metadata", []byte(`{}`))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetBalancesValidation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Invalid limit", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/balances?limit=abc", nil)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid offset", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/balances?offset=-1", nil)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Query param filter", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/balances?currency_eq=ZZZNONE", nil)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("Malformed filter", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/balances?currency_eq=USD&logical_operator=xor", nil)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetAllAccountsBranches(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Bad limit coerced", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/accounts?limit=abc&offset=-2", nil)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("Query param filter", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/accounts?bank_name_eq=NoSuchBankZZZ", nil)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("Malformed filter", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/accounts?bank_name_eq=x&logical_operator=xor", nil)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Create account invalid JSON", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/accounts", []byte("{bad"))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetAllIdentitiesBranches(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	identity := createTestIdentity(t, b)

	t.Run("Query param filter matches", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", fmt.Sprintf("/identities?first_name_eq=%s", url.QueryEscape(identity.FirstName)), nil)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("Malformed filter", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/identities?first_name_eq=x&logical_operator=xor", nil)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Bad limit coerced", func(t *testing.T) {
		resp := doJSONRequest(router, "GET", "/identities?limit=abc", nil)
		assert.Equal(t, http.StatusOK, resp.Code)
	})
}

func TestParseQueryOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	buildCtx := func(query string) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		req := httptest.NewRequest(http.MethodGet, "/ledgers?"+query, nil)
		c.Request = req
		return c
	}

	t.Run("Defaults", func(t *testing.T) {
		opts := ParseQueryOptions(buildCtx(""), "ledgers")
		assert.Equal(t, "", opts.SortBy)
		assert.Equal(t, "desc", string(opts.SortOrder))
		assert.False(t, opts.IncludeCount)
	})

	t.Run("Valid sort field and order", func(t *testing.T) {
		opts := ParseQueryOptions(buildCtx("sort_by=name&sort_order=asc&include_count=true"), "ledgers")
		assert.Equal(t, "name", opts.SortBy)
		assert.Equal(t, "asc", string(opts.SortOrder))
		assert.True(t, opts.IncludeCount)
	})

	t.Run("Invalid sort field dropped", func(t *testing.T) {
		opts := ParseQueryOptions(buildCtx("sort_by=evil;drop&sort_order=ASC"), "ledgers")
		assert.Equal(t, "", opts.SortBy, "unknown sort field must be discarded, never reach SQL")
		assert.Equal(t, "asc", string(opts.SortOrder), "sort order should be case-insensitive")
	})

	t.Run("Invalid sort order coerced to desc", func(t *testing.T) {
		opts := ParseQueryOptions(buildCtx("sort_order=sideways"), "ledgers")
		assert.Equal(t, "desc", string(opts.SortOrder))
	})
}

func TestRecoveryMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(logrusRecovery())
	router.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequest("GET", "/panic", nil)
	resp := httptest.NewRecorder()
	require.NotPanics(t, func() { router.ServeHTTP(resp, req) })
	assert.Equal(t, http.StatusInternalServerError, resp.Code)
}
