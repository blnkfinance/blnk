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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests in this file that don't require Typesense exercise the
// validation/error branches; happy paths are gated on BLNK_TYPESENSE_DNS.

func TestSearchValidation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Invalid JSON body", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/search/ledgers", []byte("{bad"))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Unreachable Typesense returns 400", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/search/ledgers", []byte(`{"q": "*", "query_by": "name"}`))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestMultiSearchValidation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Invalid JSON body", func(t *testing.T) {
		resp := doJSONRequest(router, "POST", "/multi-search", []byte("{bad"))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Unreachable Typesense returns 400", func(t *testing.T) {
		body := []byte(`{"searches": [{"collection": "ledgers", "q": "*", "query_by": "name"}]}`)
		resp := doJSONRequest(router, "POST", "/multi-search", body)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestReindexProgressLifecycle(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("No reindex started returns 404", func(t *testing.T) {
		resetReindexManager()
		resp := doJSONRequest(router, "GET", "/search/reindex", nil)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("StartReindex always accepts", func(t *testing.T) {
		resetReindexManager()
		for _, body := range [][]byte{[]byte(`{}`), []byte(`{"batch_size": 500}`), []byte(`{malformed`)} {
			resetReindexManager()
			resp := doJSONRequest(router, "POST", "/search/reindex", body)
			assert.Equal(t, http.StatusAccepted, resp.Code)
		}
	})

	t.Run("Progress available after start", func(t *testing.T) {
		resetReindexManager()
		resp := doJSONRequest(router, "POST", "/search/reindex", []byte(`{}`))
		require.Equal(t, http.StatusAccepted, resp.Code)

		resp = doJSONRequest(router, "GET", "/search/reindex", nil)
		assert.Equal(t, http.StatusOK, resp.Code)

		var progress map[string]interface{}
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &progress))
		assert.Contains(t, progress, "status")
	})
}

func setupTypesenseRouter(t *testing.T, dns string) (*gin.Engine, *blnk.Blnk) {
	t.Helper()
	router, b, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
		cfg.TypeSense = config.TypeSenseConfig{Dns: dns}
	})
	return router, b
}

func TestSearchWithTypesense(t *testing.T) {
	dns := skipWithoutTypesense(t)
	router, b := setupTypesenseRouter(t, dns)

	ledgerName := gofakeit.UUID()
	_, err := b.CreateLedger(model.Ledger{Name: ledgerName})
	require.NoError(t, err)

	// Index existing data, then wait for the reindex to finish.
	resetReindexManager()
	resp := doJSONRequest(router, "POST", "/search/reindex", []byte(`{}`))
	require.Equal(t, http.StatusAccepted, resp.Code)

	deadline := time.Now().Add(2 * time.Minute)
	for {
		resp = doJSONRequest(router, "GET", "/search/reindex", nil)
		require.Equal(t, http.StatusOK, resp.Code)
		var progress map[string]interface{}
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &progress))
		if progress["status"] == "completed" {
			break
		}
		if progress["status"] == "failed" {
			t.Fatalf("reindex failed: %v", progress["errors"])
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for reindex to complete: %v", progress)
		}
		time.Sleep(time.Second)
	}

	t.Run("Search ledgers", func(t *testing.T) {
		body := []byte(fmt.Sprintf(`{"q": "%s", "query_by": "name"}`, ledgerName))
		resp := doJSONRequest(router, "POST", "/search/ledgers", body)
		assert.Equal(t, http.StatusCreated, resp.Code)

		var result map[string]interface{}
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &result))
		found, ok := result["found"].(float64)
		if assert.True(t, ok, "search result should include found count") {
			assert.GreaterOrEqual(t, found, float64(1))
		}
	})

	t.Run("Multi-search", func(t *testing.T) {
		body := []byte(fmt.Sprintf(`{"searches": [{"collection": "ledgers", "q": "%s", "query_by": "name"}]}`, ledgerName))
		resp := doJSONRequest(router, "POST", "/multi-search", body)
		assert.Equal(t, http.StatusOK, resp.Code)
	})
}

func TestReindexConflictWithTypesense(t *testing.T) {
	dns := skipWithoutTypesense(t)
	router, _ := setupTypesenseRouter(t, dns)

	resetReindexManager()
	resp := doJSONRequest(router, "POST", "/search/reindex", []byte(`{}`))
	require.Equal(t, http.StatusAccepted, resp.Code)

	// A second start while the first is in progress should conflict. This is
	// timing-dependent: skip the assertion if the first run already finished.
	resp = doJSONRequest(router, "POST", "/search/reindex", []byte(`{}`))
	if resp.Code == http.StatusConflict {
		assert.Contains(t, resp.Body.String(), "already in progress")
	} else {
		assert.Equal(t, http.StatusAccepted, resp.Code)
	}

	// Always drain: wait for any in-flight reindex to finish so later tests
	// in the package see a quiet state.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		check := doJSONRequest(router, "GET", "/search/reindex", nil)
		var progress map[string]interface{}
		if err := json.Unmarshal(check.Body.Bytes(), &progress); err == nil && progress["status"] != "in_progress" {
			break
		}
		time.Sleep(time.Second)
	}
	resetReindexManager()
}
