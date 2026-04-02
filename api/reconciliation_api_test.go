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

	"github.com/blnkfinance/blnk/internal/request"
	"github.com/stretchr/testify/assert"
)

func TestStartReconciliation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Missing required fields", func(t *testing.T) {
		payload := struct {
			UploadID string `json:"upload_id"`
			Strategy string `json:"strategy"`
		}{
			UploadID: "",
			Strategy: "",
		}
		payloadBytes, _ := request.ToJsonReq(&payload)
		req := httptest.NewRequest("POST", "/reconciliations", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/reconciliations", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestInstantReconciliation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Missing external transactions", func(t *testing.T) {
		payload := struct {
			ExternalTransactions []interface{} `json:"external_transactions"`
			Strategy             string        `json:"strategy"`
			MatchingRuleIDs      []string      `json:"matching_rule_ids"`
		}{
			ExternalTransactions: []interface{}{},
			Strategy:             "one_to_one",
			MatchingRuleIDs:      []string{"mr_test"},
		}
		payloadBytes, _ := request.ToJsonReq(&payload)
		req := httptest.NewRequest("POST", "/reconciliations/instant", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/reconciliations/instant", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetReconciliation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Reconciliation not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/reconciliations/rec_nonexistent", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestCreateMatchingRule(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/matching-rules", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestUpdateMatchingRule(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Missing rule ID", func(t *testing.T) {
		payload := struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		}{
			Name:        "Updated Rule",
			Description: "Updated description",
		}
		payloadBytes, _ := request.ToJsonReq(&payload)
		req := httptest.NewRequest("PUT", "/matching-rules/", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/matching-rules/mr_test", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestDeleteMatchingRule(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Missing rule ID", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/matching-rules/", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Delete non-existent rule", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/matching-rules/mr_nonexistent", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
	})
}
