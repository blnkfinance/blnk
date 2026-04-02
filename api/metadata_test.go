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

func TestUpdateMetadata(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Missing entity ID", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/metadata/update/", nil)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Invalid JSON body", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/metadata/update/ldg_123", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Valid request with missing metadata field", func(t *testing.T) {
		payload := struct {
			WrongField string `json:"wrong_field"`
		}{
			WrongField: "value",
		}
		payloadBytes, _ := request.ToJsonReq(&payload)
		req := httptest.NewRequest("PATCH", "/metadata/update/ldg_123", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}
