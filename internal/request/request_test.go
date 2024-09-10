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

package request_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jerry-enebeli/blnk/internal/request"
	"github.com/stretchr/testify/assert"
)

func TestToJsonReq_Success(t *testing.T) {
	payload := map[string]string{
		"key": "value",
	}

	reqBuffer, err := request.ToJsonReq(payload)
	assert.NoError(t, err)

	// Ensure the returned buffer contains the expected JSON
	expectedJSON, _ := json.Marshal(payload)
	assert.Equal(t, expectedJSON, reqBuffer.Bytes())
}

func TestToJsonReq_Fail(t *testing.T) {
	// Payload with unsupported data type
	payload := map[string]interface{}{
		"key": make(chan int), // invalid data type for JSON encoding
	}

	reqBuffer, err := request.ToJsonReq(payload)
	assert.Error(t, err)
	assert.Nil(t, reqBuffer)
}

func TestCall_Success(t *testing.T) {
	// Mock server to return a dummy response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":"success"}`))
		assert.NoError(t, err)
	}))
	defer server.Close()

	// Create a new request to the mock server
	req, err := http.NewRequest("GET", server.URL, nil)
	assert.NoError(t, err)

	var response map[string]string
	resp, err := request.Call(req, &response)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "success", response["status"])
}

func TestCall_Fail_DecodeResponse(t *testing.T) {
	// Mock server to return a malformed response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{malformed json response`)) // Invalid JSON
		assert.NoError(t, err)
	}))
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL, nil)
	assert.NoError(t, err)

	var response map[string]string
	resp, err := request.Call(req, &response)
	assert.Error(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCall_Fail_DoRequest(t *testing.T) {
	// Create a request with an invalid URL
	req, err := http.NewRequest("GET", "http://invalid-url", nil)
	assert.NoError(t, err)

	var response map[string]string
	resp, err := request.Call(req, &response)
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestBasicAuth(t *testing.T) {
	username := "user"
	password := "pass"
	expectedAuth := "dXNlcjpwYXNz" // base64 of "user:pass"

	authHeader := request.BasicAuth(username, password)
	assert.Equal(t, expectedAuth, authHeader)
}
