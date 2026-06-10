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
	"testing"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

const testTokenizationSecret = "12345678901234567890123456789012" // exactly 32 chars

func setupTokenizationRouter(t *testing.T) (*gin.Engine, *blnk.Blnk) {
	t.Helper()
	router, b, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
		cfg.TokenizationSecret = testTokenizationSecret
	})
	return router, b
}

func TestTokenizeIdentityField(t *testing.T) {
	router, b := setupTokenizationRouter(t)
	identity := createTestIdentity(t, b)

	t.Run("Tokenize firstName", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize/firstName", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		fromDB, err := b.GetIdentity(identity.IdentityID)
		if err != nil {
			t.Fatalf("Failed to fetch identity: %v", err)
		}
		assert.NotEqual(t, identity.FirstName, fromDB.FirstName, "stored value should be a token")
	})

	t.Run("Already tokenized", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize/firstName", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusConflict, resp.Code)
		assert.Contains(t, response["error"], "already tokenized")
	})

	t.Run("Non-tokenizable field", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize/category", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, response["error"], "not tokenizable")
	})

	t.Run("Nonexistent identity", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    "/identities/idt_nonexistent/tokenize/firstName",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestTokenizationDisabled(t *testing.T) {
	// Default router config has no TokenizationSecret, so the service is disabled.
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}
	identity := createTestIdentity(t, b)

	var response map[string]interface{}
	resp, err := SetUpTestRequest(TestRequest{
		Response: &response,
		Method:   "POST",
		Route:    fmt.Sprintf("/identities/%s/tokenize/firstName", identity.IdentityID),
		Router:   router,
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, http.StatusForbidden, resp.Code)
}

func TestDetokenizeIdentityField(t *testing.T) {
	router, b := setupTokenizationRouter(t)
	identity := createTestIdentity(t, b)
	originalLastName := identity.LastName

	var tokenizeResp map[string]interface{}
	resp, err := SetUpTestRequest(TestRequest{
		Response: &tokenizeResp,
		Method:   "POST",
		Route:    fmt.Sprintf("/identities/%s/tokenize/lastName", identity.IdentityID),
		Router:   router,
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, http.StatusOK, resp.Code)

	t.Run("Round trip returns original value", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/identities/%s/detokenize/lastName", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "lastName", response["field"])
		assert.Equal(t, originalLastName, response["value"])
	})

	t.Run("Field not tokenized", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/identities/%s/detokenize/phoneNumber", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Nonexistent identity", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/identities/idt_nonexistent/detokenize/lastName",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestTokenizeIdentityBatch(t *testing.T) {
	router, b := setupTokenizationRouter(t)

	t.Run("Tokenize multiple fields", func(t *testing.T) {
		identity := createTestIdentity(t, b)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{"fields": ["firstName", "emailAddress"]}`),
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		fromDB, err := b.GetIdentity(identity.IdentityID)
		if err != nil {
			t.Fatalf("Failed to fetch identity: %v", err)
		}
		assert.NotEqual(t, identity.FirstName, fromDB.FirstName)
		assert.NotEqual(t, identity.EmailAddress, fromDB.EmailAddress)
	})

	t.Run("Empty fields list", func(t *testing.T) {
		identity := createTestIdentity(t, b)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{"fields": []}`),
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, response["error"], "at least one field")
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		identity := createTestIdentity(t, b)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{bad`),
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestDetokenizeIdentityBatch(t *testing.T) {
	router, b := setupTokenizationRouter(t)
	identity := createTestIdentity(t, b)
	originalFirstName := identity.FirstName
	originalEmail := identity.EmailAddress

	var tokenizeResp map[string]interface{}
	resp, err := SetUpTestRequest(TestRequest{
		Payload:  jsonReader(`{"fields": ["firstName", "emailAddress"]}`),
		Response: &tokenizeResp,
		Method:   "POST",
		Route:    fmt.Sprintf("/identities/%s/tokenize", identity.IdentityID),
		Router:   router,
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, http.StatusOK, resp.Code)

	t.Run("Detokenize explicit fields", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{"fields": ["firstName"]}`),
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/detokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		fields, ok := response["fields"].(map[string]interface{})
		if assert.True(t, ok, "fields key should be an object") {
			assert.Equal(t, originalFirstName, fields["firstName"])
		}
	})

	t.Run("Empty fields detokenizes all", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{"fields": []}`),
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/detokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		fields, ok := response["fields"].(map[string]interface{})
		if assert.True(t, ok, "fields key should be an object") {
			assert.Equal(t, originalFirstName, fields["FirstName"])
			assert.Equal(t, originalEmail, fields["EmailAddress"])
		}
	})

	t.Run("Untokenized field in list", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{"fields": ["street"]}`),
			Response: &response,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/detokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetTokenizedFields(t *testing.T) {
	router, b := setupTokenizationRouter(t)

	t.Run("Fresh identity has none", func(t *testing.T) {
		identity := createTestIdentity(t, b)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/identities/%s/tokenized-fields", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		fields, ok := response["tokenized_fields"].([]interface{})
		if assert.True(t, ok, "tokenized_fields should be a list") {
			assert.Empty(t, fields)
		}
	})

	t.Run("Lists tokenized fields", func(t *testing.T) {
		identity := createTestIdentity(t, b)
		var tokenizeResp map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  jsonReader(`{"fields": ["firstName", "phoneNumber"]}`),
			Response: &tokenizeResp,
			Method:   "POST",
			Route:    fmt.Sprintf("/identities/%s/tokenize", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		resp, err = SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/identities/%s/tokenized-fields", identity.IdentityID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		fields, ok := response["tokenized_fields"].([]interface{})
		if assert.True(t, ok, "tokenized_fields should be a list") {
			assert.Equal(t, 2, len(fields))
		}
	})

	t.Run("Nonexistent identity", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/identities/idt_nonexistent/tokenized-fields",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}
