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
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
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
		req := httptest.NewRequest("POST", "/reconciliation/start", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/reconciliation/start", bytes.NewReader([]byte("invalid json")))
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
		req := httptest.NewRequest("POST", "/reconciliation/start-instant", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/reconciliation/start-instant", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Too many external transactions", func(t *testing.T) {
		txns := make([]map[string]interface{}, model2.MaxInstantReconciliationItems+1)
		for i := range txns {
			txns[i] = map[string]interface{}{
				"id":        fmt.Sprintf("ext_%d", i),
				"amount":    1,
				"reference": fmt.Sprintf("r%d", i),
				"currency":  "USD",
			}
		}
		payload := map[string]interface{}{
			"external_transactions": txns,
			"strategy":              "one_to_one",
			"matching_rule_ids":     []string{"mr_test"},
		}
		payloadBytes, _ := request.ToJsonReq(&payload)
		req := httptest.NewRequest("POST", "/reconciliation/start-instant", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "too many external_transactions")
	})
}

func TestGetReconciliation(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Reconciliation not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/reconciliation/rec_nonexistent", nil)
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
		req := httptest.NewRequest("POST", "/reconciliation/matching-rules", bytes.NewReader([]byte("invalid json")))
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
		req := httptest.NewRequest("PUT", "/reconciliation/matching-rules/", payloadBytes)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest(
			"PUT",
			"/reconciliation/matching-rules/mr_test",
			bytes.NewReader([]byte("invalid json")),
		)
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
		req := httptest.NewRequest("DELETE", "/reconciliation/matching-rules/", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Delete non-existent rule", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/reconciliation/matching-rules/mr_nonexistent", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func uploadMultipart(
	t *testing.T,
	router *gin.Engine,
	fieldName, fileName, source string,
	content []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if source != "" {
		if err := writer.WriteField("source", source); err != nil {
			t.Fatalf("Failed to write source field: %v", err)
		}
	}
	if fieldName != "" {
		part, err := writer.CreateFormFile(fieldName, fileName)
		if err != nil {
			t.Fatalf("Failed to create form file: %v", err)
		}
		if _, err := part.Write(content); err != nil {
			t.Fatalf("Failed to write file content: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close multipart writer: %v", err)
	}

	req := httptest.NewRequest("POST", "/reconciliation/upload", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

func TestUploadExternalData(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Valid CSV upload", func(t *testing.T) {
		id1, id2 := "ext_"+gofakeit.UUID(), "ext_"+gofakeit.UUID()
		csvContent := []byte("ID,Amount,Currency,Reference,Description,Date\n" +
			id1 + ",100.50,USD,ref_" + gofakeit.UUID() + ",test row,2024-01-01T10:00:00Z\n" +
			id2 + ",200.00,USD,ref_" + gofakeit.UUID() + ",test row two,2024-01-02T10:00:00Z\n")
		resp := uploadMultipart(t, router, "file", "data.csv", "bank-test", csvContent)
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		assert.Contains(t, response["upload_id"], "upload_")
		assert.Equal(t, float64(2), response["record_count"])
		assert.Equal(t, "bank-test", response["source"])
	})

	t.Run("Valid JSON upload", func(t *testing.T) {
		jsonContent := []byte(
			fmt.Sprintf(
				`[{"id": "ext_%s", "amount": 300, "currency": "USD", "reference": "ref_%s", "description": "json row", "date": "2024-01-03T10:00:00Z"}]`,
				gofakeit.UUID(),
				gofakeit.UUID(),
			),
		)
		resp := uploadMultipart(t, router, "file", "data.json", "bank-test", jsonContent)
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		assert.Equal(t, float64(1), response["record_count"])
	})

	t.Run("Missing file part", func(t *testing.T) {
		resp := uploadMultipart(t, router, "", "", "bank-test", nil)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "File upload failed")
	})

	t.Run("CSV missing required columns", func(t *testing.T) {
		csvContent := []byte("Foo,Bar\n1,2\n")
		resp := uploadMultipart(t, router, "file", "data.csv", "bank-test", csvContent)
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
		assert.Contains(t, resp.Body.String(), "Failed to process upload")
	})
}

func TestUploadExternalData_RejectsOversizedBody(t *testing.T) {
	// Cap uploads at 1 MB, then send a body larger than that: the request
	// must be rejected with 413 before the file is processed.
	router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
		c.Server.MaxUploadSizeMB = 1
	})

	oversized := bytes.Repeat([]byte("a"), 2*1024*1024) // 2 MB
	resp := uploadMultipart(t, router, "file", "big.csv", "bank-test", oversized)

	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)
	assert.Contains(t, resp.Body.String(), "exceeds the maximum allowed size")
}

// newCSVContent builds a 2-row external-transaction CSV payload.
func newCSVContent(t *testing.T, rows int) []byte {
	t.Helper()
	if rows < 1 {
		rows = 1
	}
	var b strings.Builder
	b.WriteString("ID,Amount,Currency,Reference,Description,Date\n")
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&b, "ext_%s,100.50,USD,ref_%s,row %d,2024-01-0%dT10:00:00Z\n",
			gofakeit.UUID(), gofakeit.UUID(), i+1, i+1)
	}
	return []byte(b.String())
}

// uploadServerHost returns the lowercase hostname of a test server's base URL,
// suitable for use as an exact-host whitelist entry.
func uploadServerHost(srv *httptest.Server) string {
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	return strings.ToLower(u.Hostname())
}

// startUploadTestServer spins up an httptest.Server serving:
//
//	/data.csv  -> a valid 2-row CSV
//	/big.csv   -> a >1MB body (for oversize tests)
//	/error     -> HTTP 500
//	/slow      -> sleeps 5s before responding (for timeout tests)
func startUploadTestServer(t *testing.T, bigBody []byte) *httptest.Server {
	t.Helper()
	csv := newCSVContent(t, 2)
	mux := http.NewServeMux()
	mux.HandleFunc("/data.csv", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(csv)
	})
	mux.HandleFunc("/big.csv", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(bigBody)
	})
	mux.HandleFunc("/error", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(csv)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// uploadMultipartURL posts a multipart form carrying only a `url` field
// (and optional source), mirroring uploadMultipart but with no file part.
func uploadMultipartURL(t *testing.T, router *gin.Engine, urlVal, source string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if source != "" {
		if err := writer.WriteField("source", source); err != nil {
			t.Fatalf("Failed to write source field: %v", err)
		}
	}
	if err := writer.WriteField("url", urlVal); err != nil {
		t.Fatalf("Failed to write url field: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close multipart writer: %v", err)
	}
	req := httptest.NewRequest("POST", "/reconciliation/upload", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

// uploadMultipartFileAndURL posts a multipart form with BOTH a `file` part and
// a `url` field, used to verify that `file` wins.
func uploadMultipartFileAndURL(
	t *testing.T,
	router *gin.Engine,
	fileName, source string,
	content []byte,
	urlVal string,
) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if source != "" {
		if err := writer.WriteField("source", source); err != nil {
			t.Fatalf("Failed to write source field: %v", err)
		}
	}
	if err := writer.WriteField("url", urlVal); err != nil {
		t.Fatalf("Failed to write url field: %v", err)
	}
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		t.Fatalf("Failed to create form file: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("Failed to write file content: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close multipart writer: %v", err)
	}
	req := httptest.NewRequest("POST", "/reconciliation/upload", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

// uploadJSONURL posts an application/json body {"url","source"}.
func uploadJSONURL(t *testing.T, router *gin.Engine, urlVal, source string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"url": urlVal, "source": source})
	req := httptest.NewRequest("POST", "/reconciliation/upload", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

func TestUploadExternalData_URLDownload(t *testing.T) {
	// Router whose whitelist contains the test server's hostname.
	router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
		// whitelist is set per-subtest via the dedicated function below; here we
		// just ensure MaxUploadSizeMB is a sane default.
	})

	t.Run("url field points to valid CSV", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadDomainWhitelist = uploadServerHost(srv)
		})
		resp := uploadMultipartURL(t, router, srv.URL+"/data.csv", "bank-test")
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		assert.Contains(t, response["upload_id"], "upload_")
		assert.Equal(t, float64(2), response["record_count"])
		assert.Equal(t, "bank-test", response["source"])
	})

	t.Run("url field with invalid scheme", func(t *testing.T) {
		resp := uploadMultipartURL(t, router, "ftp://example.com/x.csv", "bank-test")
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "RECON_UPLOAD_URL_INVALID")
	})

	t.Run("url field pointing to a server that returns an error", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadDomainWhitelist = uploadServerHost(srv)
		})
		resp := uploadMultipartURL(t, router, srv.URL+"/error", "bank-test")
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
		assert.Contains(t, resp.Body.String(), "Failed to process upload")
	})

	t.Run("both file and url present, file wins", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadDomainWhitelist = uploadServerHost(srv)
		})
		fileContent := newCSVContent(t, 1) // 1 row — distinct from the URL's 2
		resp := uploadMultipartFileAndURL(t, router, "data.csv", "bank-test", fileContent, srv.URL+"/data.csv")
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		assert.Equal(t, float64(1), response["record_count"], "file content (1 row) must win over url (2 rows)")
	})

	t.Run("JSON body url points to valid CSV", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadDomainWhitelist = uploadServerHost(srv)
		})
		resp := uploadJSONURL(t, router, srv.URL+"/data.csv", "bank-json")
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		assert.Equal(t, float64(2), response["record_count"])
		assert.Equal(t, "bank-json", response["source"])
	})
}

func TestUploadExternalData_URLWhitelist(t *testing.T) {
	t.Run("host not in whitelist", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadDomainWhitelist = "allowed.example.com" // does not include srv host
		})
		resp := uploadMultipartURL(t, router, srv.URL+"/data.csv", "bank-test")
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "RECON_UPLOAD_HOST_NOT_ALLOWED")
	})

	t.Run("empty whitelist denies all", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadDomainWhitelist = ""
		})
		resp := uploadMultipartURL(t, router, srv.URL+"/data.csv", "bank-test")
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "RECON_UPLOAD_HOST_NOT_ALLOWED")
	})

	t.Run("oversize download returns 500", func(t *testing.T) {
		// 2 MB CSV body, but MaxUploadSizeMB=1: the LimitReader truncates, so
		// processing fails downstream.
		bigBody := bytes.Repeat([]byte("a"), 2*1024*1024)
		srv := startUploadTestServer(t, bigBody)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.MaxUploadSizeMB = 1
			c.Server.UploadDomainWhitelist = uploadServerHost(srv)
		})
		resp := uploadMultipartURL(t, router, srv.URL+"/big.csv", "bank-test")
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
		assert.Contains(t, resp.Body.String(), "Failed to process upload")
	})

	t.Run("timeout enforced", func(t *testing.T) {
		srv := startUploadTestServer(t, nil)
		router, _, _ := setupRouterWithConfig(t, func(c *config.Configuration) {
			c.Server.UploadURLTimeoutSec = 1
			c.Server.UploadDomainWhitelist = uploadServerHost(srv)
		})
		resp := uploadMultipartURL(t, router, srv.URL+"/slow", "bank-test")
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
		assert.Contains(t, resp.Body.String(), "Failed to process upload")
	})
}
