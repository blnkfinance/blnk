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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAppliedTransaction records a transaction synchronously (skip_queue)
// between two fresh balances and returns the API response.
func createAppliedTransaction(t *testing.T, router *gin.Engine, b *blnk.Blnk) *model.Transaction {
	t.Helper()

	source := createTestBalanceWithLedger(t, b, "NGN", nil)
	destination := createTestBalanceWithLedger(t, b, "NGN", nil)

	payload := model2.RecordTransaction{
		Amount:         100.50,
		Precision:      100,
		Reference:      "ref_" + gofakeit.UUID(),
		Description:    "query test",
		Currency:       "NGN",
		Source:         source.BalanceID,
		Destination:    destination.BalanceID,
		AllowOverDraft: true,
		SkipQueue:      true,
	}
	payloadBytes, _ := request.ToJsonReq(&payload)

	var response model.Transaction
	resp, err := SetUpTestRequest(TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "POST",
		Route:    "/transactions",
		Router:   router,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.Code, "failed to create applied transaction fixture")
	return &response
}

func TestGetAllTransactionsAPI(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	txn := createAppliedTransaction(t, router, b)

	t.Run("Default pagination", func(t *testing.T) {
		var response []model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/transactions",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.True(t, len(response) >= 1)
	})

	t.Run("Bad limit is coerced, not rejected", func(t *testing.T) {
		var response []model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/transactions?limit=abc&offset=-5",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("Query param filter", func(t *testing.T) {
		var response []model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/transactions?reference_eq=%s", txn.Reference),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		if assert.Equal(t, 1, len(response)) {
			assert.Equal(t, txn.TransactionID, response[0].TransactionID)
		}
	})

	t.Run("Malformed filter", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/transactions?status_eq=APPLIED&logical_operator=xor",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetTransactionByID(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	txn := createAppliedTransaction(t, router, b)

	t.Run("Existing transaction", func(t *testing.T) {
		var response model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/transactions/%s", txn.TransactionID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, txn.TransactionID, response.TransactionID)
		assert.Equal(t, txn.Reference, response.Reference)
	})

	t.Run("Nonexistent transaction", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/transactions/txn_nonexistent",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestGetTransactionByReference(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	txn := createAppliedTransaction(t, router, b)

	t.Run("Existing reference", func(t *testing.T) {
		var response model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/transactions/reference/%s", txn.Reference),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, txn.Reference, response.Reference)
	})

	t.Run("Unknown reference", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/transactions/reference/ref_does_not_exist",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestFilterTransactions(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	txn := createAppliedTransaction(t, router, b)

	t.Run("Filter by reference eq", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "reference", "operator": "eq", "value": "%s"}]}`, txn.Reference)
		var response []model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/transactions/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		if assert.Equal(t, 1, len(response)) {
			assert.Equal(t, txn.TransactionID, response[0].TransactionID)
		}
	})

	t.Run("Include count", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "reference", "operator": "eq", "value": "%s"}], "include_count": true}`, txn.Reference)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/transactions/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, response, "data")
		assert.Equal(t, float64(1), response["total_count"])
	})

	t.Run("Malformed JSON body", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/transactions/filter", bytes.NewReader([]byte("{bad")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid filter operator", func(t *testing.T) {
		body := `{"filters": [{"field": "status", "operator": "badop", "value": "x"}]}`
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/transactions/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetTransactionLineage(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	txn := createAppliedTransaction(t, router, b)

	t.Run("Existing transaction", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/transactions/%s/lineage", txn.TransactionID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, txn.TransactionID, response["transaction_id"])
	})

	t.Run("Nonexistent transaction", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/transactions/txn_nonexistent/lineage",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestRecoverQueuedTransactions(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}
	cnf, err := config.Fetch()
	if err != nil {
		t.Fatalf("Failed to fetch config: %v", err)
	}

	t.Run("Invalid threshold", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    "/transactions/recover?threshold=bogus",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, response["error"], "invalid threshold duration")
	})

	t.Run("Clean state returns counts and threshold echo", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    "/transactions/recover?threshold=24h",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "24h0m0s", response["threshold"])
		assert.Contains(t, response, "recovered")
	})

	t.Run("Recovers backdated queued transaction", func(t *testing.T) {
		// Queue a transaction with no Asynq worker running so it stays QUEUED.
		source := createTestBalanceWithLedger(t, b, "NGN", nil)
		destination := createTestBalanceWithLedger(t, b, "NGN", nil)
		reference := "ref_" + gofakeit.UUID()

		payload := model2.RecordTransaction{
			Amount:         50,
			Precision:      100,
			Reference:      reference,
			Description:    "recovery test",
			Currency:       "NGN",
			Source:         source.BalanceID,
			Destination:    destination.BalanceID,
			AllowOverDraft: true,
		}
		payloadBytes, _ := request.ToJsonReq(&payload)
		var queued model.Transaction
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  payloadBytes,
			Response: &queued,
			Method:   "POST",
			Route:    "/transactions",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, http.StatusCreated, resp.Code)

		// The QUEUED row is persisted asynchronously under the original reference;
		// with no Asynq worker running it stays QUEUED.
		queuedTxn, err := pollForTransactionStatus(context.Background(), b.GetDataSource(), reference, blnk.StatusQueued, 200*time.Millisecond, 10*time.Second)
		require.NoError(t, err, "queued transaction row never appeared")

		backdateTransaction(t, cnf, queuedTxn.TransactionID, 10*time.Minute)

		var response map[string]interface{}
		recoverResp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    "/transactions/recover?threshold=5m",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, recoverResp.Code)
		recovered, ok := response["recovered"].(float64)
		if assert.True(t, ok, "recovered should be a number") {
			assert.GreaterOrEqual(t, recovered, float64(1))
		}
	})
}
