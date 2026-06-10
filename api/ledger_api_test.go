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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blnkfinance/blnk/internal/request"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/database"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

type TestRequest struct {
	Payload  io.Reader
	Router   *gin.Engine
	Response interface{}
	Method   string
	Route    string
	Auth     string
	Header   map[string]string
}

func SetUpTestRequest(s TestRequest) (*httptest.ResponseRecorder, error) {
	req := httptest.NewRequest(s.Method, s.Route, s.Payload)
	for key, value := range s.Header {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	s.Router.ServeHTTP(resp, req)

	err := json.NewDecoder(resp.Body).Decode(&s.Response)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func setupRouter() (*gin.Engine, *blnk.Blnk, error) {
	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_md_async",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})
	cnf, err := config.Fetch()
	if err != nil {
		return nil, nil, err
	}
	db, err := database.NewDataSource(cnf)
	if err != nil {
		return nil, nil, err
	}
	newBlnk, err := blnk.NewBlnk(db)
	if err != nil {
		return nil, nil, err
	}
	router := NewAPI(newBlnk).Router()

	return router, newBlnk, nil
}

func TestCreateLedger(t *testing.T) {
	router, blnk, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.CreateLedger
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Ledger",
			payload: model2.CreateLedger{
				Name: gofakeit.Name(),
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Empty Name",
			payload: model2.CreateLedger{
				Name: "",
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Ledger
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/ledgers",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if tt.expectedCode == http.StatusCreated {
				// Verify that the ledger is actually created in the database
				ledgerFromDB, err := blnk.GetLedgerByID(response.LedgerID)
				if err != nil {
					t.Errorf("Failed to retrieve ledger by ID: %v", err)
					return
				}
				assert.Equal(t, response.LedgerID, ledgerFromDB.LedgerID)
				assert.Equal(t, tt.payload.Name, ledgerFromDB.Name)
			}
		})
	}
}

func TestGetLedger(t *testing.T) {
	router, b, _ := setupRouter()
	validPayload := model.Ledger{Name: gofakeit.Name()}
	newLedger, err := b.CreateLedger(validPayload)
	if err != nil {
		return
	}
	fmt.Println(newLedger)
	var response model.Ledger
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/ledgers/%s", newLedger.LedgerID),
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(response)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Name, newLedger.Name)
}

func TestGetAllLedgers(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.UUID()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	t.Run("Default pagination", func(t *testing.T) {
		var response []model.Ledger
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/ledgers",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.True(t, len(response) >= 1)
	})

	t.Run("Invalid limit", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/ledgers?limit=abc",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid offset", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/ledgers?offset=-1",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Query param filter", func(t *testing.T) {
		var response []model.Ledger
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/ledgers?name_eq=%s", newLedger.Name),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, 1, len(response))
		assert.Equal(t, newLedger.LedgerID, response[0].LedgerID)
	})

	t.Run("Malformed filter operator", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/ledgers?name_eq=x&logical_operator=xor",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestUpdateLedger(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	t.Run("Valid update", func(t *testing.T) {
		newName := gofakeit.UUID()
		payloadBytes, _ := request.ToJsonReq(&model2.UpdateLedger{Name: newName})
		var response model.Ledger
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  payloadBytes,
			Response: &response,
			Method:   "PUT",
			Route:    fmt.Sprintf("/ledgers/%s", newLedger.LedgerID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		ledgerFromDB, err := b.GetLedgerByID(newLedger.LedgerID)
		if err != nil {
			t.Fatalf("Failed to retrieve ledger by ID: %v", err)
		}
		assert.Equal(t, newName, ledgerFromDB.Name)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("PUT", fmt.Sprintf("/ledgers/%s", newLedger.LedgerID), bytes.NewReader([]byte("not json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Empty name", func(t *testing.T) {
		payloadBytes, _ := request.ToJsonReq(&model2.UpdateLedger{Name: ""})
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  payloadBytes,
			Response: &response,
			Method:   "PUT",
			Route:    fmt.Sprintf("/ledgers/%s", newLedger.LedgerID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Nonexistent ledger", func(t *testing.T) {
		payloadBytes, _ := request.ToJsonReq(&model2.UpdateLedger{Name: gofakeit.Name()})
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  payloadBytes,
			Response: &response,
			Method:   "PUT",
			Route:    "/ledgers/ldg_nonexistent",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestFilterLedgers(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.UUID()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	t.Run("Filter by name eq", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "name", "operator": "eq", "value": "%s"}]}`, newLedger.Name)
		var response []model.Ledger
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/ledgers/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, 1, len(response))
		assert.Equal(t, newLedger.LedgerID, response[0].LedgerID)
	})

	t.Run("Include count", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "name", "operator": "eq", "value": "%s"}], "include_count": true}`, newLedger.Name)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/ledgers/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, response, "data")
		assert.Contains(t, response, "total_count")
		assert.Equal(t, float64(1), response["total_count"])
	})

	t.Run("Malformed JSON body", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/ledgers/filter", bytes.NewReader([]byte("{bad json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid logical operator", func(t *testing.T) {
		body := `{"filters": [{"field": "name", "operator": "eq", "value": "x"}], "logical_operator": "xor"}`
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/ledgers/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid filter operator", func(t *testing.T) {
		body := `{"filters": [{"field": "name", "operator": "badop", "value": "x"}]}`
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/ledgers/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Oversized limit coerced to default", func(t *testing.T) {
		body := `{"filters": [], "limit": 500}`
		var response []model.Ledger
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/ledgers/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.LessOrEqual(t, len(response), 20)
	})
}
