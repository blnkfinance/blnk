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
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateBalance(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	// Create a ledger for positive test case
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.CreateBalance
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Balance",
			payload: model2.CreateBalance{
				LedgerId: newLedger.LedgerID,
				Currency: gofakeit.Currency().Short,
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Missing Ledger ID",
			payload: model2.CreateBalance{
				Currency: gofakeit.Currency().Short,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Currency",
			payload: model2.CreateBalance{
				LedgerId: newLedger.LedgerID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Balance
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/balances",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if tt.expectedCode == http.StatusCreated {
				// Verify that the balance is actually created in the database
				balanceFromDB, err := b.GetBalanceByID(context.Background(), response.BalanceID, nil, false)
				if err != nil {
					t.Errorf("Failed to retrieve balance by ID: %v", err)
				} else {
					assert.Equal(t, response.BalanceID, balanceFromDB.BalanceID)
					assert.Equal(t, tt.payload.LedgerId, balanceFromDB.LedgerID)
					assert.Equal(t, tt.payload.Currency, balanceFromDB.Currency)
					assert.Equal(t, big.NewInt(0), balanceFromDB.Balance)
					assert.Equal(t, big.NewInt(0), balanceFromDB.DebitBalance)
					assert.Equal(t, big.NewInt(0), balanceFromDB.CreditBalance)
				}
			}
		})
	}
}

func TestGetBalance(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}
	newBalance, err := b.CreateBalance(context.Background(), model.Balance{LedgerID: newLedger.LedgerID, Currency: gofakeit.CurrencyShort()})
	if err != nil {
		return
	}
	var response model.Balance
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/balances/%s", newBalance.BalanceID),
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, newBalance.Currency)
	assert.Equal(t, big.NewInt(0), newBalance.Balance)
	assert.Equal(t, big.NewInt(0), newBalance.DebitBalance)
	assert.Equal(t, big.NewInt(0), newBalance.CreditBalance)
	assert.Equal(t, big.NewInt(0), newBalance.InflightBalance)
	assert.Equal(t, big.NewInt(0), newBalance.InflightCreditBalance)
	assert.Equal(t, big.NewInt(0), newBalance.InflightDebitBalance)
	assert.Equal(t, int64(0), newBalance.Version)
}

func TestGetBalanceWithIncludes(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)

	orgName := "Acme " + gofakeit.UUID()
	identity, err := b.CreateIdentity(model.Identity{
		IdentityType:     "organization",
		OrganizationName: orgName,
		Category:         "business",
		EmailAddress:     gofakeit.Email(),
	})
	require.NoError(t, err)

	newBalance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID:   newLedger.LedgerID,
		Currency:   "USD",
		IdentityID: identity.IdentityID,
	})
	require.NoError(t, err)

	t.Run("include=identity returns the joined identity", func(t *testing.T) {
		var response model.Balance
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s?include=identity", newBalance.BalanceID),
			Router:   router,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, newBalance.BalanceID, response.BalanceID)
		require.NotNil(t, response.Identity, "identity must be populated")
		assert.Equal(t, identity.IdentityID, response.Identity.IdentityID)
		assert.Equal(t, orgName, response.Identity.OrganizationName, "organization_name must round-trip (regression: i_name typo)")
	})

	t.Run("include=ledger returns the joined ledger", func(t *testing.T) {
		var response model.Balance
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s?include=ledger", newBalance.BalanceID),
			Router:   router,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.Code)
		require.NotNil(t, response.Ledger)
		assert.Equal(t, newLedger.LedgerID, response.Ledger.LedgerID)
	})
}

func TestGetBalances(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	_, err = b.CreateBalance(context.Background(), model.Balance{
		LedgerID: newLedger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	if err != nil {
		t.Fatalf("Failed to create balance: %v", err)
	}

	var response []model.Balance
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    "/balances",
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, len(response) >= 1)
}

func TestCreateBalanceMonitor(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newBalance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID: newLedger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	if err != nil {
		t.Fatalf("Failed to create balance: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.CreateBalanceMonitor
		expectedCode int
	}{
		{
			name: "Valid Balance Monitor",
			payload: model2.CreateBalanceMonitor{
				BalanceId: newBalance.BalanceID,
				Condition: model2.MonitorCondition{
					Field:     "balance",
					Operator:  ">",
					Value:     1000,
					Precision: 100,
				},
			},
			expectedCode: http.StatusCreated,
		},
		{
			name: "Missing Balance ID",
			payload: model2.CreateBalanceMonitor{
				Condition: model2.MonitorCondition{
					Field:     "balance",
					Operator:  ">",
					Value:     1000,
					Precision: 100,
				},
			},
			expectedCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.BalanceMonitor
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/balance-monitors",
				Auth:     "",
				Router:   router,
			}

			resp, _ := SetUpTestRequest(testRequest)
			assert.Equal(t, tt.expectedCode, resp.Code)
		})
	}
}

func TestGetBalanceMonitor(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newBalance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID: newLedger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	if err != nil {
		t.Fatalf("Failed to create balance: %v", err)
	}

	newMonitor, err := b.CreateMonitor(context.Background(), model.BalanceMonitor{
		BalanceID: newBalance.BalanceID,
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: ">",
			Value:    1000,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}

	var response model.BalanceMonitor
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/balance-monitors/%s", newMonitor.MonitorID),
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, newMonitor.MonitorID, response.MonitorID)
}

func TestGetAllBalanceMonitors(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newBalance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID: newLedger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	if err != nil {
		t.Fatalf("Failed to create balance: %v", err)
	}

	_, err = b.CreateMonitor(context.Background(), model.BalanceMonitor{
		BalanceID: newBalance.BalanceID,
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: ">",
			Value:    1000,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}

	var response []model.BalanceMonitor
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    "/balance-monitors",
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, len(response) >= 1)
}

func TestUpdateBalanceMonitor(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newBalance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID: newLedger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	if err != nil {
		t.Fatalf("Failed to create balance: %v", err)
	}

	newMonitor, err := b.CreateMonitor(context.Background(), model.BalanceMonitor{
		BalanceID: newBalance.BalanceID,
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: ">",
			Value:    1000,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}

	updatedMonitor := model.BalanceMonitor{
		BalanceID: newBalance.BalanceID,
		Condition: model.AlertCondition{
			Field:    "balance",
			Operator: ">",
			Value:    2000,
		},
	}
	payloadBytes, _ := request.ToJsonReq(&updatedMonitor)

	var response map[string]interface{}
	testRequest := TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "PUT",
		Route:    fmt.Sprintf("/balance-monitors/%s", newMonitor.MonitorID),
		Auth:     "",
		Router:   router,
	}

	resp, _ := SetUpTestRequest(testRequest)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestFilterBalances(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	// A currency unlikely to collide with other test data
	currency := "XB" + gofakeit.LetterN(6)
	newBalance := createTestBalanceWithLedger(t, b, currency, nil)

	t.Run("Filter by currency eq", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "currency", "operator": "eq", "value": "%s"}]}`, currency)
		var response []model.Balance
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/balances/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, 1, len(response))
		assert.Equal(t, newBalance.BalanceID, response[0].BalanceID)
	})

	t.Run("Filter with in operator", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "currency", "operator": "in", "values": ["%s", "ZZZ"]}]}`, currency)
		var response []model.Balance
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/balances/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, 1, len(response))
	})

	t.Run("Include count", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "currency", "operator": "eq", "value": "%s"}], "include_count": true}`, currency)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/balances/filter",
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
		req := httptest.NewRequest("POST", "/balances/filter", bytes.NewReader([]byte("{bad")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid filter operator", func(t *testing.T) {
		body := `{"filters": [{"field": "currency", "operator": "badop", "value": "x"}]}`
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/balances/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestGetBalanceAtTime(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), nil)

	t.Run("From source", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s/at?from_source=true", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, true, response["from_source"])
		assert.Contains(t, response, "timestamp")
		balanceResult, ok := response["balance"].(map[string]interface{})
		if assert.True(t, ok, "balance key should be an object") {
			assert.Equal(t, newBalance.BalanceID, balanceResult["balance_id"])
		}
	})

	t.Run("Invalid timestamp", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s/at?timestamp=not-a-time", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, response["error"], "invalid timestamp format")
	})

	t.Run("Snapshot path", func(t *testing.T) {
		var snapshotResp map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &snapshotResp,
			Method:   "POST",
			Route:    "/balances-snapshots",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		var response map[string]interface{}
		ts := time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
		resp, err = SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s/at?timestamp=%s", newBalance.BalanceID, ts),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, false, response["from_source"])
	})

	t.Run("Nonexistent balance", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/balances/bln_nonexistent/at?from_source=true",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestGetBalanceByIndicator(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	indicator := "@test_" + gofakeit.LetterN(8)
	currency := gofakeit.CurrencyShort()
	newBalance := createTestBalanceWithLedger(t, b, currency, &balanceFixtureOpts{indicator: indicator})

	t.Run("Existing indicator", func(t *testing.T) {
		var response model.Balance
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/indicator/%s/currency/%s", indicator, currency),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, newBalance.BalanceID, response.BalanceID)
	})

	t.Run("Unknown indicator", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/balances/indicator/@does_not_exist/currency/USD",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestTakeBalanceSnapshots(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Default batch size", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "POST",
			Route:    "/balances-snapshots",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("Invalid batch size", func(t *testing.T) {
		for _, route := range []string{"/balances-snapshots?batch_size=0", "/balances-snapshots?batch_size=abc"} {
			var response map[string]interface{}
			resp, err := SetUpTestRequest(TestRequest{
				Response: &response,
				Method:   "POST",
				Route:    route,
				Router:   router,
			})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, http.StatusBadRequest, resp.Code)
		}
	})
}

func TestUpdateBalanceIdentity(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), nil)
	identity := createTestIdentity(t, b)

	t.Run("Valid update", func(t *testing.T) {
		body := fmt.Sprintf(`{"identity_id": "%s"}`, identity.IdentityID)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "PUT",
			Route:    fmt.Sprintf("/balances/%s/identity", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		balanceFromDB, err := b.GetBalanceByID(context.Background(), newBalance.BalanceID, nil, false)
		if err != nil {
			t.Fatalf("Failed to retrieve balance: %v", err)
		}
		assert.Equal(t, identity.IdentityID, balanceFromDB.IdentityID)
	})

	t.Run("Missing identity_id", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(`{}`)),
			Response: &response,
			Method:   "PUT",
			Route:    fmt.Sprintf("/balances/%s/identity", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
		// gin's required binding rejects the empty body before the handler's manual check
		assert.Contains(t, response["error"], "IdentityId")
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("PUT", fmt.Sprintf("/balances/%s/identity", newBalance.BalanceID), bytes.NewReader([]byte("{bad")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Nonexistent identity", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(`{"identity_id": "idt_nonexistent"}`)),
			Response: &response,
			Method:   "PUT",
			Route:    fmt.Sprintf("/balances/%s/identity", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("Nonexistent balance", func(t *testing.T) {
		body := fmt.Sprintf(`{"identity_id": "%s"}`, identity.IdentityID)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "PUT",
			Route:    "/balances/bln_nonexistent/identity",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestGetBalanceLineage(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	t.Run("Lineage tracking disabled", func(t *testing.T) {
		plainBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), nil)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s/lineage", plainBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Lineage tracking enabled", func(t *testing.T) {
		lineageBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), &balanceFixtureOpts{trackFundLineage: true})
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balances/%s/lineage", lineageBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, lineageBalance.BalanceID, response["balance_id"])
	})

	t.Run("Nonexistent balance", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    "/balances/bln_nonexistent/lineage",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestGetBalanceMonitorsByBalanceID(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), nil)

	t.Run("No monitors", func(t *testing.T) {
		var response []model.BalanceMonitor
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balance-monitors/balances/%s", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, 0, len(response))
	})

	t.Run("With monitors", func(t *testing.T) {
		newMonitor, err := b.CreateMonitor(context.Background(), model.BalanceMonitor{
			BalanceID: newBalance.BalanceID,
			Condition: model.AlertCondition{Field: "balance", Operator: ">", Value: 1000},
		})
		if err != nil {
			t.Fatalf("Failed to create monitor: %v", err)
		}

		var response []model.BalanceMonitor
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/balance-monitors/balances/%s", newBalance.BalanceID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		if assert.Equal(t, 1, len(response)) {
			assert.Equal(t, newMonitor.MonitorID, response[0].MonitorID)
			assert.Equal(t, newBalance.BalanceID, response[0].BalanceID)
		}
	})
}

func TestDeleteBalanceMonitor(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), nil)
	newMonitor, err := b.CreateMonitor(context.Background(), model.BalanceMonitor{
		BalanceID: newBalance.BalanceID,
		Condition: model.AlertCondition{Field: "balance", Operator: ">", Value: 1000},
	})
	if err != nil {
		t.Fatalf("Failed to create monitor: %v", err)
	}

	t.Run("Delete existing monitor", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "DELETE",
			Route:    fmt.Sprintf("/balance-monitors/%s", newMonitor.MonitorID),
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)

		_, err = b.GetMonitorByID(context.Background(), newMonitor.MonitorID)
		assert.Error(t, err, "monitor should no longer exist")
	})

	t.Run("Delete nonexistent monitor", func(t *testing.T) {
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "DELETE",
			Route:    "/balance-monitors/mon_nonexistent",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}
