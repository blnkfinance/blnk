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
	"context"
	"fmt"
	"math/big"
	"net/http"
	"testing"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/model"

	"github.com/stretchr/testify/assert"
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
