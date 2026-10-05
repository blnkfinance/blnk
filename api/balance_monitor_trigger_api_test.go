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

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// newMonitorTestBalance creates the ledger and balance a monitor needs.
func newMonitorTestBalance(t *testing.T, b *blnk.Blnk) model.Balance {
	t.Helper()

	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)

	balance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID: ledger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	require.NoError(t, err)

	return balance
}

func TestCreateBalanceMonitor_Trigger(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	balance := newMonitorTestBalance(t, b)

	tests := []struct {
		name            string
		trigger         string
		expectedCode    int
		expectedTrigger string
	}{
		{name: "Omitted trigger defaults to edge", trigger: "", expectedCode: http.StatusCreated, expectedTrigger: model.TriggerEdge},
		{name: "Edge accepted", trigger: model.TriggerEdge, expectedCode: http.StatusCreated, expectedTrigger: model.TriggerEdge},
		{name: "Level accepted", trigger: model.TriggerLevel, expectedCode: http.StatusCreated, expectedTrigger: model.TriggerLevel},
		{name: "Unknown trigger rejected", trigger: "sometimes", expectedCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := model2.CreateBalanceMonitor{
				BalanceId: balance.BalanceID,
				Trigger:   tt.trigger,
				Condition: model2.MonitorCondition{
					Field:     "balance",
					Operator:  "<",
					Value:     1000,
					Precision: 100,
				},
			}

			payloadBytes, _ := request.ToJsonReq(&payload)
			var response model.BalanceMonitor
			resp, _ := SetUpTestRequest(TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/balance-monitors",
				Router:   router,
			})

			assert.Equal(t, tt.expectedCode, resp.Code)
			if tt.expectedCode == http.StatusCreated {
				assert.Equal(t, tt.expectedTrigger, response.Trigger)
				assert.False(t, response.ConditionState, "a new monitor starts armed")
			}
		})
	}
}

func TestUpdateBalanceMonitor_Trigger(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	balance := newMonitorTestBalance(t, b)

	monitor, err := b.CreateMonitor(context.Background(), model.BalanceMonitor{
		BalanceID: balance.BalanceID,
		Trigger:   model.TriggerEdge,
		Condition: model.AlertCondition{
			Field:        "balance",
			Operator:     "<",
			Value:        1000,
			Precision:    100,
			PreciseValue: big.NewInt(100000),
		},
	})
	require.NoError(t, err)

	t.Run("Unknown trigger rejected", func(t *testing.T) {
		payloadBytes, _ := request.ToJsonReq(&model.BalanceMonitor{
			BalanceID: balance.BalanceID,
			Trigger:   "occasionally",
			Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 1000},
		})
		resp, _ := SetUpTestRequest(TestRequest{
			Payload: payloadBytes,
			Method:  "PUT",
			Route:   fmt.Sprintf("/balance-monitors/%s", monitor.MonitorID),
			Router:  router,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Switching to level and back", func(t *testing.T) {
		for _, trigger := range []string{model.TriggerLevel, model.TriggerEdge} {
			payloadBytes, _ := request.ToJsonReq(&model.BalanceMonitor{
				BalanceID: balance.BalanceID,
				Trigger:   trigger,
				Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 1000},
			})
			resp, _ := SetUpTestRequest(TestRequest{
				Payload: payloadBytes,
				Method:  "PUT",
				Route:   fmt.Sprintf("/balance-monitors/%s", monitor.MonitorID),
				Router:  router,
			})
			require.Equal(t, http.StatusOK, resp.Code)

			stored, err := b.GetMonitorByID(context.Background(), monitor.MonitorID)
			require.NoError(t, err)
			assert.Equal(t, trigger, stored.Trigger)
		}
	})

	t.Run("Omitted trigger falls back to edge", func(t *testing.T) {
		payloadBytes, _ := request.ToJsonReq(&model.BalanceMonitor{
			BalanceID: balance.BalanceID,
			Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 1000},
		})
		resp, _ := SetUpTestRequest(TestRequest{
			Payload: payloadBytes,
			Method:  "PUT",
			Route:   fmt.Sprintf("/balance-monitors/%s", monitor.MonitorID),
			Router:  router,
		})
		require.Equal(t, http.StatusOK, resp.Code)

		stored, err := b.GetMonitorByID(context.Background(), monitor.MonitorID)
		require.NoError(t, err)
		assert.Equal(t, model.TriggerEdge, stored.Trigger)
	})
}
