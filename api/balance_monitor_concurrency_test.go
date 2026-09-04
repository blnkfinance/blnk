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
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blnkservice "github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// TestBalanceMonitorEdge_ConcurrentTransactions is the whole point of the
// design, exercised through the product rather than the datasource: monitor
// checks are dispatched into detached goroutines once the balance lock is
// released, so several of them evaluate the same crossing at once. The consumer
// must still be told once.
func TestBalanceMonitorEdge_ConcurrentTransactions(t *testing.T) {
	e := setupMonitorE2E(t)

	funding := e.newBalance(t)
	wallet := e.newBalance(t)
	monitor := e.createMonitor(t, wallet.BalanceID, model.TriggerEdge, 500)

	const senders = 12

	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payloadBytes, _ := request.ToJsonReq(&model2.RecordTransaction{
				Amount:         200,
				Precision:      1,
				Reference:      gofakeit.UUID(),
				Description:    "concurrent monitor crossing",
				Currency:       "USD",
				Source:         funding.BalanceID,
				Destination:    wallet.BalanceID,
				SkipQueue:      true,
				AllowOverDraft: true,
			})
			resp, _ := SetUpTestRequest(TestRequest{
				Payload: payloadBytes,
				Method:  "POST",
				Route:   "/transactions",
				Router:  e.router,
			})
			assert.Equal(t, http.StatusCreated, resp.Code)
		}()
	}
	wg.Wait()

	// Every one of those transactions left the wallet past the threshold, so
	// most of the evaluations saw a true condition.
	e.awaitWebhooks(t, 1)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "%d concurrent transactions past the threshold must produce one alert", senders)
	assert.True(t, e.monitorState(t, monitor.MonitorID))
}

// TestBalanceMonitorEdge_MixedTriggersOnOneBalance pins that the two modes stay
// independent when they watch the same balance.
func TestBalanceMonitorEdge_MixedTriggersOnOneBalance(t *testing.T) {
	e := setupMonitorE2E(t)

	funding := e.newBalance(t)
	wallet := e.newBalance(t)
	edge := e.createMonitor(t, wallet.BalanceID, model.TriggerEdge, 500)
	level := e.createMonitor(t, wallet.BalanceID, model.TriggerLevel, 500)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 600) // crossing: edge + level
	e.awaitWebhooks(t, 2)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 100) // still past: level only
	e.awaitWebhooks(t, 3)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 100) // still past: level only
	e.awaitWebhooks(t, 4)

	assert.True(t, e.monitorState(t, edge.MonitorID))
	assert.False(t, e.monitorState(t, level.MonitorID), "a level monitor keeps no state")
}

// TestBalanceMonitorEdge_InflightCommit pins that the inflight settlement path
// reaches the monitors too. It arrives by way of RecordTransaction, so it shares
// the post-commit work, but it is the path a real wallet balance actually moves
// on and is worth holding still.
func TestBalanceMonitorEdge_InflightCommit(t *testing.T) {
	e := setupMonitorE2E(t)

	funding := e.newBalance(t)
	wallet := e.newBalance(t)
	monitor := e.createMonitor(t, wallet.BalanceID, model.TriggerEdge, 500)

	payloadBytes, _ := request.ToJsonReq(&model2.RecordTransaction{
		Amount:         900,
		Precision:      1,
		Reference:      gofakeit.UUID(),
		Description:    "inflight crossing",
		Currency:       "USD",
		Source:         funding.BalanceID,
		Destination:    wallet.BalanceID,
		Inflight:       true,
		SkipQueue:      true,
		AllowOverDraft: true,
	})
	var queued model.Transaction
	resp, _ := SetUpTestRequest(TestRequest{
		Payload:  payloadBytes,
		Response: &queued,
		Method:   "POST",
		Route:    "/transactions",
		Router:   e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code)
	require.NotEmpty(t, queued.TransactionID)

	// Inflight money is not the balance, so nothing has crossed yet.
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "an inflight transaction has not moved the balance")

	commitBytes, _ := request.ToJsonReq(&model2.InflightUpdate{Status: blnkservice.InflightActionCommit, SkipQueue: true})
	commitResp, _ := SetUpTestRequest(TestRequest{
		Payload: commitBytes,
		Method:  "PUT",
		Route:   fmt.Sprintf("/transactions/inflight/%s", queued.TransactionID),
		Router:  e.router,
	})
	require.Equal(t, http.StatusOK, commitResp.Code)

	e.awaitWebhooks(t, 1)
	assert.True(t, e.monitorState(t, monitor.MonitorID))
}

// TestBalanceMonitorEdge_WebhookPayload checks what a consumer actually
// receives, not just that something was queued.
func TestBalanceMonitorEdge_WebhookPayload(t *testing.T) {
	e := setupMonitorE2E(t)

	funding := e.newBalance(t)
	wallet := e.newBalance(t)
	monitor := e.createMonitor(t, wallet.BalanceID, model.TriggerEdge, 500)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)
	e.awaitWebhooks(t, 1)

	tasks := e.pendingTasks()

	// NewWebhook serialises its payload under "data", not "payload".
	var payload struct {
		Event string               `json:"event"`
		Data  model.BalanceMonitor `json:"data"`
	}
	found := false
	for _, task := range tasks {
		var probe struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(task.Payload, &probe); err != nil || probe.Event != "balance.monitor" {
			continue
		}
		require.NoError(t, json.Unmarshal(task.Payload, &payload))
		found = true
		break
	}
	require.True(t, found, "no balance.monitor task on the queue")

	assert.Equal(t, monitor.MonitorID, payload.Data.MonitorID)
	assert.Equal(t, wallet.BalanceID, payload.Data.BalanceID)
	assert.Equal(t, model.TriggerEdge, payload.Data.Trigger)
	assert.True(t, payload.Data.ConditionState, "the alert says the monitor is now triggered")
	assert.Equal(t, "balance", payload.Data.Condition.Field)
	assert.Equal(t, ">", payload.Data.Condition.Operator)
}
