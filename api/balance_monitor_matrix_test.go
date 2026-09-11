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
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blnkservice "github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// createMonitorOn is createMonitor with the condition field and operator open,
// so the matrix can cover every combination the ledger supports.
func (e *monitorE2E) createMonitorOn(t *testing.T, balanceID, trigger, field, operator string, value float64) model.BalanceMonitor {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.CreateBalanceMonitor{
		BalanceId: balanceID,
		Trigger:   trigger,
		Condition: model2.MonitorCondition{Field: field, Operator: operator, Value: value, Precision: 1},
	})

	var monitor model.BalanceMonitor
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Response: &monitor,
		Method: "POST", Route: "/balance-monitors", Router: e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code)
	return monitor
}

// TestBalanceMonitorEdge_EveryConditionField covers all six fields
// CheckCondition understands, each driven by the ledger movement that actually
// changes it.
func TestBalanceMonitorEdge_EveryConditionField(t *testing.T) {
	t.Run("credit_balance", func(t *testing.T) {
		e := setupMonitorE2E(t)
		funding, wallet := e.newBalance(t), e.newBalance(t)
		m := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "credit_balance", ">", 500)

		e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
		e.settle()
		assert.Equal(t, 0, e.webhooks())

		e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
		e.awaitWebhooks(t, 1)

		// credit_balance only ever grows, so it can never re-arm.
		e.transfer(t, wallet.BalanceID, funding.BalanceID, 400)
		e.settle()
		assert.Equal(t, 1, e.webhooks(), "a debit does not reduce credit_balance, so nothing re-arms")
		assert.True(t, e.monitorState(t, m.MonitorID))
	})

	t.Run("debit_balance", func(t *testing.T) {
		e := setupMonitorE2E(t)
		funding, wallet := e.newBalance(t), e.newBalance(t)
		e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "debit_balance", ">", 500)

		e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)
		e.settle()
		assert.Equal(t, 0, e.webhooks(), "crediting the wallet does not move its debit_balance")

		e.transfer(t, wallet.BalanceID, funding.BalanceID, 600)
		e.awaitWebhooks(t, 1)
	})

	t.Run("inflight_credit_balance fires before the commit", func(t *testing.T) {
		e := setupMonitorE2E(t)
		funding, wallet := e.newBalance(t), e.newBalance(t)
		m := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "inflight_credit_balance", ">", 500)

		txn := e.inflight(t, funding.BalanceID, wallet.BalanceID, 900)
		e.awaitWebhooks(t, 1)
		assert.True(t, e.monitorState(t, m.MonitorID), "money in flight is what this monitor watches")

		// Committing drains the inflight balance, which re-arms the monitor.
		e.settleInflight(t, txn, blnkservice.InflightActionCommit)
		e.settle()
		assert.Equal(t, 1, e.webhooks())
		assert.False(t, e.monitorState(t, m.MonitorID), "a commit drains inflight, so the monitor re-arms")
	})

	t.Run("inflight_balance re-arms on void", func(t *testing.T) {
		e := setupMonitorE2E(t)
		funding, wallet := e.newBalance(t), e.newBalance(t)
		m := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "inflight_balance", ">", 500)

		txn := e.inflight(t, funding.BalanceID, wallet.BalanceID, 900)
		e.awaitWebhooks(t, 1)

		e.settleInflight(t, txn, blnkservice.InflightActionVoid)
		e.settle()
		assert.Equal(t, 1, e.webhooks(), "voiding is not an alert")
		assert.False(t, e.monitorState(t, m.MonitorID), "a void releases inflight, so the monitor re-arms")
	})

	t.Run("inflight_debit_balance on the source", func(t *testing.T) {
		e := setupMonitorE2E(t)
		funding, wallet := e.newBalance(t), e.newBalance(t)
		e.createMonitorOn(t, funding.BalanceID, model.TriggerEdge, "inflight_debit_balance", ">", 500)

		e.inflight(t, funding.BalanceID, wallet.BalanceID, 900)
		e.awaitWebhooks(t, 1)
	})
}

// TestBalanceMonitorEdge_EveryOperator drives each operator across its own
// threshold. Equality and inequality have the least obvious crossing
// semantics, so they matter most.
func TestBalanceMonitorEdge_EveryOperator(t *testing.T) {
	cases := []struct {
		operator string
		value    float64
		steps    []float64 // signed transfers into the wallet
		want     int
		reason   string
	}{
		{">", 500, []float64{300, 300, 300}, 1, "one upward crossing"},
		{">=", 600, []float64{300, 300, 300}, 1, "reaching the threshold counts"},
		{"<", 100, []float64{300, -250, -100}, 1, "one downward crossing"},
		{"<=", 50, []float64{300, -250, -50}, 1, "reaching the threshold counts"},
		{"=", 600, []float64{300, 300, 300}, 1, "true only at exactly 600, so one crossing"},
		{"!=", 0, []float64{300, -300, 300}, 2, "leaves zero, returns to zero, leaves again"},
	}

	for _, tc := range cases {
		t.Run(tc.operator, func(t *testing.T) {
			e := setupMonitorE2E(t)
			funding, wallet := e.newBalance(t), e.newBalance(t)
			e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", tc.operator, tc.value)

			for _, amount := range tc.steps {
				if amount > 0 {
					e.transfer(t, funding.BalanceID, wallet.BalanceID, amount)
				} else {
					e.transfer(t, wallet.BalanceID, funding.BalanceID, -amount)
				}
			}

			e.awaitWebhooks(t, tc.want)
			e.settle()
			assert.Equal(t, tc.want, e.webhooks(), "operator %q: %s", tc.operator, tc.reason)
		})
	}
}

// TestBalanceMonitorEdge_BulkBatch covers the coalescing post-commit path,
// which is a different caller from the single-transaction one. A batch that
// coalesces several transactions on one balance updates it once, so it must
// alert once.
func TestBalanceMonitorEdge_BulkBatch(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)
	m := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)

	items := make([]*model2.RecordTransaction, 0, 6)
	for i := 0; i < 6; i++ {
		items = append(items, &model2.RecordTransaction{
			Amount: 200, Precision: 1, Reference: gofakeit.UUID(), Description: "bulk",
			Currency: "USD", Source: funding.BalanceID, Destination: wallet.BalanceID,
			AllowOverDraft: true,
		})
	}
	payloadBytes, _ := request.ToJsonReq(&model2.BulkTransactionRequest{Transactions: items, SkipQueue: true})
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Method: "POST", Route: "/transactions/bulk", Router: e.router,
	})
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, resp.Code, "bulk batch rejected: %s", resp.Body.String())

	e.awaitWebhooks(t, 1)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "a batch that crosses the threshold once must alert once")
	assert.True(t, e.monitorState(t, m.MonitorID))
}

// TestBalanceMonitorEdge_OverdraftThreshold covers a negative threshold, which
// is what a credit line actually needs.
func TestBalanceMonitorEdge_OverdraftThreshold(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)
	e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", "<", -500)

	e.transfer(t, wallet.BalanceID, funding.BalanceID, 300)
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "-300 has not breached the credit line")

	e.transfer(t, wallet.BalanceID, funding.BalanceID, 300)
	e.awaitWebhooks(t, 1)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 400)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "back inside the line is not an alert")

	e.transfer(t, wallet.BalanceID, funding.BalanceID, 400)
	e.awaitWebhooks(t, 2)
}

// TestBalanceMonitorEdge_ManyMonitorsOnOneBalance checks that every monitor on
// a balance is evaluated, not just the first, and that each owns its own edge.
func TestBalanceMonitorEdge_ManyMonitorsOnOneBalance(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)

	const monitors = 12
	for i := 0; i < monitors; i++ {
		e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", float64(100*i+50))
	}

	// One transaction past every threshold: each monitor owes exactly one alert.
	e.transfer(t, funding.BalanceID, wallet.BalanceID, 5000)
	e.awaitWebhooks(t, monitors)
	e.settle()
	assert.Equal(t, monitors, e.webhooks())

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 1000)
	e.settle()
	assert.Equal(t, monitors, e.webhooks(), "still past every threshold, so nothing more to say")
}

// TestBalanceMonitorEdge_CreatedWhileConditionAlreadyHolds pins the documented
// creation rule: a monitor starts armed, so one whose condition already holds
// alerts on its balance's next transaction and then goes quiet.
func TestBalanceMonitorEdge_CreatedWhileConditionAlreadyHolds(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)
	e.settle()

	m := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)
	assert.False(t, m.ConditionState, "a new monitor starts armed")
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "creating a monitor does not evaluate it")

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 1)
	e.awaitWebhooks(t, 1)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "and it settles after the one catch-up alert")
}

// TestBalanceMonitorEdge_DuplicateMonitors checks that two identical monitors
// on one balance keep separate state.
func TestBalanceMonitorEdge_DuplicateMonitors(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)
	first := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)
	second := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)
	e.awaitWebhooks(t, 2)
	assert.True(t, e.monitorState(t, first.MonitorID))
	assert.True(t, e.monitorState(t, second.MonitorID))
}

// TestBalanceMonitor_GetEndpointsReportTriggerAndState covers the read surface
// a consumer uses to answer "why is this monitor quiet?".
func TestBalanceMonitor_GetEndpointsReportTriggerAndState(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)
	created := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)
	e.awaitWebhooks(t, 1)

	var one model.BalanceMonitor
	resp, _ := SetUpTestRequest(TestRequest{
		Response: &one, Method: "GET",
		Route: fmt.Sprintf("/balance-monitors/%s", created.MonitorID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, model.TriggerEdge, one.Trigger)
	assert.True(t, one.ConditionState, "the detail endpoint reports the monitor is triggered")

	var byBalance []model.BalanceMonitor
	resp, _ = SetUpTestRequest(TestRequest{
		Response: &byBalance, Method: "GET",
		Route: fmt.Sprintf("/balance-monitors/balances/%s", wallet.BalanceID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)
	require.Len(t, byBalance, 1)
	assert.Equal(t, model.TriggerEdge, byBalance[0].Trigger)
	assert.True(t, byBalance[0].ConditionState)

	var all []model.BalanceMonitor
	resp, _ = SetUpTestRequest(TestRequest{
		Response: &all, Method: "GET", Route: "/balance-monitors", Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)
	found := false
	for _, m := range all {
		if m.MonitorID == created.MonitorID {
			assert.Equal(t, model.TriggerEdge, m.Trigger)
			assert.True(t, m.ConditionState)
			found = true
		}
	}
	assert.True(t, found, "the list endpoint must report the same monitor")
}

// TestBalanceMonitorEdge_DeleteWhileTriggered checks a triggered monitor can
// still be removed and stops alerting.
func TestBalanceMonitorEdge_DeleteWhileTriggered(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)
	created := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)
	e.awaitWebhooks(t, 1)

	resp, _ := SetUpTestRequest(TestRequest{
		Method: "DELETE", Route: fmt.Sprintf("/balance-monitors/%s", created.MonitorID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	e.transfer(t, wallet.BalanceID, funding.BalanceID, 800)
	e.transfer(t, funding.BalanceID, wallet.BalanceID, 800)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "a deleted monitor alerts no further")

	_, err := e.blnk.GetMonitorByID(context.Background(), created.MonitorID)
	assert.Error(t, err)
}

// inflight records a transaction that holds its amount in flight and returns it.
func (e *monitorE2E) inflight(t *testing.T, source, destination string, amount float64) model.Transaction {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.RecordTransaction{
		Amount: amount, Precision: 1, Reference: gofakeit.UUID(), Description: "inflight",
		Currency: "USD", Source: source, Destination: destination,
		Inflight: true, SkipQueue: true, AllowOverDraft: true,
	})

	var txn model.Transaction
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Response: &txn,
		Method: "POST", Route: "/transactions", Router: e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code)
	require.NotEmpty(t, txn.TransactionID)
	return txn
}

func (e *monitorE2E) settleInflight(t *testing.T, txn model.Transaction, action string) {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.InflightUpdate{Status: action, SkipQueue: true})
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Method: "PUT",
		Route: fmt.Sprintf("/transactions/inflight/%s", txn.TransactionID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code, "inflight %s failed: %s", action, resp.Body.String())
}
