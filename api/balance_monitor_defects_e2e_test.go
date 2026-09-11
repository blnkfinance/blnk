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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// execSQL runs a statement directly against the test database, for setting up
// rows the API deliberately cannot produce.
func execSQL(t *testing.T, cnf *config.Configuration, statement string, args ...interface{}) {
	t.Helper()

	ds, err := database.GetDBConnection(cnf)
	require.NoError(t, err)
	_, err = ds.Conn.ExecContext(context.Background(), statement, args...)
	require.NoError(t, err)
}

// A monitor written before the precision migration carries NULLs. Reading it
// used to fail, and because checkBalanceMonitors gives up when the fetch
// errors, that one row silenced every other monitor on the same balance. This
// is the symptom an operator would actually see.
func TestBalanceMonitor_LegacyRowDoesNotSilenceItsSiblings(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)

	healthy := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 500)

	// The shape a row created before precision and precise_value existed still
	// has today: both columns NULL.
	legacyID := model.GenerateUUIDWithSuffix("mon")
	execSQL(t, e.cnf, `
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, description, call_back_url, created_at)
		VALUES ($1, $2, 'balance', '>', 500, 'legacy', '', NOW())
	`, legacyID, wallet.BalanceID)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 900)

	// Both monitors watch the same threshold, so both owe one alert.
	e.awaitWebhooks(t, 2)
	assert.True(t, e.monitorState(t, healthy.MonitorID),
		"a legacy sibling must not stop a healthy monitor from working")
	assert.True(t, e.monitorState(t, legacyID), "and the legacy monitor itself must work")
}

// A moved edge monitor stops alerting on the balance it left and starts on the
// one it joined. This one holds even with a stale cached list, because the
// balance guard on the state write rejects the stale evaluation; the cache fix
// is what covers the level case below.
func TestBalanceMonitor_MovedMonitorFollowsItsBalance(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, oldWallet, newWallet := e.newBalance(t), e.newBalance(t), e.newBalance(t)

	created := e.createMonitorOn(t, oldWallet.BalanceID, model.TriggerEdge, "balance", ">", 500)

	// Warm the old balance's cached monitor list.
	e.transfer(t, funding.BalanceID, oldWallet.BalanceID, 100)
	e.settle()
	require.Equal(t, 0, e.webhooks())

	payloadBytes, _ := request.ToJsonReq(&model.BalanceMonitor{
		BalanceID: newWallet.BalanceID,
		Trigger:   model.TriggerEdge,
		Condition: model.AlertCondition{Field: "balance", Operator: ">", Value: 500, Precision: 1},
	})
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Method: "PUT",
		Route: fmt.Sprintf("/balance-monitors/%s", created.MonitorID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	// The balance it left must no longer produce alerts.
	e.transfer(t, funding.BalanceID, oldWallet.BalanceID, 900)
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "a moved monitor must stop watching the balance it left")

	// The balance it joined must.
	e.transfer(t, funding.BalanceID, newWallet.BalanceID, 900)
	e.awaitWebhooks(t, 1)
	assert.True(t, e.monitorState(t, created.MonitorID))
}

// The balance guard covers an edge monitor moved between balances, because the
// stale evaluation cannot win the state write. A level monitor keeps no state,
// so nothing stops it firing on the balance it left -- only invalidating that
// balance's cached list does.
func TestBalanceMonitor_MovedLevelMonitorStopsAlertingOnTheOldBalance(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, oldWallet, newWallet := e.newBalance(t), e.newBalance(t), e.newBalance(t)

	created := e.createMonitorOn(t, oldWallet.BalanceID, model.TriggerLevel, "balance", ">", 500)

	// Put the old balance's monitor list in the cache.
	e.transfer(t, funding.BalanceID, oldWallet.BalanceID, 100)
	e.settle()
	require.Equal(t, 0, e.webhooks())

	payloadBytes, _ := request.ToJsonReq(&model.BalanceMonitor{
		BalanceID: newWallet.BalanceID,
		Trigger:   model.TriggerLevel,
		Condition: model.AlertCondition{Field: "balance", Operator: ">", Value: 500, Precision: 1},
	})
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Method: "PUT",
		Route: fmt.Sprintf("/balance-monitors/%s", created.MonitorID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	e.transfer(t, funding.BalanceID, oldWallet.BalanceID, 900)
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "a moved level monitor must not keep alerting on the balance it left")

	e.transfer(t, funding.BalanceID, newWallet.BalanceID, 900)
	e.awaitWebhooks(t, 1)
}

// A fractional threshold is a normal thing to want and used to fail at the
// database with a 500.
func TestBalanceMonitor_FractionalThresholdEndToEnd(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)

	payloadBytes, _ := request.ToJsonReq(&model2.CreateBalanceMonitor{
		BalanceId: wallet.BalanceID,
		Condition: model2.MonitorCondition{Field: "balance", Operator: ">", Value: 10.5, Precision: 100},
	})
	var created model.BalanceMonitor
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Response: &created,
		Method: "POST", Route: "/balance-monitors", Router: e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code, "body: %s", resp.Body.String())
	assert.Equal(t, 10.5, created.Condition.Value)

	// precision 100 means the threshold is 1050 in the ledger's own units.
	e.transferPrecise(t, funding.BalanceID, wallet.BalanceID, 10.40, 100)
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "10.40 has not passed 10.50")

	e.transferPrecise(t, funding.BalanceID, wallet.BalanceID, 0.20, 100)
	e.awaitWebhooks(t, 1)
	assert.True(t, e.monitorState(t, created.MonitorID))
}

// A zero threshold is the most natural monitor there is and used to be
// rejected outright.
func TestBalanceMonitor_ZeroThresholdEndToEnd(t *testing.T) {
	e := setupMonitorE2E(t)
	funding, wallet := e.newBalance(t), e.newBalance(t)

	created := e.createMonitorOn(t, wallet.BalanceID, model.TriggerEdge, "balance", ">", 0)

	e.transfer(t, wallet.BalanceID, funding.BalanceID, 100)
	e.settle()
	assert.Equal(t, 0, e.webhooks(), "a negative balance is not above zero")

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 200)
	e.awaitWebhooks(t, 1)
	assert.True(t, e.monitorState(t, created.MonitorID))
}

// The list endpoint used to report a different threshold from the detail
// endpoint, because it did not select precision.
func TestBalanceMonitor_ListAndDetailAgreeOnThreshold(t *testing.T) {
	e := setupMonitorE2E(t)
	wallet := e.newBalance(t)

	payloadBytes, _ := request.ToJsonReq(&model2.CreateBalanceMonitor{
		BalanceId: wallet.BalanceID,
		Condition: model2.MonitorCondition{Field: "balance", Operator: "<", Value: 100, Precision: 100},
	})
	var created model.BalanceMonitor
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Response: &created,
		Method: "POST", Route: "/balance-monitors", Router: e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code)

	var detail model.BalanceMonitor
	resp, _ = SetUpTestRequest(TestRequest{
		Response: &detail, Method: "GET",
		Route: fmt.Sprintf("/balance-monitors/%s", created.MonitorID), Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var all []model.BalanceMonitor
	resp, _ = SetUpTestRequest(TestRequest{
		Response: &all, Method: "GET", Route: "/balance-monitors", Router: e.router,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	for _, listed := range all {
		if listed.MonitorID != created.MonitorID {
			continue
		}
		assert.Equal(t, detail.Condition.Precision, listed.Condition.Precision, "list and detail must agree on precision")
		assert.Equal(t, detail.Condition.PreciseValue, listed.Condition.PreciseValue, "and on the threshold the ledger compares against")
		return
	}
	t.Fatal("the monitor did not appear in the list endpoint")
}

// transferPrecise moves an amount at a given precision.
func (e *monitorE2E) transferPrecise(t *testing.T, source, destination string, amount, precision float64) {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.RecordTransaction{
		Amount: amount, Precision: precision, Reference: model.GenerateUUIDWithSuffix("ref"),
		Description: "precise", Currency: "USD", Source: source, Destination: destination,
		SkipQueue: true, AllowOverDraft: true,
	})
	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes, Method: "POST", Route: "/transactions", Router: e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code, "body: %s", resp.Body.String())
}
