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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
)

// monitorE2E drives the whole path a real deployment takes: an HTTP request,
// the ledger, the post-commit monitor check, and the webhook task that a worker
// would pick up.
type monitorE2E struct {
	router    *gin.Engine
	blnk      *blnk.Blnk
	cnf       *config.Configuration
	inspector *asynq.Inspector
	queue     string
}

func setupMonitorE2E(t *testing.T) *monitorE2E {
	t.Helper()

	queue := fmt.Sprintf("monitor_e2e_%d", time.Now().UnixNano())
	router, b, cnf := setupRouterWithConfig(t, func(cfg *config.Configuration) {
		cfg.Queue.WebhookQueue = queue
		// SendWebhook returns early when no URL is set, so the queue would stay
		// empty however correct the monitor logic was.
		cfg.Notification = config.Notification{Webhook: config.WebhookConfig{Url: "http://127.0.0.1:1/webhook"}}
	})

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = inspector.Close() })

	return &monitorE2E{router: router, blnk: b, cnf: cnf, inspector: inspector, queue: queue}
}

// pendingTasks returns every pending task on this harness's queue. The ledger
// puts its own webhooks on the same queue and a busy test overruns a single
// page, so this pages to the end rather than sampling the first one.
func (e *monitorE2E) pendingTasks() []*asynq.TaskInfo {
	const pageSize = 200

	var all []*asynq.TaskInfo
	for page := 1; ; page++ {
		batch, err := e.inspector.ListPendingTasks(e.queue, asynq.PageSize(pageSize), asynq.Page(page))
		if err != nil {
			return all
		}
		all = append(all, batch...)
		if len(batch) < pageSize {
			return all
		}
	}
}

// webhooks counts the balance.monitor tasks waiting on the queue. The payload,
// not the queue depth, is what the assertions are about.
func (e *monitorE2E) webhooks() int {
	count := 0
	for _, task := range e.pendingTasks() {
		var hook struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(task.Payload, &hook); err != nil {
			continue
		}
		if hook.Event == "balance.monitor" {
			count++
		}
	}
	return count
}

// webhooksFor counts the alerts owed to one monitor, so a test with several
// monitors on a queue can pin each one's share rather than only the total.
func (e *monitorE2E) webhooksFor(monitorID string) int {
	count := 0
	for _, task := range e.pendingTasks() {
		if strings.Contains(string(task.Payload), monitorID) {
			count++
		}
	}
	return count
}

// awaitWebhooks waits for the detached post-commit goroutines to settle.
func (e *monitorE2E) awaitWebhooks(t *testing.T, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return e.webhooks() == want }, 5*time.Second, 25*time.Millisecond,
		"expected %d webhooks, queue holds %d", want, e.webhooks())
}

// settle gives any further goroutine a chance to enqueue, so a test that asserts
// "no more webhooks" is not just winning a race.
func (e *monitorE2E) settle() { time.Sleep(300 * time.Millisecond) }

func (e *monitorE2E) newBalance(t *testing.T) model.Balance {
	t.Helper()

	ledger, err := e.blnk.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)

	balance, err := e.blnk.CreateBalance(context.Background(), model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	return balance
}

func (e *monitorE2E) createMonitor(t *testing.T, balanceID, trigger string, value float64) model.BalanceMonitor {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.CreateBalanceMonitor{
		BalanceId: balanceID,
		Trigger:   trigger,
		Condition: model2.MonitorCondition{Field: "balance", Operator: ">", Value: value, Precision: 1},
	})

	var monitor model.BalanceMonitor
	resp, _ := SetUpTestRequest(TestRequest{
		Payload:  payloadBytes,
		Response: &monitor,
		Method:   "POST",
		Route:    "/balance-monitors",
		Router:   e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code)

	return monitor
}

// transfer moves amount from source to destination synchronously, so the
// monitor check has run by the time the request returns.
func (e *monitorE2E) transfer(t *testing.T, source, destination string, amount float64) {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.RecordTransaction{
		Amount:      amount,
		Precision:   1,
		Reference:   gofakeit.UUID(),
		Description: "monitor e2e",
		Currency:    "USD",
		Source:      source,
		Destination: destination,
		SkipQueue:   true,
		// The funding balance is a plain ledger balance, not @world, so the
		// debits that move the wallet have to be allowed to take it negative.
		AllowOverDraft: true,
	})

	resp, _ := SetUpTestRequest(TestRequest{
		Payload: payloadBytes,
		Method:  "POST",
		Route:   "/transactions",
		Router:  e.router,
	})
	require.Equal(t, http.StatusCreated, resp.Code)
}

func (e *monitorE2E) monitorState(t *testing.T, monitorID string) bool {
	t.Helper()
	monitor, err := e.blnk.GetMonitorByID(context.Background(), monitorID)
	require.NoError(t, err)
	return monitor.ConditionState
}

// TestBalanceMonitorEdge_EndToEnd walks a balance across its threshold twice
// over six transactions and asserts the consumer is told twice, not five times.
func TestBalanceMonitorEdge_EndToEnd(t *testing.T) {
	e := setupMonitorE2E(t)

	funding := e.newBalance(t)
	wallet := e.newBalance(t)
	monitor := e.createMonitor(t, wallet.BalanceID, model.TriggerEdge, 500)

	require.Equal(t, 0, e.webhooks())
	require.False(t, e.monitorState(t, monitor.MonitorID))

	// Below the threshold: nothing to say.
	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.settle()
	assert.Equal(t, 0, e.webhooks())
	assert.False(t, e.monitorState(t, monitor.MonitorID))

	// The crossing.
	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.awaitWebhooks(t, 1)
	assert.True(t, e.monitorState(t, monitor.MonitorID), "the monitor is left triggered")

	// Still past the threshold, so the consumer already knows.
	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "an edge monitor stays quiet while the condition holds")

	// Back under: re-arm, silently.
	e.transfer(t, wallet.BalanceID, funding.BalanceID, 400)
	e.settle()
	assert.Equal(t, 1, e.webhooks(), "recovering is not an alert")
	assert.False(t, e.monitorState(t, monitor.MonitorID), "the monitor re-arms")

	// The second crossing.
	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.awaitWebhooks(t, 2)
	assert.True(t, e.monitorState(t, monitor.MonitorID))
}

// TestBalanceMonitorLevel_EndToEnd runs the identical ledger activity against a
// level-triggered monitor, which is what today's behaviour looks like and what
// the opt-in still has to deliver.
func TestBalanceMonitorLevel_EndToEnd(t *testing.T) {
	e := setupMonitorE2E(t)

	funding := e.newBalance(t)
	wallet := e.newBalance(t)
	monitor := e.createMonitor(t, wallet.BalanceID, model.TriggerLevel, 500)

	require.Equal(t, 0, e.webhooks())

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.settle()
	assert.Equal(t, 0, e.webhooks())

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.awaitWebhooks(t, 1)

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.awaitWebhooks(t, 2)

	e.transfer(t, wallet.BalanceID, funding.BalanceID, 400)
	e.settle()
	assert.Equal(t, 2, e.webhooks())

	e.transfer(t, funding.BalanceID, wallet.BalanceID, 300)
	e.awaitWebhooks(t, 3)

	assert.False(t, e.monitorState(t, monitor.MonitorID),
		"a level monitor keeps no state, so nothing writes to it")
}
