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
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
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

func (e *monitorE2E) createMonitorOn(t *testing.T, balanceID, field, operator string, value float64) model.BalanceMonitor {
	t.Helper()

	payloadBytes, _ := request.ToJsonReq(&model2.CreateBalanceMonitor{
		BalanceId: balanceID,
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
