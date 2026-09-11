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

package blnk

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/model"
)

// noopCache stands in for the Redis-backed monitor cache. Every Get is a miss,
// so each evaluation goes to the datasource and the test controls exactly what
// the monitor list contains.
type noopCache struct{}

func (noopCache) Set(_ context.Context, _ string, _ interface{}, _ time.Duration) error { return nil }
func (noopCache) Get(_ context.Context, _ string, _ interface{}) error                  { return nil }
func (noopCache) Delete(_ context.Context, _ string) error                              { return nil }

// monitorHarness is a Blnk wired to a mock datasource and a real asynq client,
// so an enqueued balance.monitor webhook is observable through the queue rather
// than inferred.
type monitorHarness struct {
	blnk      *Blnk
	ds        *mocks.MockDataSource
	inspector *asynq.Inspector
	queue     string
}

func newMonitorHarness(t *testing.T) *monitorHarness {
	t.Helper()

	queue := fmt.Sprintf("edge_test_webhook_%d", time.Now().UnixNano())
	// Stored directly rather than through MockConfig, which runs the full
	// validation this partial test configuration cannot satisfy.
	config.ConfigStore.Store(&config.Configuration{
		Redis:              config.RedisConfig{Dns: "localhost:6379"},
		Queue:              config.QueueConfig{WebhookQueue: queue, TransactionQueue: queue + "_txn", IndexQueue: queue + "_idx", NumberOfQueues: 1},
		Server:             config.ServerConfig{SecretKey: "some-secret"},
		TokenizationSecret: "12345678901234567890123456789012",
		// A configured URL is what makes SendWebhook enqueue rather than
		// return early; nothing ever delivers to it in these tests.
		Notification: config.Notification{Webhook: config.WebhookConfig{Url: "http://127.0.0.1:1/webhook"}},
	})

	ds := new(mocks.MockDataSource)
	b, err := NewBlnk(ds)
	require.NoError(t, err)
	b.cache = noopCache{}

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = inspector.Close() })

	return &monitorHarness{blnk: b, ds: ds, inspector: inspector, queue: queue}
}

// enqueued reports how many webhook tasks are waiting on this harness's queue.
func (h *monitorHarness) enqueued(t *testing.T) int {
	t.Helper()
	info, err := h.inspector.GetQueueInfo(h.queue)
	if err != nil {
		// asynq reports a queue it has never seen as not found, which is the
		// same thing as empty for our purposes.
		return 0
	}
	return info.Pending + info.Active + info.Scheduled + info.Retry
}

func monitorFor(trigger string) model.BalanceMonitor {
	return model.BalanceMonitor{
		MonitorID: "mon_edge_test",
		BalanceID: "bln_edge_test",
		Trigger:   trigger,
		Condition: model.AlertCondition{
			Field:        "balance",
			Operator:     "<",
			PreciseValue: big.NewInt(100),
		},
	}
}

func balanceAt(value int64, version int64) *model.Balance {
	b := &model.Balance{BalanceID: "bln_edge_test", Balance: big.NewInt(value), Version: version}
	b.InitializeBalanceFields()
	return b
}

func TestCheckBalanceMonitors_EdgeFiresOnceOnCrossing(t *testing.T) {
	h := newMonitorHarness(t)
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor(model.TriggerEdge)}, nil)

	// The crossing: this evaluation owns the false -> true transition.
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(2)).Return(true, nil).Once()
	// Still below the threshold, so the state does not move and nothing is owned.
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(3)).Return(false, nil).Once()
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(4)).Return(false, nil).Once()

	before := h.enqueued(t)
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(70, 3))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(55, 4))

	assert.Equal(t, 1, h.enqueued(t)-before, "three transactions below the threshold must produce exactly one webhook")
	h.ds.AssertExpectations(t)
}

func TestCheckBalanceMonitors_EdgeRearmsAndFiresAgain(t *testing.T) {
	h := newMonitorHarness(t)
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor(model.TriggerEdge)}, nil)

	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(2)).Return(true, nil).Once()
	// Recovered: the monitor re-arms. Owning a true -> false transition must not
	// send anything.
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", false, int64(3)).Return(true, nil).Once()
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(4)).Return(true, nil).Once()

	before := h.enqueued(t)
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(120, 3))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(85, 4))

	assert.Equal(t, 2, h.enqueued(t)-before, "a monitor that recovers and crosses again must alert on the second crossing")
	h.ds.AssertExpectations(t)
}

func TestCheckBalanceMonitors_EdgeStaleEvaluationDoesNotFire(t *testing.T) {
	h := newMonitorHarness(t)
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor(model.TriggerEdge)}, nil)

	// An evaluation carrying an older balance version loses the version fence
	// and is discarded, however true its condition looks.
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(2)).Return(false, nil).Once()

	before := h.enqueued(t)
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))

	assert.Equal(t, 0, h.enqueued(t)-before)
	h.ds.AssertExpectations(t)
}

func TestCheckBalanceMonitors_LevelFiresEveryTime(t *testing.T) {
	h := newMonitorHarness(t)
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor(model.TriggerLevel)}, nil)

	before := h.enqueued(t)
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(70, 3))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(150, 4))

	assert.Equal(t, 2, h.enqueued(t)-before, "level triggering alerts on every update while the condition holds")
	h.ds.AssertNotCalled(t, "TransitionMonitorState", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestCheckBalanceMonitors_UnsetTriggerIsEdge(t *testing.T) {
	h := newMonitorHarness(t)
	// A row or cache entry written before the column existed decodes with an
	// empty trigger and must behave as edge, not fall back to level.
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor("")}, nil)
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(2)).Return(true, nil).Once()
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(3)).Return(false, nil).Once()

	before := h.enqueued(t)
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(80, 3))

	assert.Equal(t, 1, h.enqueued(t)-before)
	h.ds.AssertExpectations(t)
}

func TestCheckBalanceMonitors_EdgeReleasesWhenTheWebhookCannotBeQueued(t *testing.T) {
	h := newMonitorHarness(t)
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor(model.TriggerEdge)}, nil)
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(2)).Return(true, nil).Once()
	h.ds.On("ReleaseMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", int64(2)).Return(nil).Once()

	// An edge fires once, so a transition this process owned but could not hand
	// off has to go back, or the crossing is lost for good.
	require.NoError(t, h.blnk.asynqClient.Close())

	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))

	h.ds.AssertExpectations(t)
}

func TestCheckBalanceMonitors_EdgeTransitionErrorDoesNotFire(t *testing.T) {
	h := newMonitorHarness(t)
	h.ds.On("GetBalanceMonitors", "bln_edge_test").Return([]model.BalanceMonitor{monitorFor(model.TriggerEdge)}, nil)
	h.ds.On("TransitionMonitorState", mock.Anything, "mon_edge_test", "bln_edge_test", true, int64(2)).Return(false, errors.New("boom")).Once()

	before := h.enqueued(t)
	h.blnk.checkBalanceMonitors(context.Background(), balanceAt(90, 2))

	assert.Equal(t, 0, h.enqueued(t)-before, "a monitor whose state could not be written must not alert on an unproven crossing")
	h.ds.AssertExpectations(t)
}

// preUpgradeMonitor is the shape a BalanceMonitor was cached in before the
// trigger existed. Cache entries outlive a deploy, so the decode has to be
// exercised, not assumed.
type preUpgradeMonitor struct {
	MonitorID   string               `json:"monitor_id"`
	BalanceID   string               `json:"balance_id"`
	Description string               `json:"description,omitempty"`
	CallBackURL string               `json:"-"`
	CreatedAt   time.Time            `json:"created_at"`
	Condition   model.AlertCondition `json:"condition"`
}

func TestMonitorCache_PreUpgradeEntryDecodesAsEdge(t *testing.T) {
	h := newMonitorHarness(t)
	redisCache := cache.NewCacheWithClient(h.blnk.redis)
	ctx := context.Background()
	key := fmt.Sprintf("monitors:cache_decode_%d", time.Now().UnixNano())

	require.NoError(t, redisCache.Set(ctx, key, []preUpgradeMonitor{{
		MonitorID: "mon_old",
		BalanceID: "bln_old",
		Condition: model.AlertCondition{Field: "balance", Operator: "<", PreciseValue: big.NewInt(100)},
	}}, time.Minute))

	var decoded []model.BalanceMonitor
	require.NoError(t, redisCache.Get(ctx, key, &decoded))
	require.Len(t, decoded, 1)

	assert.Empty(t, decoded[0].Trigger, "an entry written before the field existed has no trigger")
	assert.Equal(t, model.TriggerEdge, decoded[0].TriggerMode(), "and must still be treated as edge")
	assert.Equal(t, "mon_old", decoded[0].MonitorID)
}

func TestMonitorCache_RoundTripsTheTrigger(t *testing.T) {
	h := newMonitorHarness(t)
	redisCache := cache.NewCacheWithClient(h.blnk.redis)
	ctx := context.Background()
	key := fmt.Sprintf("monitors:cache_roundtrip_%d", time.Now().UnixNano())

	require.NoError(t, redisCache.Set(ctx, key, []model.BalanceMonitor{
		monitorFor(model.TriggerLevel),
		monitorFor(model.TriggerEdge),
	}, time.Minute))

	var decoded []model.BalanceMonitor
	require.NoError(t, redisCache.Get(ctx, key, &decoded))
	require.Len(t, decoded, 2)
	assert.Equal(t, model.TriggerLevel, decoded[0].TriggerMode())
	assert.Equal(t, model.TriggerEdge, decoded[1].TriggerMode())
}
