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
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// recordingCache notes which keys were invalidated.
type recordingCache struct {
	mu      sync.Mutex
	deleted []string
}

func (c *recordingCache) Set(_ context.Context, _ string, _ interface{}, _ time.Duration) error {
	return nil
}
func (c *recordingCache) Get(_ context.Context, _ string, _ interface{}) error { return nil }
func (c *recordingCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, key)
	return nil
}

// The monitor cache is keyed by balance. An update that moves a monitor to a
// different balance has to invalidate both, or the monitor stays in the old
// balance's cached list and keeps being evaluated against a balance it no
// longer watches.
func TestUpdateMonitor_InvalidatesBothBalancesWhenMoved(t *testing.T) {
	h := newMonitorHarness(t)
	recorder := &recordingCache{}
	h.blnk.cache = recorder

	stored := &model.BalanceMonitor{
		MonitorID: "mon_move",
		BalanceID: "bln_old",
		Condition: model.AlertCondition{Field: "balance", Operator: "<", PreciseValue: big.NewInt(100)},
	}
	h.ds.On("GetMonitorByID", "mon_move").Return(stored, nil).Once()
	h.ds.On("UpdateMonitor", mock.Anything).Return(nil).Once()

	moved := &model.BalanceMonitor{
		MonitorID: "mon_move",
		BalanceID: "bln_new",
		Condition: model.AlertCondition{Field: "balance", Operator: "<", PreciseValue: big.NewInt(100)},
	}
	require.NoError(t, h.blnk.UpdateMonitor(context.Background(), moved))

	assert.ElementsMatch(t, []string{"monitors:bln_new", "monitors:bln_old"}, recorder.deleted)
	h.ds.AssertExpectations(t)
}

func TestUpdateMonitor_InvalidatesOnceWhenTheBalanceIsUnchanged(t *testing.T) {
	h := newMonitorHarness(t)
	recorder := &recordingCache{}
	h.blnk.cache = recorder

	stored := &model.BalanceMonitor{
		MonitorID: "mon_same",
		BalanceID: "bln_same",
		Condition: model.AlertCondition{Field: "balance", Operator: "<"},
	}
	h.ds.On("GetMonitorByID", "mon_same").Return(stored, nil).Once()
	h.ds.On("UpdateMonitor", mock.Anything).Return(nil).Once()

	require.NoError(t, h.blnk.UpdateMonitor(context.Background(), &model.BalanceMonitor{
		MonitorID: "mon_same", BalanceID: "bln_same",
	}))

	assert.Equal(t, []string{"monitors:bln_same"}, recorder.deleted)
}

// A Go caller that builds AlertCondition{Value: 0} looks the same as an
// omitted JSON value. SetValue marks the zero as intentional.
func TestUpdateMonitor_SetValueZeroLowersTheStoredLine(t *testing.T) {
	h := newMonitorHarness(t)
	stored := &model.BalanceMonitor{
		MonitorID:   "mon_zero",
		BalanceID:   "bln_zero",
		Description: "named",
		Condition:   model.AlertCondition{Field: "balance", Operator: ">", Value: 500, Precision: 1},
	}
	h.ds.On("GetMonitorByID", "mon_zero").Return(stored, nil).Once()
	h.ds.On("UpdateMonitor", mock.MatchedBy(func(m *model.BalanceMonitor) bool {
		return m.Condition.Value == 0 && m.Description == "named"
	})).Return(nil).Once()

	update := &model.BalanceMonitor{MonitorID: "mon_zero"}
	update.Condition.SetValue(0)
	require.NoError(t, h.blnk.UpdateMonitor(context.Background(), update))
	h.ds.AssertExpectations(t)
}

func TestUpdateMonitor_SetDescriptionEmptyClearsTheName(t *testing.T) {
	h := newMonitorHarness(t)
	stored := &model.BalanceMonitor{
		MonitorID:   "mon_name",
		BalanceID:   "bln_name",
		Description: "named",
		Condition:   model.AlertCondition{Field: "balance", Operator: ">", Value: 500, Precision: 1},
	}
	h.ds.On("GetMonitorByID", "mon_name").Return(stored, nil).Once()
	h.ds.On("UpdateMonitor", mock.MatchedBy(func(m *model.BalanceMonitor) bool {
		return m.Description == "" && m.Condition.Value == 500
	})).Return(nil).Once()

	update := &model.BalanceMonitor{MonitorID: "mon_name"}
	update.SetDescription("")
	require.NoError(t, h.blnk.UpdateMonitor(context.Background(), update))
	h.ds.AssertExpectations(t)
}

func TestUpdateMonitor_GoZeroValueWithoutSetValueKeepsTheStoredLine(t *testing.T) {
	h := newMonitorHarness(t)
	stored := &model.BalanceMonitor{
		MonitorID: "mon_keep",
		BalanceID: "bln_keep",
		Condition: model.AlertCondition{Field: "balance", Operator: ">", Value: 500, Precision: 1},
	}
	h.ds.On("GetMonitorByID", "mon_keep").Return(stored, nil).Once()
	h.ds.On("UpdateMonitor", mock.MatchedBy(func(m *model.BalanceMonitor) bool {
		return m.Condition.Value == 500
	})).Return(nil).Once()

	require.NoError(t, h.blnk.UpdateMonitor(context.Background(), &model.BalanceMonitor{
		MonitorID: "mon_keep",
		Condition: model.AlertCondition{Value: 0},
	}))
	h.ds.AssertExpectations(t)
}
