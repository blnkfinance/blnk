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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// ledgerWalk is a random but fully determined sequence of transfers, together
// with the balance the wallet holds after each one.
type ledgerWalk struct {
	amounts  []float64 // positive credits the wallet, negative debits it
	balances []int64
}

func newLedgerWalk(seed int64, steps int) ledgerWalk {
	rng := rand.New(rand.NewSource(seed))
	walk := ledgerWalk{}
	balance := int64(0)
	for i := 0; i < steps; i++ {
		amount := float64(rng.Intn(400) + 50)
		if rng.Intn(2) == 0 {
			amount = -amount
		}
		balance += int64(amount)
		walk.amounts = append(walk.amounts, amount)
		walk.balances = append(walk.balances, balance)
	}
	return walk
}

// expectedEdges counts false -> true transitions of "balance > threshold",
// which is the number of alerts an edge monitor owes the consumer.
func (w ledgerWalk) expectedEdges(threshold int64) int {
	count, previous := 0, false
	for _, balance := range w.balances {
		current := balance > threshold
		if current && !previous {
			count++
		}
		previous = current
	}
	return count
}

// expectedLevels counts every step where the condition holds, which is what a
// level monitor owes.
func (w ledgerWalk) expectedLevels(threshold int64) int {
	count := 0
	for _, balance := range w.balances {
		if balance > threshold {
			count++
		}
	}
	return count
}

// TestBalanceMonitor_AlertCountMatchesTheLedgerWalk drives randomised ledger
// activity and checks the alert count against a definition of the semantics
// computed independently of the implementation. Hand-picked sequences only
// prove the cases someone thought of; this one wanders across the threshold in
// ways nobody chose.
func TestBalanceMonitor_AlertCountMatchesTheLedgerWalk(t *testing.T) {
	const threshold = 500

	for _, seed := range []int64{1, 7, 13, 42, 99} {
		walk := newLedgerWalk(seed, 14)

		t.Run("edge", func(t *testing.T) {
			e := setupMonitorE2E(t)
			funding := e.newBalance(t)
			wallet := e.newBalance(t)
			monitor := e.createMonitor(t, wallet.BalanceID, model.TriggerEdge, threshold)

			for _, amount := range walk.amounts {
				if amount > 0 {
					e.transfer(t, funding.BalanceID, wallet.BalanceID, amount)
				} else {
					e.transfer(t, wallet.BalanceID, funding.BalanceID, -amount)
				}
			}

			want := walk.expectedEdges(threshold)
			e.awaitWebhooks(t, want)
			e.settle()
			assert.Equal(t, want, e.webhooks(),
				"seed %d: %d transactions crossing %d must alert on each false->true transition only",
				seed, len(walk.amounts), threshold)

			finalState := walk.balances[len(walk.balances)-1] > threshold
			assert.Equal(t, finalState, e.monitorState(t, monitor.MonitorID),
				"seed %d: the stored state must match the final balance", seed)
		})

		t.Run("level", func(t *testing.T) {
			e := setupMonitorE2E(t)
			funding := e.newBalance(t)
			wallet := e.newBalance(t)
			e.createMonitor(t, wallet.BalanceID, model.TriggerLevel, threshold)

			for _, amount := range walk.amounts {
				if amount > 0 {
					e.transfer(t, funding.BalanceID, wallet.BalanceID, amount)
				} else {
					e.transfer(t, wallet.BalanceID, funding.BalanceID, -amount)
				}
			}

			want := walk.expectedLevels(threshold)
			e.awaitWebhooks(t, want)
			e.settle()
			assert.Equal(t, want, e.webhooks(),
				"seed %d: level triggering alerts on every update while the condition holds", seed)
		})
	}
}

// TestBalanceMonitor_EdgeIsNeverNoisierThanLevel is the property the whole
// change exists to deliver, stated directly.
func TestBalanceMonitor_EdgeIsNeverNoisierThanLevel(t *testing.T) {
	const threshold = 500

	for _, seed := range []int64{3, 21, 55} {
		walk := newLedgerWalk(seed, 16)
		edges := walk.expectedEdges(threshold)
		levels := walk.expectedLevels(threshold)
		require.LessOrEqual(t, edges, levels, "seed %d: the walk itself must satisfy the property", seed)

		e := setupMonitorE2E(t)
		funding := e.newBalance(t)
		edgeWallet := e.newBalance(t)
		levelWallet := e.newBalance(t)
		e.createMonitor(t, edgeWallet.BalanceID, model.TriggerEdge, threshold)
		e.createMonitor(t, levelWallet.BalanceID, model.TriggerLevel, threshold)

		for _, wallet := range []string{edgeWallet.BalanceID, levelWallet.BalanceID} {
			for _, amount := range walk.amounts {
				if amount > 0 {
					e.transfer(t, funding.BalanceID, wallet, amount)
				} else {
					e.transfer(t, wallet, funding.BalanceID, -amount)
				}
			}
		}

		e.awaitWebhooks(t, edges+levels)
		e.settle()
		assert.Equal(t, edges+levels, e.webhooks(), "seed %d", seed)
	}
}
