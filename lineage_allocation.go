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
	"fmt"
	"math/big"
	"sort"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

func (l *Blnk) getLineageSources(ctx context.Context, mappings []model.LineageMapping) ([]LineageSource, error) {
	if len(mappings) == 0 {
		return nil, nil
	}

	// Collect all shadow balance IDs for batch query
	shadowBalanceIDs := make([]string, 0, len(mappings))
	for _, mapping := range mappings {
		shadowBalanceIDs = append(shadowBalanceIDs, mapping.ShadowBalanceID)
	}

	// Fetch all shadow balances in a single query
	balances, err := l.datasource.GetBalancesByIDsLite(ctx, shadowBalanceIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shadow balances: %w", err)
	}

	// Build sources from the fetched balances
	var sources []LineageSource
	for _, mapping := range mappings {
		balance, exists := balances[mapping.ShadowBalanceID]
		if !exists {
			continue
		}

		if balance.DebitBalance != nil && balance.CreditBalance != nil && balance.DebitBalance.Cmp(big.NewInt(0)) > 0 {
			available := new(big.Int).Sub(balance.DebitBalance, balance.CreditBalance)
			if available.Cmp(big.NewInt(0)) > 0 {
				sources = append(sources, LineageSource{
					BalanceID: mapping.ShadowBalanceID,
					Balance:   available,
					CreatedAt: mapping.CreatedAt,
				})
			}
		}
	}

	return sources, nil
}

// calculateAllocation calculates fund allocations based on the specified strategy.
// Supported strategies are FIFO, LIFO, and PROPORTIONAL.
//
// Parameters:
// - sources []LineageSource: The available fund sources.
// - amount *big.Int: The amount to allocate.
// - strategy string: The allocation strategy (FIFO, LIFO, or PROPORTIONAL).
//
// Returns:
// - []Allocation: The calculated allocations.
func (l *Blnk) calculateAllocation(sources []LineageSource, amount *big.Int, strategy string) []Allocation {
	if len(sources) == 0 {
		return nil
	}

	switch strategy {
	case AllocationLIFO:
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].CreatedAt.After(sources[j].CreatedAt)
		})
		return l.sequentialAllocation(sources, amount)
	case AllocationProp:
		return l.proportionalAllocation(sources, amount)
	default:
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].CreatedAt.Before(sources[j].CreatedAt)
		})
		return l.sequentialAllocation(sources, amount)
	}
}

// sequentialAllocation allocates funds sequentially from sources (used for FIFO/LIFO).
//
// Parameters:
// - sources []LineageSource: The available fund sources in order.
// - amount *big.Int: The amount to allocate.
//
// Returns:
// - []Allocation: The calculated allocations.
func (l *Blnk) sequentialAllocation(sources []LineageSource, amount *big.Int) []Allocation {
	var allocations []Allocation

	// Skip if amount is zero or negative
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}

	remaining := new(big.Int).Set(amount)

	for _, source := range sources {
		if remaining.Cmp(big.NewInt(0)) <= 0 {
			break
		}

		alloc := new(big.Int)
		if source.Balance.Cmp(remaining) >= 0 {
			alloc.Set(remaining)
		} else {
			alloc.Set(source.Balance)
		}

		// Skip zero allocations
		if alloc.Cmp(big.NewInt(0)) <= 0 {
			continue
		}

		allocations = append(allocations, Allocation{
			BalanceID: source.BalanceID,
			Amount:    alloc,
		})

		remaining.Sub(remaining, alloc)
	}

	// Log warning if we couldn't allocate the full amount
	if remaining.Cmp(big.NewInt(0)) > 0 {
		logrus.Warnf("sequential allocation: could not allocate full amount, %s remaining unallocated", remaining.String())
	}

	return allocations
}

// proportionalAllocation allocates funds proportionally across all sources.
//
// Parameters:
// - sources []LineageSource: The available fund sources.
// - amount *big.Int: The amount to allocate.
//
// Returns:
// - []Allocation: The calculated allocations.
func (l *Blnk) proportionalAllocation(sources []LineageSource, amount *big.Int) []Allocation {
	var allocations []Allocation

	// Skip if amount is zero or negative
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}

	total := big.NewInt(0)
	for _, source := range sources {
		total.Add(total, source.Balance)
	}

	if total.Cmp(big.NewInt(0)) == 0 {
		return nil
	}

	// Cap amount at total available to prevent over-allocation
	effectiveAmount := new(big.Int).Set(amount)
	if effectiveAmount.Cmp(total) > 0 {
		logrus.Warnf("proportional allocation: requested amount %s exceeds total available %s, capping at available", amount.String(), total.String())
		effectiveAmount.Set(total)
	}

	remaining := new(big.Int).Set(effectiveAmount)

	for i, source := range sources {
		var alloc *big.Int

		if i == len(sources)-1 {
			// Last source gets the remainder to handle rounding
			alloc = new(big.Int).Set(remaining)
		} else {
			// Calculate proportional share: (effectiveAmount * source.Balance) / total
			proportion := new(big.Int).Mul(effectiveAmount, source.Balance)
			alloc = new(big.Int).Div(proportion, total)
		}

		// Cap at source's available balance
		if alloc.Cmp(source.Balance) > 0 {
			alloc.Set(source.Balance)
		}

		// Skip zero allocations
		if alloc.Cmp(big.NewInt(0)) <= 0 {
			continue
		}

		allocations = append(allocations, Allocation{
			BalanceID: source.BalanceID,
			Amount:    alloc,
		})

		remaining.Sub(remaining, alloc)
	}

	return allocations
}

// findMappingByShadowID finds a lineage mapping by its shadow balance ID.
//
// Parameters:
// - mappings []model.LineageMapping: The lineage mappings to search.
// - shadowBalanceID string: The shadow balance ID to find.
//
// Returns:
// - *model.LineageMapping: The matching mapping, or nil if not found.
func (l *Blnk) findMappingByShadowID(mappings []model.LineageMapping, shadowBalanceID string) *model.LineageMapping {
	for i := range mappings {
		if mappings[i].ShadowBalanceID == shadowBalanceID {
			return &mappings[i]
		}
	}
	return nil
}

// GetBalanceLineage retrieves the fund lineage for a balance.
// It returns a breakdown of funds by provider, showing how much was received and spent from each source.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The ID of the balance to get lineage for.
//
// Returns:
// - *BalanceLineage: The lineage information for the balance.
// - error: An error if the lineage could not be retrieved.
