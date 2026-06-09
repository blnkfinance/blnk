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
	"strings"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

func (l *Blnk) updateFundAllocationMetadata(ctx context.Context, txn *model.Transaction, allocations []Allocation, mappings []model.LineageMapping) {
	if len(allocations) == 0 {
		return
	}

	fundAllocation := l.buildFundAllocationList(allocations, mappings, txn.Precision)
	if len(fundAllocation) == 0 {
		return
	}

	newMetadata := map[string]interface{}{
		LineageFundAllocation: fundAllocation,
	}
	if err := l.datasource.UpdateTransactionMetadata(ctx, txn.TransactionID, newMetadata); err != nil {
		logrus.Errorf("failed to update transaction with fund allocation: %v", err)
	}
}

// buildFundAllocationList builds a list of fund allocations for metadata storage.
//
// Parameters:
// - allocations []Allocation: The calculated allocations.
// - mappings []model.LineageMapping: The lineage mappings.
// - precision float64: The precision multiplier.
//
// Returns:
// - []map[string]interface{}: The fund allocation list for metadata.
func (l *Blnk) buildFundAllocationList(allocations []Allocation, mappings []model.LineageMapping, precision float64) []map[string]interface{} {
	fundAllocation := make([]map[string]interface{}, 0, len(allocations))

	for _, alloc := range allocations {
		mapping := l.findMappingByShadowID(mappings, alloc.BalanceID)
		if mapping == nil {
			continue
		}

		fundAllocation = append(fundAllocation, map[string]interface{}{
			"provider": mapping.Provider,
			"amount":   alloc.Amount,
		})
	}

	return fundAllocation
}

// getIdentityIdentifier generates a unique identifier string for an identity.
// It uses the identity's name (first/last or organization) combined with a portion of the ID.
//
// Parameters:
// - identity *model.Identity: The identity to generate an identifier for.
//
// Returns:
// - string: The generated identifier.
func (l *Blnk) getIdentityIdentifier(identity *model.Identity) string {
	var namePart string

	if identity.FirstName != "" && identity.LastName != "" {
		namePart = strings.ToLower(fmt.Sprintf("%s_%s", identity.FirstName, identity.LastName))
	} else if identity.OrganizationName != "" {
		namePart = strings.ToLower(strings.ReplaceAll(identity.OrganizationName, " ", "_"))
	} else {
		// No name available, use full ID
		return identity.IdentityID
	}

	// Use first 8 characters of ID for uniqueness
	idPart := identity.IdentityID
	if len(idPart) > 8 {
		idPart = idPart[:8]
	}

	return fmt.Sprintf("%s_%s", namePart, idPart)
}

// getLineageSources retrieves the available fund sources from shadow balances for allocation.
// Uses a single batch query to fetch all shadow balances instead of N individual queries.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - mappings []model.LineageMapping: The lineage mappings to get sources from.
//
// Returns:
// - []LineageSource: The available fund sources.
// - error: An error if the sources could not be retrieved.
