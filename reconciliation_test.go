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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/jerry-enebeli/blnk/database/mocks"
	"github.com/jerry-enebeli/blnk/model"
)

func TestOneToOneReconciliation(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()
	externalTxns := []*model.Transaction{
		{TransactionID: "ext1", Amount: 100, CreatedAt: time.Now()},
		{TransactionID: "ext2", Amount: 200, CreatedAt: time.Now()},
	}
	internalTxns := []*model.Transaction{
		{TransactionID: "int1", Amount: 100, CreatedAt: time.Now()},
		{TransactionID: "int2", Amount: 200, CreatedAt: time.Now()},
	}

	mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100000, int64(0)).Return(internalTxns, nil)
	mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100000, int64(2)).Return([]*model.Transaction{}, nil)

	matchingRules := []model.MatchingRule{
		{
			RuleID: "rule1",
			Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0},
			},
		},
	}

	matches, unmatched := blnk.oneToOneReconciliation(ctx, externalTxns, matchingRules)
	assert.Equal(t, 2, len(matches), "Expected 2 matches")
	assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")

	// Create a map to easily check for expected matches
	matchMap := make(map[string]string)
	for _, match := range matches {
		matchMap[match.ExternalTransactionID] = match.InternalTransactionID
	}

	// Check if all expected matches are present
	assert.Equal(t, "int1", matchMap["ext1"], "Expected ext1 to match with int1")
	assert.Equal(t, "int2", matchMap["ext2"], "Expected ext2 to match with int2")

	mockDS.AssertExpectations(t)
}

// TestOneToManyReconciliation tests the one-to-many reconciliation strategy
func TestOneToManyReconciliation(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()
	externalTxns := []*model.Transaction{
		{TransactionID: "ext1", Amount: 300, CreatedAt: time.Now(), Description: "External 1 Internal 1 2 3", Reference: "REF1", Currency: "USD"},
	}
	internalTxns := []*model.Transaction{
		{TransactionID: "int1", Amount: 100, CreatedAt: time.Now().Add(-1 * time.Hour), Description: "Internal 1", Reference: "REF1-A", Currency: "USD"},
		{TransactionID: "int2", Amount: 150, CreatedAt: time.Now(), Description: "Internal 2", Reference: "REF1-B", Currency: "USD"},
		{TransactionID: "int3", Amount: 53, CreatedAt: time.Now().Add(1 * time.Hour), Description: "Internal 3", Reference: "REF1-C", Currency: "USD"},
	}

	groupedInternalTxns := map[string][]*model.Transaction{
		"parent_transaction": internalTxns,
	}

	emptyGroup := map[string][]*model.Transaction{}

	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(0)).Return(groupedInternalTxns, nil)
	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(100000)).Return(emptyGroup, nil)

	matchingRules := []model.MatchingRule{
		{
			RuleID: "parent_transaction",
			Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0.01}, // 1% drift allowed
				{Field: "description", Operator: "contains"},
				{Field: "reference", Operator: "contains"},
				{Field: "currency", Operator: "equals"},
			},
		},
	}

	matches, unmatched := blnk.oneToManyReconciliation(ctx, externalTxns, "parent_transaction", matchingRules, false)

	assert.Equal(t, 3, len(matches), "Expected 3 matches")
	assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")

	assert.Equal(t, "ext1", matches[0].ExternalTransactionID)
	assert.Equal(t, "int1", matches[0].InternalTransactionID)
	assert.Equal(t, "ext1", matches[1].ExternalTransactionID)
	assert.Equal(t, "int2", matches[1].InternalTransactionID)
	assert.Equal(t, "ext1", matches[2].ExternalTransactionID)
	assert.Equal(t, "int3", matches[2].InternalTransactionID)
	mockDS.AssertExpectations(t)
}

func TestOneToManyReconciliationNoMatches(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()
	externalTxns := []*model.Transaction{
		{TransactionID: "ext1", Amount: 300, CreatedAt: time.Now(), Description: "External 1 Internal 1 2 3", Reference: "REF1", Currency: "USD"},
	}
	internalTxns := []*model.Transaction{
		{TransactionID: "int1", Amount: 100, CreatedAt: time.Now().Add(-1 * time.Hour), Description: "Internal 1", Reference: "REF1-A", Currency: "USD"},
		{TransactionID: "int2", Amount: 150, CreatedAt: time.Now(), Description: "Internal 2", Reference: "REF1-B", Currency: "USD"},
		{TransactionID: "int3", Amount: 54, CreatedAt: time.Now().Add(1 * time.Hour), Description: "Internal 3", Reference: "REF1-C", Currency: "USD"},
	}

	groupedInternalTxns := map[string][]*model.Transaction{
		"parent_transaction": internalTxns,
	}

	emptyGroup := map[string][]*model.Transaction{}

	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(0)).Return(groupedInternalTxns, nil)
	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(100000)).Return(emptyGroup, nil)

	matchingRules := []model.MatchingRule{
		{
			RuleID: "parent_transaction",
			Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0.01}, // 1% drift allowed
				{Field: "description", Operator: "contains"},
				{Field: "reference", Operator: "contains"},
				{Field: "currency", Operator: "equals"},
			},
		},
	}

	matches, unmatched := blnk.oneToManyReconciliation(ctx, externalTxns, "parent_transaction", matchingRules, false)

	assert.Equal(t, 0, len(matches), "Expected 3 matches")
	assert.Equal(t, 1, len(unmatched), "Expected 0 unmatched transactions")
	mockDS.AssertExpectations(t)
}

// func TestManyToOneReconciliation(t *testing.T) {
// 	mockDS := new(mocks.MockDataSource)
// 	blnk := &Blnk{datasource: mockDS}

// 	ctx := context.Background()
// 	internalTxns := []*model.Transaction{
// 		{TransactionID: "int1", Amount: 300, CreatedAt: time.Now(), Description: "Internal 1 External 1 2 3", Reference: "REF1", Currency: "USD"},
// 	}
// 	externalTxns := []*model.Transaction{
// 		{TransactionID: "ext1", Amount: 100, CreatedAt: time.Now().Add(-1 * time.Hour), Description: "External 1", Reference: "REF1-A", Currency: "USD"},
// 		{TransactionID: "ext2", Amount: 150, CreatedAt: time.Now(), Description: "External 2", Reference: "REF1-B", Currency: "USD"},
// 		{TransactionID: "ext3", Amount: 53, CreatedAt: time.Now().Add(1 * time.Hour), Description: "External 3", Reference: "REF1-C", Currency: "USD"},
// 	}

// 	mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100, int64(0)).Return(internalTxns, nil).Once()
// 	mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100, int64(1)).Return([]*model.Transaction{}, nil).Once()

// 	matchingRules := []model.MatchingRule{
// 		{
// 			RuleID: "2024-8-1",
// 			Criteria: []model.MatchingCriteria{
// 				{Field: "amount", Operator: "equals", AllowableDrift: 0.01}, // 1% drift allowed
// 				{Field: "description", Operator: "contains"},
// 				{Field: "reference", Operator: "contains"},
// 				{Field: "currency", Operator: "equals"},
// 			},
// 		},
// 	}

// 	matches, unmatched := blnk.groupToNReconciliation(ctx, externalTxns, "2024-8-1", matchingRules, true)

// 	assert.Equal(t, 3, len(matches), "Expected 3 matches")
// 	assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")

// 	assert.Equal(t, "int1", matches[0].InternalTransactionID)
// 	assert.Equal(t, "ext1", matches[0].ExternalTransactionID)
// 	assert.Equal(t, "int1", matches[1].InternalTransactionID)
// 	assert.Equal(t, "ext2", matches[1].ExternalTransactionID)
// 	assert.Equal(t, "int1", matches[2].InternalTransactionID)
// 	assert.Equal(t, "ext3", matches[2].ExternalTransactionID)

// 	mockDS.AssertExpectations(t)
// }

// TestMatchingRules tests the matching rules functionality
func TestMatchingRules(t *testing.T) {
	blnk := &Blnk{}

	externalTxn := &model.Transaction{
		TransactionID: "ext1",
		Amount:        100,
		CreatedAt:     time.Now(),
		Description:   "Test transaction",
		Reference:     "REF123",
		Currency:      "USD",
	}

	internalTxn := model.Transaction{
		TransactionID: "int1",
		Amount:        101,
		CreatedAt:     time.Now().Add(1 * time.Hour),
		Description:   "Internal test transaction",
		Reference:     "INT-REF123",
		Currency:      "USD",
	}

	tests := []struct {
		name          string
		matchingRules []model.MatchingRule
		expected      bool
	}{
		{
			name: "Amount matching with drift",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule1",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
					},
				},
			},
			expected: true,
		},
		{
			name: "Amount matching with drift",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule1",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0},
					},
				},
			},
			expected: false,
		},
		{
			name: "Date matching with drift",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule2",
					Criteria: []model.MatchingCriteria{
						{Field: "date", Operator: "equals", AllowableDrift: 3600},
					},
				},
			},
			expected: true,
		},
		{
			name: "Description partial matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule3",
					Criteria: []model.MatchingCriteria{
						{Field: "description", Operator: "contains"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Reference partial matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule4",
					Criteria: []model.MatchingCriteria{
						{Field: "reference", Operator: "contains"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Currency exact matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule5",
					Criteria: []model.MatchingCriteria{
						{Field: "currency", Operator: "equals"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Multiple criteria matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule6",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
						{Field: "currency", Operator: "equals"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Non-matching rule",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule7",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0},
						{Field: "description", Operator: "equals"},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.matchesRules(externalTxn, internalTxn, tt.matchingRules)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestReconciliationEdgeCases tests edge cases in the reconciliation process
func TestReconciliationEdgeCases(t *testing.T) {
	mockDS := new(mocks.MockDataSource)

	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()

	t.Run("Empty external transactions", func(t *testing.T) {
		externalTxns := []*model.Transaction{}
		matchingRules := []model.MatchingRule{
			{
				RuleID: "rule1",
				Criteria: []model.MatchingCriteria{
					{Field: "amount", Operator: "equals", AllowableDrift: 0},
				},
			},
		}

		matches, unmatched := blnk.oneToOneReconciliation(ctx, externalTxns, matchingRules)

		assert.Equal(t, 0, len(matches), "Expected 0 matches")
		assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")
	})

	t.Run("No matching internal transactions", func(t *testing.T) {
		externalTxns := []*model.Transaction{
			{TransactionID: "ext1", Amount: 100, CreatedAt: time.Now()},
		}
		internalTxns := []*model.Transaction{}

		mockDS.On("GetTransactionsPaginated", mock.Anything, "", 100000, int64(0)).Return(internalTxns, nil)

		matchingRules := []model.MatchingRule{
			{
				RuleID: "rule1",
				Criteria: []model.MatchingCriteria{
					{Field: "amount", Operator: "equals", AllowableDrift: 0},
				},
			},
		}

		matches, unmatched := blnk.oneToOneReconciliation(ctx, externalTxns, matchingRules)

		assert.Equal(t, 0, len(matches), "Expected 0 matches")
		assert.Equal(t, 1, len(unmatched), "Expected 1 unmatched transaction")

		mockDS.AssertExpectations(t)
	})

	t.Run("One-to-many with no matching group", func(t *testing.T) {
		externalTxns := []*model.Transaction{
			{TransactionID: "ext1", Amount: 300, CreatedAt: time.Now()},
		}
		groupedInternalTxns := map[string][]*model.Transaction{}

		mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(0)).Return(groupedInternalTxns, nil)

		matchingRules := []model.MatchingRule{
			{
				RuleID: "rule1",
				Criteria: []model.MatchingCriteria{
					{Field: "amount", Operator: "equals", AllowableDrift: 1},
				},
			},
		}

		matches, unmatched := blnk.oneToManyReconciliation(ctx, externalTxns, "", matchingRules, false)

		assert.Equal(t, 0, len(matches), "Expected 0 matches")
		assert.Equal(t, 0, len(unmatched), "Expected 1 unmatched transaction")

		mockDS.AssertExpectations(t)
	})
}
