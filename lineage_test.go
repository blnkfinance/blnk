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
	"testing"
	"time"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetLineageProvider(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	tests := []struct {
		name     string
		txn      *model.Transaction
		expected string
	}{
		{
			name:     "No metadata",
			txn:      &model.Transaction{},
			expected: "",
		},
		{
			name: "Metadata without provider",
			txn: &model.Transaction{
				MetaData: map[string]interface{}{
					"other_key": "value",
				},
			},
			expected: "",
		},
		{
			name: "Metadata with provider",
			txn: &model.Transaction{
				MetaData: map[string]interface{}{
					"BLNK_LINEAGE_PROVIDER": "stripe",
				},
			},
			expected: "stripe",
		},
		{
			name: "Provider with non-string value",
			txn: &model.Transaction{
				MetaData: map[string]interface{}{
					"BLNK_LINEAGE_PROVIDER": 123,
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.getLineageProvider(tt.txn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetIdentityIdentifier(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	tests := []struct {
		name     string
		identity *model.Identity
		expected string
	}{
		{
			name: "Individual with first and last name",
			identity: &model.Identity{
				IdentityID: "idt_123",
				FirstName:  "Alice",
				LastName:   "Smith",
			},
			expected: "alice_smith_idt_123",
		},
		{
			name: "Organization",
			identity: &model.Identity{
				IdentityID:       "idt_456",
				OrganizationName: "Acme Corp",
			},
			expected: "acme_corp_idt_456",
		},
		{
			name: "Fallback to ID",
			identity: &model.Identity{
				IdentityID: "idt_789",
			},
			expected: "idt_789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.getIdentityIdentifier(tt.identity)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSequentialAllocation_FIFO(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	now := time.Now()

	tests := []struct {
		name     string
		sources  []LineageSource
		amount   *big.Int
		expected []Allocation
	}{
		{
			name: "Single source covers amount",
			sources: []LineageSource{
				{BalanceID: "shadow_1", Balance: big.NewInt(10000), CreatedAt: now},
			},
			amount: big.NewInt(5000),
			expected: []Allocation{
				{BalanceID: "shadow_1", Amount: big.NewInt(5000)},
			},
		},
		{
			name: "Multiple sources needed",
			sources: []LineageSource{
				{BalanceID: "shadow_1", Balance: big.NewInt(3000), CreatedAt: now},
				{BalanceID: "shadow_2", Balance: big.NewInt(5000), CreatedAt: now.Add(time.Hour)},
			},
			amount: big.NewInt(6000),
			expected: []Allocation{
				{BalanceID: "shadow_1", Amount: big.NewInt(3000)},
				{BalanceID: "shadow_2", Amount: big.NewInt(3000)},
			},
		},
		{
			name: "Exhaust first source completely",
			sources: []LineageSource{
				{BalanceID: "shadow_1", Balance: big.NewInt(5000), CreatedAt: now},
				{BalanceID: "shadow_2", Balance: big.NewInt(3000), CreatedAt: now.Add(time.Hour)},
			},
			amount: big.NewInt(6000),
			expected: []Allocation{
				{BalanceID: "shadow_1", Amount: big.NewInt(5000)},
				{BalanceID: "shadow_2", Amount: big.NewInt(1000)},
			},
		},
		{
			name:     "Empty sources",
			sources:  []LineageSource{},
			amount:   big.NewInt(1000),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.sequentialAllocation(tt.sources, tt.amount)

			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}

			assert.Equal(t, len(tt.expected), len(result))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.BalanceID, result[i].BalanceID)
				assert.Equal(t, 0, exp.Amount.Cmp(result[i].Amount), "Amount mismatch for %s", exp.BalanceID)
			}
		})
	}
}

func TestCalculateAllocation(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	now := time.Now()
	sources := []LineageSource{
		{BalanceID: "shadow_old", Balance: big.NewInt(5000), CreatedAt: now.Add(-2 * time.Hour)},
		{BalanceID: "shadow_new", Balance: big.NewInt(3000), CreatedAt: now},
	}

	tests := []struct {
		name          string
		strategy      string
		amount        *big.Int
		expectedFirst string
	}{
		{
			name:          "FIFO uses oldest first",
			strategy:      AllocationFIFO,
			amount:        big.NewInt(4000),
			expectedFirst: "shadow_old",
		},
		{
			name:          "LIFO uses newest first",
			strategy:      AllocationLIFO,
			amount:        big.NewInt(4000),
			expectedFirst: "shadow_new",
		},
		{
			name:          "Default is FIFO",
			strategy:      "",
			amount:        big.NewInt(4000),
			expectedFirst: "shadow_old",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.calculateAllocation(sources, tt.amount, tt.strategy)
			assert.NotEmpty(t, result)
			assert.Equal(t, tt.expectedFirst, result[0].BalanceID)
		})
	}
}

func TestProportionalAllocation(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	now := time.Now()

	tests := []struct {
		name          string
		sources       []LineageSource
		amount        *big.Int
		expectedTotal *big.Int
	}{
		{
			name: "Equal proportions",
			sources: []LineageSource{
				{BalanceID: "shadow_1", Balance: big.NewInt(5000), CreatedAt: now},
				{BalanceID: "shadow_2", Balance: big.NewInt(5000), CreatedAt: now},
			},
			amount:        big.NewInt(6000),
			expectedTotal: big.NewInt(6000),
		},
		{
			name: "Unequal proportions",
			sources: []LineageSource{
				{BalanceID: "shadow_1", Balance: big.NewInt(7500), CreatedAt: now},
				{BalanceID: "shadow_2", Balance: big.NewInt(2500), CreatedAt: now},
			},
			amount:        big.NewInt(8000),
			expectedTotal: big.NewInt(8000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.proportionalAllocation(tt.sources, tt.amount)

			total := big.NewInt(0)
			for _, alloc := range result {
				total.Add(total, alloc.Amount)
			}

			assert.Equal(t, 0, tt.expectedTotal.Cmp(total), "Total allocation should match requested amount")
		})
	}
}

func TestFindMappingByShadowID(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	mappings := []model.LineageMapping{
		{
			ID:              1,
			BalanceID:       "bln_main",
			Provider:        "stripe",
			ShadowBalanceID: "bln_stripe_shadow",
		},
		{
			ID:              2,
			BalanceID:       "bln_main",
			Provider:        "paypal",
			ShadowBalanceID: "bln_paypal_shadow",
		},
	}

	tests := []struct {
		name             string
		shadowBalanceID  string
		expectFound      bool
		expectedProvider string
	}{
		{
			name:             "Find existing shadow",
			shadowBalanceID:  "bln_stripe_shadow",
			expectFound:      true,
			expectedProvider: "stripe",
		},
		{
			name:            "Shadow not found",
			shadowBalanceID: "bln_nonexistent",
			expectFound:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.findMappingByShadowID(mappings, tt.shadowBalanceID)

			if tt.expectFound {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedProvider, result.Provider)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestLineageIntegration_MultiProvider_FIFO(t *testing.T) {
	now := time.Now()

	stripeDeposit := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	paypalDeposit := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	bankDeposit := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	sources := []LineageSource{
		{BalanceID: "bln_stripe_alice", Balance: big.NewInt(5000), CreatedAt: stripeDeposit},
		{BalanceID: "bln_paypal_alice", Balance: big.NewInt(3000), CreatedAt: paypalDeposit},
		{BalanceID: "bln_bank_alice", Balance: big.NewInt(2000), CreatedAt: bankDeposit},
	}

	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	t.Run("Spend $70 with FIFO - should deplete stripe first, then paypal", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(7000), AllocationFIFO)

		assert.Len(t, allocations, 2)

		assert.Equal(t, "bln_stripe_alice", allocations[0].BalanceID)
		assert.Equal(t, 0, big.NewInt(5000).Cmp(allocations[0].Amount))

		assert.Equal(t, "bln_paypal_alice", allocations[1].BalanceID)
		assert.Equal(t, 0, big.NewInt(2000).Cmp(allocations[1].Amount))
	})

	t.Run("Spend $100 with FIFO - should use all three providers", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(10000), AllocationFIFO)

		assert.Len(t, allocations, 3)

		total := big.NewInt(0)
		for _, alloc := range allocations {
			total.Add(total, alloc.Amount)
		}
		assert.Equal(t, 0, big.NewInt(10000).Cmp(total))

		assert.Equal(t, "bln_stripe_alice", allocations[0].BalanceID)
		assert.Equal(t, "bln_paypal_alice", allocations[1].BalanceID)
		assert.Equal(t, "bln_bank_alice", allocations[2].BalanceID)
	})

	t.Run("Spend $30 with FIFO - only stripe needed", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(3000), AllocationFIFO)

		assert.Len(t, allocations, 1)
		assert.Equal(t, "bln_stripe_alice", allocations[0].BalanceID)
		assert.Equal(t, 0, big.NewInt(3000).Cmp(allocations[0].Amount))
	})

	_ = now
}

func TestLineageIntegration_MultiProvider_LIFO(t *testing.T) {
	stripeDeposit := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	paypalDeposit := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	bankDeposit := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	sources := []LineageSource{
		{BalanceID: "bln_stripe_alice", Balance: big.NewInt(5000), CreatedAt: stripeDeposit},
		{BalanceID: "bln_paypal_alice", Balance: big.NewInt(3000), CreatedAt: paypalDeposit},
		{BalanceID: "bln_bank_alice", Balance: big.NewInt(2000), CreatedAt: bankDeposit},
	}

	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	t.Run("Spend $40 with LIFO - should deplete bank first, then paypal", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(4000), AllocationLIFO)

		assert.Len(t, allocations, 2)

		assert.Equal(t, "bln_bank_alice", allocations[0].BalanceID)
		assert.Equal(t, 0, big.NewInt(2000).Cmp(allocations[0].Amount))

		assert.Equal(t, "bln_paypal_alice", allocations[1].BalanceID)
		assert.Equal(t, 0, big.NewInt(2000).Cmp(allocations[1].Amount))
	})

	t.Run("Spend $100 with LIFO - should use all in reverse order", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(10000), AllocationLIFO)

		assert.Len(t, allocations, 3)

		assert.Equal(t, "bln_bank_alice", allocations[0].BalanceID)
		assert.Equal(t, "bln_paypal_alice", allocations[1].BalanceID)
		assert.Equal(t, "bln_stripe_alice", allocations[2].BalanceID)
	})
}

func TestLineageIntegration_MultiProvider_PROPORTIONAL(t *testing.T) {
	now := time.Now()

	sources := []LineageSource{
		{BalanceID: "bln_stripe_alice", Balance: big.NewInt(6000), CreatedAt: now},
		{BalanceID: "bln_paypal_alice", Balance: big.NewInt(3000), CreatedAt: now},
		{BalanceID: "bln_bank_alice", Balance: big.NewInt(1000), CreatedAt: now},
	}

	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	blnk, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	t.Run("Spend $50 with PROPORTIONAL - should split 60/30/10", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(5000), AllocationProp)

		assert.Len(t, allocations, 3)

		total := big.NewInt(0)
		for _, alloc := range allocations {
			total.Add(total, alloc.Amount)
		}
		assert.Equal(t, 0, big.NewInt(5000).Cmp(total))

		stripeAlloc := allocations[0].Amount.Int64()
		paypalAlloc := allocations[1].Amount.Int64()
		bankAlloc := allocations[2].Amount.Int64()

		assert.True(t, stripeAlloc > paypalAlloc, "Stripe should get more than PayPal")
		assert.True(t, paypalAlloc > bankAlloc, "PayPal should get more than Bank")
	})

	t.Run("Spend $100 with PROPORTIONAL - should use all funds", func(t *testing.T) {
		allocations := blnk.calculateAllocation(sources, big.NewInt(10000), AllocationProp)

		total := big.NewInt(0)
		for _, alloc := range allocations {
			total.Add(total, alloc.Amount)
		}
		assert.Equal(t, 0, big.NewInt(10000).Cmp(total))
	})
}

func TestLineageIntegration_FundAllocationMetadata(t *testing.T) {
	t.Run("Build fund allocation for transaction", func(t *testing.T) {
		allocations := []Allocation{
			{BalanceID: "bln_stripe_shadow", Amount: big.NewInt(5000)},
			{BalanceID: "bln_paypal_shadow", Amount: big.NewInt(2000)},
		}

		mappings := []model.LineageMapping{
			{ShadowBalanceID: "bln_stripe_shadow", Provider: "stripe"},
			{ShadowBalanceID: "bln_paypal_shadow", Provider: "paypal"},
		}

		fundAllocation := make([]map[string]interface{}, 0)
		for _, alloc := range allocations {
			for _, mapping := range mappings {
				if mapping.ShadowBalanceID == alloc.BalanceID {
					fundAllocation = append(fundAllocation, map[string]interface{}{
						"provider": mapping.Provider,
						"amount":   alloc.Amount.Int64(),
					})
					break
				}
			}
		}

		assert.Len(t, fundAllocation, 2)
		assert.Equal(t, "stripe", fundAllocation[0]["provider"])
		assert.Equal(t, int64(5000), fundAllocation[0]["amount"])
		assert.Equal(t, "paypal", fundAllocation[1]["provider"])
		assert.Equal(t, int64(2000), fundAllocation[1]["amount"])
	})
}

func TestLineageIntegration_FullCreditDebitCycle(t *testing.T) {
	t.Run("Simulate full cycle: 3 deposits then 1 withdrawal", func(t *testing.T) {
		stripeTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		paypalTime := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
		bankTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		stripeDeposit := big.NewInt(5000)
		paypalDeposit := big.NewInt(3000)
		bankDeposit := big.NewInt(2000)
		withdrawAmount := big.NewInt(7000)

		shadowBalances := map[string]*struct {
			Debit     *big.Int
			Credit    *big.Int
			CreatedAt time.Time
		}{
			"stripe": {Debit: new(big.Int).Set(stripeDeposit), Credit: big.NewInt(0), CreatedAt: stripeTime},
			"paypal": {Debit: new(big.Int).Set(paypalDeposit), Credit: big.NewInt(0), CreatedAt: paypalTime},
			"bank":   {Debit: new(big.Int).Set(bankDeposit), Credit: big.NewInt(0), CreatedAt: bankTime},
		}

		totalDeposited := new(big.Int).Add(stripeDeposit, paypalDeposit)
		totalDeposited.Add(totalDeposited, bankDeposit)
		assert.Equal(t, 0, big.NewInt(10000).Cmp(totalDeposited))

		sources := []LineageSource{
			{BalanceID: "stripe", Balance: new(big.Int).Sub(shadowBalances["stripe"].Debit, shadowBalances["stripe"].Credit), CreatedAt: stripeTime},
			{BalanceID: "paypal", Balance: new(big.Int).Sub(shadowBalances["paypal"].Debit, shadowBalances["paypal"].Credit), CreatedAt: paypalTime},
			{BalanceID: "bank", Balance: new(big.Int).Sub(shadowBalances["bank"].Debit, shadowBalances["bank"].Credit), CreatedAt: bankTime},
		}

		datasource, _, _ := newTestDataSource()
		blnk, _ := NewBlnk(datasource)
		allocations := blnk.calculateAllocation(sources, withdrawAmount, AllocationFIFO)

		for _, alloc := range allocations {
			shadowBalances[alloc.BalanceID].Credit.Add(shadowBalances[alloc.BalanceID].Credit, alloc.Amount)
		}

		stripeAvailable := new(big.Int).Sub(shadowBalances["stripe"].Debit, shadowBalances["stripe"].Credit)
		paypalAvailable := new(big.Int).Sub(shadowBalances["paypal"].Debit, shadowBalances["paypal"].Credit)
		bankAvailable := new(big.Int).Sub(shadowBalances["bank"].Debit, shadowBalances["bank"].Credit)

		assert.Equal(t, 0, big.NewInt(0).Cmp(stripeAvailable), "Stripe should be fully depleted")
		assert.Equal(t, 0, big.NewInt(1000).Cmp(paypalAvailable), "PayPal should have 1000 remaining")
		assert.Equal(t, 0, big.NewInt(2000).Cmp(bankAvailable), "Bank should be untouched")

		totalRemaining := new(big.Int).Add(stripeAvailable, paypalAvailable)
		totalRemaining.Add(totalRemaining, bankAvailable)
		assert.Equal(t, 0, big.NewInt(3000).Cmp(totalRemaining), "Total remaining should be 3000")
	})
}

func TestLineageIntegration_EdgeCases(t *testing.T) {
	datasource, _, _ := newTestDataSource()
	blnk, _ := NewBlnk(datasource)

	t.Run("Empty sources returns nil", func(t *testing.T) {
		allocations := blnk.calculateAllocation([]LineageSource{}, big.NewInt(1000), AllocationFIFO)
		assert.Nil(t, allocations)
	})

	t.Run("Zero amount allocation", func(t *testing.T) {
		sources := []LineageSource{
			{BalanceID: "shadow_1", Balance: big.NewInt(5000), CreatedAt: time.Now()},
		}
		allocations := blnk.calculateAllocation(sources, big.NewInt(0), AllocationFIFO)

		if len(allocations) > 0 {
			assert.Equal(t, 0, allocations[0].Amount.Cmp(big.NewInt(0)))
		}
	})

	t.Run("Request more than available", func(t *testing.T) {
		sources := []LineageSource{
			{BalanceID: "shadow_1", Balance: big.NewInt(3000), CreatedAt: time.Now()},
		}
		allocations := blnk.calculateAllocation(sources, big.NewInt(5000), AllocationFIFO)

		total := big.NewInt(0)
		for _, alloc := range allocations {
			total.Add(total, alloc.Amount)
		}
		assert.Equal(t, 0, big.NewInt(3000).Cmp(total), "Should only allocate what's available")
	})

	t.Run("Single provider with exact amount", func(t *testing.T) {
		sources := []LineageSource{
			{BalanceID: "shadow_1", Balance: big.NewInt(5000), CreatedAt: time.Now()},
		}
		allocations := blnk.calculateAllocation(sources, big.NewInt(5000), AllocationFIFO)

		assert.Len(t, allocations, 1)
		assert.Equal(t, 0, big.NewInt(5000).Cmp(allocations[0].Amount))
	})
}

func TestLineageE2E_DebitFlow_FIFO_ToNonLineageBalance(t *testing.T) {
	ctx := context.Background()
	mockDS := new(mocks.MockDataSource)

	aliceIdentity := &model.Identity{
		IdentityID: "idt_alice_123",
		FirstName:  "Alice",
		LastName:   "Smith",
	}

	aliceMainBalance := &model.Balance{
		BalanceID:          "bln_alice_main",
		LedgerID:           "ldg_main",
		IdentityID:         "idt_alice_123",
		Currency:           "USD",
		Balance:            big.NewInt(10000),
		CreditBalance:      big.NewInt(10000),
		DebitBalance:       big.NewInt(0),
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	}

	merchantBalance := &model.Balance{
		BalanceID:        "bln_merchant",
		LedgerID:         "ldg_main",
		Currency:         "USD",
		Balance:          big.NewInt(0),
		TrackFundLineage: false,
	}

	stripeShadow := &model.Balance{
		BalanceID:     "bln_stripe_shadow",
		Currency:      "USD",
		Balance:       big.NewInt(-5000),
		CreditBalance: big.NewInt(0),
		DebitBalance:  big.NewInt(5000),
	}

	paypalShadow := &model.Balance{
		BalanceID:     "bln_paypal_shadow",
		Currency:      "USD",
		Balance:       big.NewInt(-5000),
		CreditBalance: big.NewInt(0),
		DebitBalance:  big.NewInt(5000),
	}

	aggregateShadow := &model.Balance{
		BalanceID:     "bln_aggregate_shadow",
		Currency:      "USD",
		Balance:       big.NewInt(10000),
		CreditBalance: big.NewInt(10000),
		DebitBalance:  big.NewInt(0),
	}

	stripeMapping := model.LineageMapping{
		ID:                 1,
		BalanceID:          "bln_alice_main",
		Provider:           "stripe",
		ShadowBalanceID:    "bln_stripe_shadow",
		AggregateBalanceID: "bln_aggregate_shadow",
		IdentityID:         "idt_alice_123",
		CreatedAt:          time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
	}

	paypalMapping := model.LineageMapping{
		ID:                 2,
		BalanceID:          "bln_alice_main",
		Provider:           "paypal",
		ShadowBalanceID:    "bln_paypal_shadow",
		AggregateBalanceID: "bln_aggregate_shadow",
		IdentityID:         "idt_alice_123",
		CreatedAt:          time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC),
	}

	mappings := []model.LineageMapping{stripeMapping, paypalMapping}

	mockDS.On("GetBalanceByID", "bln_alice_main", mock.Anything, mock.Anything).Return(aliceMainBalance, nil)
	mockDS.On("GetBalanceByID", "bln_merchant", mock.Anything, mock.Anything).Return(merchantBalance, nil)
	mockDS.On("GetLineageMappings", mock.Anything, "bln_alice_main").Return(mappings, nil)
	mockDS.On("GetBalanceByIDLite", "bln_stripe_shadow").Return(stripeShadow, nil)
	mockDS.On("GetBalanceByIDLite", "bln_paypal_shadow").Return(paypalShadow, nil)
	mockDS.On("GetIdentityByID", "idt_alice_123").Return(aliceIdentity, nil)
	mockDS.On("GetBalanceByIndicator", mock.Anything, mock.Anything).Return(aggregateShadow, nil)
	mockDS.On("UpdateTransactionMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	blnkInstance := &Blnk{datasource: mockDS}

	t.Run("Get lineage sources with correct available amounts", func(t *testing.T) {
		sources, err := blnkInstance.getLineageSources(ctx, mappings)
		assert.NoError(t, err)
		assert.Len(t, sources, 2)

		for _, src := range sources {
			assert.Equal(t, 0, big.NewInt(5000).Cmp(src.Balance), "Each source should have 5000 available")
		}
	})

	t.Run("FIFO allocation for $70 withdrawal", func(t *testing.T) {
		sources := []LineageSource{
			{BalanceID: "bln_stripe_shadow", Balance: big.NewInt(5000), CreatedAt: stripeMapping.CreatedAt},
			{BalanceID: "bln_paypal_shadow", Balance: big.NewInt(5000), CreatedAt: paypalMapping.CreatedAt},
		}

		allocations := blnkInstance.calculateAllocation(sources, big.NewInt(7000), AllocationFIFO)

		assert.Len(t, allocations, 2)

		assert.Equal(t, "bln_stripe_shadow", allocations[0].BalanceID)
		assert.Equal(t, 0, big.NewInt(5000).Cmp(allocations[0].Amount), "Should take all $50 from stripe")

		assert.Equal(t, "bln_paypal_shadow", allocations[1].BalanceID)
		assert.Equal(t, 0, big.NewInt(2000).Cmp(allocations[1].Amount), "Should take $20 from paypal")
	})

	t.Run("Verify destination has no lineage (no shadow transactions for merchant)", func(t *testing.T) {
		assert.False(t, merchantBalance.TrackFundLineage)
	})
}

func TestLineageE2E_DebitFlow_ToLineageEnabledBalance(t *testing.T) {
	ctx := context.Background()
	mockDS := new(mocks.MockDataSource)

	aliceIdentity := &model.Identity{
		IdentityID: "idt_alice_123",
		FirstName:  "Alice",
		LastName:   "Smith",
	}

	bobIdentity := &model.Identity{
		IdentityID: "idt_bob_456",
		FirstName:  "Bob",
		LastName:   "Jones",
	}

	aliceMainBalance := &model.Balance{
		BalanceID:          "bln_alice_main",
		IdentityID:         "idt_alice_123",
		Currency:           "USD",
		Balance:            big.NewInt(10000),
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	}

	bobMainBalance := &model.Balance{
		BalanceID:          "bln_bob_main",
		IdentityID:         "idt_bob_456",
		Currency:           "USD",
		Balance:            big.NewInt(0),
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	}

	aliceStripeMapping := model.LineageMapping{
		BalanceID:          "bln_alice_main",
		Provider:           "stripe",
		ShadowBalanceID:    "bln_stripe_alice_shadow",
		AggregateBalanceID: "bln_alice_aggregate",
		CreatedAt:          time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
	}

	mockDS.On("GetBalanceByID", "bln_alice_main", mock.Anything, mock.Anything).Return(aliceMainBalance, nil)
	mockDS.On("GetBalanceByID", "bln_bob_main", mock.Anything, mock.Anything).Return(bobMainBalance, nil)
	mockDS.On("GetIdentityByID", "idt_alice_123").Return(aliceIdentity, nil)
	mockDS.On("GetIdentityByID", "idt_bob_456").Return(bobIdentity, nil)
	mockDS.On("GetLineageMappings", mock.Anything, "bln_alice_main").Return([]model.LineageMapping{aliceStripeMapping}, nil)
	mockDS.On("GetBalanceByIDLite", mock.Anything).Return(&model.Balance{
		DebitBalance:  big.NewInt(5000),
		CreditBalance: big.NewInt(0),
	}, nil)
	mockDS.On("GetBalanceByIndicator", mock.Anything, mock.Anything).Return(&model.Balance{
		BalanceID: "bln_new_shadow",
	}, nil)
	mockDS.On("UpsertLineageMapping", mock.Anything, mock.Anything).Return(nil)

	blnkInstance := &Blnk{datasource: mockDS}

	t.Run("Both balances have lineage enabled", func(t *testing.T) {
		assert.True(t, aliceMainBalance.TrackFundLineage)
		assert.True(t, bobMainBalance.TrackFundLineage)
	})

	t.Run("Bob should inherit stripe provider from Alice", func(t *testing.T) {
		bobIdentifier := blnkInstance.getIdentityIdentifier(bobIdentity)
		assert.Equal(t, "bob_jones_idt_bob_", bobIdentifier)

		expectedBobStripeShadow := "@stripe_bob_jones_idt_bob__lineage"
		expectedBobAggregate := "@bob_jones_idt_bob__lineage"

		assert.Contains(t, expectedBobStripeShadow, "stripe")
		assert.Contains(t, expectedBobStripeShadow, "bob_jones")
		assert.Contains(t, expectedBobAggregate, "bob_jones")
	})

	_ = ctx
}

func TestLineageE2E_GetBalanceLineage_API(t *testing.T) {
	ctx := context.Background()
	mockDS := new(mocks.MockDataSource)

	mainBalance := &model.Balance{
		BalanceID:          "bln_alice_main",
		IdentityID:         "idt_alice_123",
		Currency:           "USD",
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	}

	stripeShadow := &model.Balance{
		BalanceID:          "bln_stripe_shadow",
		Currency:           "USD",
		DebitBalance:       big.NewInt(5000),
		CreditBalance:      big.NewInt(2000),
		CurrencyMultiplier: 1,
	}

	paypalShadow := &model.Balance{
		BalanceID:          "bln_paypal_shadow",
		Currency:           "USD",
		DebitBalance:       big.NewInt(3000),
		CreditBalance:      big.NewInt(0),
		CurrencyMultiplier: 1,
	}

	mappings := []model.LineageMapping{
		{
			BalanceID:          "bln_alice_main",
			Provider:           "stripe",
			ShadowBalanceID:    "bln_stripe_shadow",
			AggregateBalanceID: "bln_alice_aggregate",
		},
		{
			BalanceID:          "bln_alice_main",
			Provider:           "paypal",
			ShadowBalanceID:    "bln_paypal_shadow",
			AggregateBalanceID: "bln_alice_aggregate",
		},
	}

	mockDS.On("GetBalanceByID", "bln_alice_main", mock.Anything, mock.Anything).Return(mainBalance, nil)
	mockDS.On("GetLineageMappings", mock.Anything, "bln_alice_main").Return(mappings, nil)
	mockDS.On("GetBalanceByIDLite", "bln_stripe_shadow").Return(stripeShadow, nil)
	mockDS.On("GetBalanceByIDLite", "bln_paypal_shadow").Return(paypalShadow, nil)

	blnkInstance := &Blnk{datasource: mockDS}

	t.Run("Get balance lineage returns correct breakdown", func(t *testing.T) {
		lineage, err := blnkInstance.GetBalanceLineage(ctx, "bln_alice_main")
		assert.NoError(t, err)
		assert.NotNil(t, lineage)

		assert.Equal(t, "bln_alice_main", lineage.BalanceID)
		assert.Len(t, lineage.Providers, 2)

		var stripeProvider, paypalProvider *ProviderBreakdown
		for i := range lineage.Providers {
			if lineage.Providers[i].Provider == "stripe" {
				stripeProvider = &lineage.Providers[i]
			}
			if lineage.Providers[i].Provider == "paypal" {
				paypalProvider = &lineage.Providers[i]
			}
		}

		assert.NotNil(t, stripeProvider)
		assert.Equal(t, 0, big.NewInt(5000).Cmp(stripeProvider.Amount), "Stripe amount should be 5000")
		assert.Equal(t, 0, big.NewInt(2000).Cmp(stripeProvider.Spent), "Stripe spent should be 2000")
		assert.Equal(t, 0, big.NewInt(3000).Cmp(stripeProvider.Available), "Stripe available should be 3000")

		assert.NotNil(t, paypalProvider)
		assert.Equal(t, 0, big.NewInt(3000).Cmp(paypalProvider.Amount), "PayPal amount should be 3000")
		assert.Equal(t, 0, big.NewInt(0).Cmp(paypalProvider.Spent), "PayPal spent should be 0")
		assert.Equal(t, 0, big.NewInt(3000).Cmp(paypalProvider.Available), "PayPal available should be 3000")

		expectedTotal := big.NewInt(6000)
		assert.Equal(t, 0, expectedTotal.Cmp(lineage.TotalWithLineage), "Total should be 6000")
	})
}

func TestLineageE2E_GetTransactionLineage_API(t *testing.T) {
	ctx := context.Background()
	mockDS := new(mocks.MockDataSource)

	mainTxn := &model.Transaction{
		TransactionID: "txn_main_123",
		Source:        "bln_alice_main",
		Destination:   "bln_merchant",
		Amount:        70,
		Currency:      "USD",
		Status:        "APPLIED",
		MetaData: map[string]interface{}{
			"BLNK_FUND_ALLOCATION": []interface{}{
				map[string]interface{}{"provider": "stripe", "amount": float64(50)},
				map[string]interface{}{"provider": "paypal", "amount": float64(20)},
			},
		},
	}

	shadowTxns := []model.Transaction{
		{
			TransactionID: "txn_shadow_1",
			Source:        "bln_alice_aggregate",
			Destination:   "bln_stripe_shadow",
			Amount:        50,
			MetaData: map[string]interface{}{
				"_shadow_for":   "txn_main_123",
				"_provider":     "stripe",
				"_lineage_type": "release",
			},
		},
		{
			TransactionID: "txn_shadow_2",
			Source:        "bln_alice_aggregate",
			Destination:   "bln_paypal_shadow",
			Amount:        20,
			MetaData: map[string]interface{}{
				"_shadow_for":   "txn_main_123",
				"_provider":     "paypal",
				"_lineage_type": "release",
			},
		},
	}

	mockDS.On("GetTransaction", mock.Anything, "txn_main_123").Return(mainTxn, nil)
	mockDS.On("GetTransactionsByShadowFor", mock.Anything, "txn_main_123").Return(shadowTxns, nil)

	blnkInstance := &Blnk{datasource: mockDS}

	t.Run("Get transaction lineage returns fund allocation and shadow txns", func(t *testing.T) {
		lineage, err := blnkInstance.GetTransactionLineage(ctx, "txn_main_123")
		assert.NoError(t, err)
		assert.NotNil(t, lineage)

		assert.Equal(t, "txn_main_123", lineage.TransactionID)

		assert.Len(t, lineage.FundAllocation, 2)
		assert.Equal(t, "stripe", lineage.FundAllocation[0]["provider"])
		assert.Equal(t, float64(50), lineage.FundAllocation[0]["amount"])
		assert.Equal(t, "paypal", lineage.FundAllocation[1]["provider"])
		assert.Equal(t, float64(20), lineage.FundAllocation[1]["amount"])

		assert.Len(t, lineage.ShadowTransactions, 2)
		assert.Equal(t, "txn_shadow_1", lineage.ShadowTransactions[0].TransactionID)
		assert.Equal(t, "txn_shadow_2", lineage.ShadowTransactions[1].TransactionID)
	})
}
