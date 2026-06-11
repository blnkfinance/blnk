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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
)

var reconBaseTime = time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

// --- amount matching ---

func TestRecon_MatchesGroupAmount_EqualsBoundaries(t *testing.T) {
	b := &Blnk{}

	// AllowableDrift is a FRACTION of the internal amount (0.25 => 25%); the
	// validator and SQL prefilter use the same unit. These cases pin the
	// exact <= boundary.
	tests := []struct {
		name     string
		external float64
		group    float64
		drift    float64
		want     bool
	}{
		{"exact match, zero drift", 100, 100, 0, true},
		{"one cent over, zero drift", 100.01, 100, 0, false},
		{"one cent under, zero drift", 99.99, 100, 0, false},
		{"exactly at upper drift boundary", 150, 100, 0.5, true},
		{"exactly at lower drift boundary", 50, 100, 0.5, true},
		{"just past upper drift boundary", 150.0001, 100, 0.5, false},
		{"just past lower drift boundary", 49.9999, 100, 0.5, false},
		{"boundary with quarter drift", 250, 200, 0.25, true},
		{"just past quarter drift", 250.001, 200, 0.25, false},
		{"zero amounts, zero drift", 0, 0, 0, true},
		{"zero group amount kills all drift", 5, 0, 0.5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			criteria := model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: tt.drift}
			assert.Equal(t, tt.want, b.matchesGroupAmount(tt.external, tt.group, criteria))
		})
	}
}

func TestRecon_MatchesGroupAmount_GreaterAndLessThanAreStrict(t *testing.T) {
	b := &Blnk{}

	gt := model.MatchingCriteria{Field: "amount", Operator: "greater_than"}
	assert.True(t, b.matchesGroupAmount(100.01, 100, gt))
	assert.False(t, b.matchesGroupAmount(100, 100, gt), "greater_than must be strict at the boundary")
	assert.False(t, b.matchesGroupAmount(99.99, 100, gt))

	lt := model.MatchingCriteria{Field: "amount", Operator: "less_than"}
	assert.True(t, b.matchesGroupAmount(99.99, 100, lt))
	assert.False(t, b.matchesGroupAmount(100, 100, lt), "less_than must be strict at the boundary")
	assert.False(t, b.matchesGroupAmount(100.01, 100, lt))
}

func TestRecon_MatchesGroupAmount_UnknownOperatorNeverMatches(t *testing.T) {
	b := &Blnk{}
	// "contains" passes rule validation for the amount field but has no
	// implementation here: it must never match (fail closed).
	assert.False(t, b.matchesGroupAmount(100, 100, model.MatchingCriteria{Field: "amount", Operator: "contains"}))
	assert.False(t, b.matchesGroupAmount(100, 100, model.MatchingCriteria{Field: "amount", Operator: "bogus"}))
}

func TestRecon_AmountDriftFractionSemantics(t *testing.T) {
	// Regression: drift units used to disagree — the validator/SQL prefilter
	// treated AllowableDrift as a percentage while the matcher treated it as
	// a fraction, a 100x mismatch. The canonical unit is now a FRACTION
	// (0.01 = 1%) in all three places.
	b := &Blnk{}
	criteria := model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0.01} // 1%
	assert.False(t, b.matchesGroupAmount(150, 100, criteria), "a 50%% deviation must not satisfy a 1%% drift rule")
	assert.True(t, b.matchesGroupAmount(100.5, 100, criteria), "a 0.5%% deviation must satisfy a 1%% drift rule")
	assert.True(t, b.matchesGroupAmount(101, 100, criteria), "exactly 1%% deviation must satisfy a 1%% drift rule")
	assert.False(t, b.matchesGroupAmount(101.001, 100, criteria), "just past 1%% must not match")
}

func TestRecon_NegativeAmountsMatchWithinDrift(t *testing.T) {
	// Regression: the matcher used to compute groupAmount*AllowableDrift
	// without abs(), so negative internal amounts (refunds, debits) produced
	// a negative tolerance and could never reconcile.
	b := &Blnk{}
	criteria := model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0.01}
	assert.True(t, b.matchesGroupAmount(-100, -100, criteria), "identical negative amounts must match")
	assert.True(t, b.matchesGroupAmount(-100.5, -100, criteria), "negative amounts within drift must match")
	assert.False(t, b.matchesGroupAmount(-150, -100, criteria), "negative amounts past drift must not match")
}

// --- date matching ---

func TestRecon_MatchesGroupDate_EqualsBoundaries(t *testing.T) {
	b := &Blnk{}

	tests := []struct {
		name     string
		external time.Time
		drift    float64 // seconds
		want     bool
	}{
		{"same instant, zero drift", reconBaseTime, 0, true},
		{"one second late, zero drift", reconBaseTime.Add(time.Second), 0, false},
		{"one second early, zero drift", reconBaseTime.Add(-time.Second), 0, false},
		{"exactly at +drift boundary", reconBaseTime.Add(60 * time.Second), 60, true},
		{"exactly at -drift boundary", reconBaseTime.Add(-60 * time.Second), 60, true},
		{"one second past +drift", reconBaseTime.Add(61 * time.Second), 60, false},
		{"one second past -drift", reconBaseTime.Add(-61 * time.Second), 60, false},
		{"one day drift covers 24h", reconBaseTime.Add(24 * time.Hour), 86400, true},
		{"one day drift, one second over", reconBaseTime.Add(24*time.Hour + time.Second), 86400, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			criteria := model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: tt.drift}
			assert.Equal(t, tt.want, b.matchesGroupDate(tt.external, reconBaseTime, criteria))
		})
	}
}

func TestRecon_MatchesGroupDate_AfterBefore(t *testing.T) {
	b := &Blnk{}

	after := model.MatchingCriteria{Field: "date", Operator: "after"}
	assert.True(t, b.matchesGroupDate(reconBaseTime.Add(time.Nanosecond), reconBaseTime, after))
	assert.False(t, b.matchesGroupDate(reconBaseTime, reconBaseTime, after), "after must be strict")
	assert.False(t, b.matchesGroupDate(reconBaseTime.Add(-time.Nanosecond), reconBaseTime, after))

	before := model.MatchingCriteria{Field: "date", Operator: "before"}
	assert.True(t, b.matchesGroupDate(reconBaseTime.Add(-time.Nanosecond), reconBaseTime, before))
	assert.False(t, b.matchesGroupDate(reconBaseTime, reconBaseTime, before), "before must be strict")

	assert.False(t, b.matchesGroupDate(reconBaseTime, reconBaseTime, model.MatchingCriteria{Field: "date", Operator: "bogus"}))
}

func TestRecon_DateDriftComparesFractionalSeconds(t *testing.T) {
	// Regression: integer Duration division used to truncate the sub-second
	// remainder, so a 60.9s difference passed a 60s drift rule.
	b := &Blnk{}

	drift60 := model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: 60}
	assert.False(t, b.matchesGroupDate(reconBaseTime.Add(60*time.Second+900*time.Millisecond), reconBaseTime, drift60),
		"60.9s difference must not satisfy a 60s drift")

	exact := model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: 0}
	assert.False(t, b.matchesGroupDate(reconBaseTime.Add(990*time.Millisecond), reconBaseTime, exact),
		"0.99s difference must not satisfy an exact (0s drift) rule")
}

func TestRecon_DateOperatorsAlignedWithValidation(t *testing.T) {
	// Regression: the validator accepts greater_than/less_than but the
	// matcher only implemented after/before, so every validated date
	// comparison rule silently evaluated to false. greater_than/less_than
	// now alias after/before.
	b := &Blnk{}

	// greater_than is accepted by validation for the date field...
	require.NoError(t, b.validateCriteria(model.MatchingCriteria{Field: "date", Operator: "greater_than"}))
	// ...so it must behave like a date comparison instead of always returning false.
	assert.True(t, b.matchesGroupDate(reconBaseTime.Add(time.Hour), reconBaseTime,
		model.MatchingCriteria{Field: "date", Operator: "greater_than"}))
	assert.True(t, b.matchesGroupDate(reconBaseTime.Add(-time.Hour), reconBaseTime,
		model.MatchingCriteria{Field: "date", Operator: "less_than"}))
}

// --- string / currency matching ---

func TestRecon_MatchesString_Equals(t *testing.T) {
	b := &Blnk{}
	equals := model.MatchingCriteria{Operator: "equals"}

	assert.True(t, b.matchesString("REF-1", "REF-1", equals))
	assert.True(t, b.matchesString("ref-1", "REF-1", equals), "equals must be case-insensitive")
	assert.False(t, b.matchesString("REF-1", "REF-2", equals))
	assert.False(t, b.matchesString("REF", "REF-1", equals), "prefix must not satisfy equals")

	// Grouped internal values are joined with " | "; equals must match any part exactly.
	assert.True(t, b.matchesString("ref1-b", "REF1-A | REF1-B", equals))
	assert.False(t, b.matchesString("ref1", "REF1-A | REF1-B", equals))

	assert.False(t, b.matchesString("REF-1", "REF-1", model.MatchingCriteria{Operator: "greater_than"}),
		"unsupported string operators must fail closed")
}

func TestRecon_MatchesString_ContainsAndLevenshteinBoundary(t *testing.T) {
	b := &Blnk{}

	t.Run("substring containment matches regardless of drift", func(t *testing.T) {
		c := model.MatchingCriteria{Operator: "contains", AllowableDrift: 0}
		assert.True(t, b.matchesString("INV-123", "payment inv-123 settled", c))
		assert.True(t, b.matchesString("Payment INV-123 settled", "inv-123", c), "containment works in both directions")
	})

	t.Run("levenshtein boundary at allowable drift", func(t *testing.T) {
		// "payment-12345" vs "payment-12399": distance 2, length 13.
		// drift 15%: int(13*0.15) = 1 -> no match. drift 16%: int(13*0.16) = 2 -> match.
		assert.False(t, b.matchesString("payment-12345", "payment-12399",
			model.MatchingCriteria{Operator: "contains", AllowableDrift: 15}))
		assert.True(t, b.matchesString("payment-12345", "payment-12399",
			model.MatchingCriteria{Operator: "contains", AllowableDrift: 16}))
	})

	t.Run("contains matches any joined group part", func(t *testing.T) {
		c := model.MatchingCriteria{Operator: "contains"}
		assert.True(t, b.matchesString("invoice 42", "salary may | invoice 42 paid | rent june", c))
		assert.False(t, b.matchesString("invoice 99", "salary may | rent june", c))
	})

	t.Run("drift 100 matches completely different strings of equal length", func(t *testing.T) {
		// Adversarial documentation: at 100% drift the max allowed Levenshtein distance
		// equals the longer string's length, so everything matches. Operators must
		// treat near-100 drift values as effectively disabling the check.
		c := model.MatchingCriteria{Operator: "contains", AllowableDrift: 100}
		assert.True(t, b.matchesString("abc", "xyz", c))
	})
}

func TestRecon_MatchesCurrency(t *testing.T) {
	b := &Blnk{}
	equals := model.MatchingCriteria{Operator: "equals"}

	assert.True(t, b.matchesCurrency("USD", "USD", equals))
	assert.True(t, b.matchesCurrency("usd", "USD", equals))
	assert.False(t, b.matchesCurrency("USD", "EUR", equals))
	// MIXED groups bypass the currency check entirely (documented TODO in the code).
	assert.True(t, b.matchesCurrency("GBP", "MIXED", equals))
}

func TestRecon_DominantCurrency(t *testing.T) {
	b := &Blnk{}
	assert.Equal(t, "USD", b.dominantCurrency(map[string]bool{"USD": true}))
	assert.Equal(t, "MIXED", b.dominantCurrency(map[string]bool{"USD": true, "EUR": true}))
	assert.Equal(t, "MIXED", b.dominantCurrency(map[string]bool{}))
}

// --- matchesRules / matchesGroup ---

func TestRecon_MatchesRules_EdgeCases(t *testing.T) {
	b := &Blnk{}
	external := &model.Transaction{TransactionID: "ext_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD", Reference: "REF-1", Description: "salary"}
	internal := model.Transaction{TransactionID: "int_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD", Reference: "REF-1", Description: "salary"}

	t.Run("no rules never matches", func(t *testing.T) {
		assert.False(t, b.matchesRules(external, internal, nil))
		assert.False(t, b.matchesRules(external, internal, []model.MatchingRule{}))
	})

	t.Run("unknown criteria field fails the rule", func(t *testing.T) {
		rules := []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{{Field: "narration", Operator: "equals"}}}}
		assert.False(t, b.matchesRules(external, internal, rules))
	})

	t.Run("one failing criterion fails the whole rule", func(t *testing.T) {
		rules := []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{
			{Field: "amount", Operator: "equals", AllowableDrift: 0},
			{Field: "currency", Operator: "equals"},
			{Field: "reference", Operator: "equals"},
			{Field: "date", Operator: "equals", AllowableDrift: 0},
			{Field: "description", Operator: "equals"},
		}}}
		assert.True(t, b.matchesRules(external, internal, rules))

		mismatched := internal
		mismatched.Reference = "OTHER"
		assert.False(t, b.matchesRules(external, mismatched, rules))
	})

	t.Run("any satisfied rule matches (OR across rules)", func(t *testing.T) {
		rules := []model.MatchingRule{
			{RuleID: "r1", Criteria: []model.MatchingCriteria{{Field: "reference", Operator: "equals"}}},
			{RuleID: "r2", Criteria: []model.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: 0}}},
		}
		mismatchedRef := internal
		mismatchedRef.Reference = "OTHER"
		assert.True(t, b.matchesRules(external, mismatchedRef, rules), "rule r2 must still match on amount")
	})

	t.Run("rule with zero criteria matches vacuously", func(t *testing.T) {
		// Documented (dangerous) behavior: validateRuleBasics blocks empty-criteria
		// rules at the API boundary, but matchesRules itself treats them as an
		// always-true rule. If such a rule ever reaches the matcher (e.g. written
		// directly to the DB), it will reconcile everything against everything.
		rules := []model.MatchingRule{{RuleID: "r", Criteria: nil}}
		assert.True(t, b.matchesRules(external, internal, rules))
	})
}

func TestRecon_MatchesGroup_SumAndEarliestDate(t *testing.T) {
	b := &Blnk{}

	group := []*model.Transaction{
		{TransactionID: "int_1", Amount: 100.25, CreatedAt: reconBaseTime.Add(2 * time.Hour), Description: "Part A", Reference: "RA", Currency: "USD"},
		{TransactionID: "int_2", Amount: 199.75, CreatedAt: reconBaseTime, Description: "Part B", Reference: "RB", Currency: "USD"},
	}

	amountRule := func(drift float64) []model.MatchingRule {
		return []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: drift}}}}
	}

	t.Run("external must equal the group SUM", func(t *testing.T) {
		external := &model.Transaction{TransactionID: "ext_1", Amount: 300, CreatedAt: reconBaseTime, Currency: "USD"}
		assert.True(t, b.matchesGroup(external, group, amountRule(0)), "100.25+199.75 = 300 must match exactly")

		offByOneCent := &model.Transaction{TransactionID: "ext_1", Amount: 299.99, CreatedAt: reconBaseTime, Currency: "USD"}
		assert.False(t, b.matchesGroup(offByOneCent, group, amountRule(0)), "one cent off the group sum must not match with zero drift")
	})

	t.Run("group date is the EARLIEST transaction date", func(t *testing.T) {
		dateRule := func(drift float64) []model.MatchingRule {
			return []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{{Field: "date", Operator: "equals", AllowableDrift: drift}}}}
		}
		atEarliest := &model.Transaction{TransactionID: "ext_1", Amount: 300, CreatedAt: reconBaseTime, Currency: "USD"}
		assert.True(t, b.matchesGroup(atEarliest, group, dateRule(0)))

		atLatest := &model.Transaction{TransactionID: "ext_1", Amount: 300, CreatedAt: reconBaseTime.Add(2 * time.Hour), Currency: "USD"}
		assert.False(t, b.matchesGroup(atLatest, group, dateRule(0)), "the group is anchored on its earliest date, not its latest")
		assert.True(t, b.matchesGroup(atLatest, group, dateRule(7200)), "a 2h drift must cover the span")
	})

	t.Run("descriptions and references are joined with separator", func(t *testing.T) {
		descRule := []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{{Field: "description", Operator: "equals"}}}}
		external := &model.Transaction{TransactionID: "ext_1", Description: "part b"}
		assert.True(t, b.matchesGroup(external, group, descRule), "equals must match any single joined part")
	})

	t.Run("mixed currency group bypasses currency check", func(t *testing.T) {
		mixed := []*model.Transaction{
			{TransactionID: "int_1", Amount: 10, CreatedAt: reconBaseTime, Currency: "USD"},
			{TransactionID: "int_2", Amount: 20, CreatedAt: reconBaseTime, Currency: "EUR"},
		}
		currencyRule := []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{{Field: "currency", Operator: "equals"}}}}
		external := &model.Transaction{TransactionID: "ext_1", Currency: "GBP", CreatedAt: reconBaseTime}
		assert.True(t, b.matchesGroup(external, mixed, currencyRule),
			"MIXED groups currently match any currency (documented TODO in matchesCurrency)")
	})
}

// --- calculateMatchingBounds ---

func TestRecon_CalculateMatchingBounds(t *testing.T) {
	b := &Blnk{}
	external := &model.Transaction{TransactionID: "ext_1", Amount: 200, CreatedAt: reconBaseTime, Currency: "USD"}

	rule := func(criteria ...model.MatchingCriteria) model.MatchingRule {
		return model.MatchingRule{RuleID: "r", Criteria: criteria}
	}

	t.Run("no rules leaves bounds open but pins currency", func(t *testing.T) {
		minA, maxA, minD, maxD, currency := b.calculateMatchingBounds(external, nil)
		assert.Nil(t, minA)
		assert.Nil(t, maxA)
		assert.Nil(t, minD)
		assert.Nil(t, maxD)
		require.NotNil(t, currency)
		assert.Equal(t, "USD", *currency)
	})

	t.Run("amount equals drift is a fraction of the external amount", func(t *testing.T) {
		minA, maxA, _, _, _ := b.calculateMatchingBounds(external, []model.MatchingRule{
			rule(model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0.1}),
		})
		require.NotNil(t, minA)
		require.NotNil(t, maxA)
		assert.InDelta(t, 180, *minA, 1e-9, "10%% below 200")
		assert.InDelta(t, 220, *maxA, 1e-9, "10%% above 200")
	})

	t.Run("zero drift collapses amount bounds to the exact value", func(t *testing.T) {
		minA, maxA, _, _, _ := b.calculateMatchingBounds(external, []model.MatchingRule{
			rule(model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0}),
		})
		require.NotNil(t, minA)
		require.NotNil(t, maxA)
		assert.Equal(t, 200.0, *minA)
		assert.Equal(t, 200.0, *maxA)
	})

	t.Run("multiple rules take the widest amount range", func(t *testing.T) {
		minA, maxA, _, _, _ := b.calculateMatchingBounds(external, []model.MatchingRule{
			rule(model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0.1}),
			rule(model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0.5}),
		})
		require.NotNil(t, minA)
		require.NotNil(t, maxA)
		assert.InDelta(t, 100, *minA, 1e-9)
		assert.InDelta(t, 300, *maxA, 1e-9)
	})

	t.Run("date equals drift expands in seconds both ways", func(t *testing.T) {
		_, _, minD, maxD, _ := b.calculateMatchingBounds(external, []model.MatchingRule{
			rule(model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: 3600}),
		})
		require.NotNil(t, minD)
		require.NotNil(t, maxD)
		assert.True(t, minD.Equal(reconBaseTime.Add(-time.Hour)), "got %v", minD)
		assert.True(t, maxD.Equal(reconBaseTime.Add(time.Hour)), "got %v", maxD)
	})

	t.Run("non-equals criteria do not constrain bounds", func(t *testing.T) {
		minA, maxA, minD, maxD, _ := b.calculateMatchingBounds(external, []model.MatchingRule{
			rule(
				model.MatchingCriteria{Field: "amount", Operator: "greater_than"},
				model.MatchingCriteria{Field: "date", Operator: "less_than"},
				model.MatchingCriteria{Field: "currency", Operator: "equals"},
			),
		})
		assert.Nil(t, minA)
		assert.Nil(t, maxA)
		assert.Nil(t, minD)
		assert.Nil(t, maxD)
	})
}

// --- one-to-one strategy ---

func TestRecon_OneToOne_ExactMatchAndQueryBounds(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	external := &model.Transaction{TransactionID: "ext_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD", Reference: "REF-1"}
	internal := &model.Transaction{TransactionID: "int_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD", Reference: "REF-1"}

	rules := []model.MatchingRule{{RuleID: "r1", Criteria: []model.MatchingCriteria{
		{Field: "amount", Operator: "equals", AllowableDrift: 0},
		{Field: "currency", Operator: "equals"},
	}}}

	amtPtr := mock.MatchedBy(func(p *float64) bool { return p != nil && *p == 100 })
	curPtr := mock.MatchedBy(func(p *string) bool { return p != nil && *p == "USD" })
	mockDS.On("GetTransactionsByCriteria", mock.Anything, amtPtr, amtPtr, curPtr, mock.Anything, mock.Anything, 100, int64(0)).
		Return([]*model.Transaction{internal}, nil).Once()
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.Transaction{}, nil).Maybe()

	matches, unmatched := b.oneToOneReconciliation(context.Background(), []*model.Transaction{external}, rules)

	require.Len(t, matches, 1)
	assert.Empty(t, unmatched)
	assert.Equal(t, "ext_1", matches[0].ExternalTransactionID)
	assert.Equal(t, "int_1", matches[0].InternalTransactionID)
	assert.Equal(t, 100.0, matches[0].Amount, "the recorded match amount is the external amount")
	assert.True(t, matches[0].Date.Equal(reconBaseTime))
	mockDS.AssertExpectations(t)
}

func TestRecon_OneToOne_NoRulesNeverMatches(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	external := &model.Transaction{TransactionID: "ext_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD"}
	identicalCandidate := &model.Transaction{TransactionID: "int_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD"}

	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, int64(0)).
		Return([]*model.Transaction{identicalCandidate}, nil).Once()
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
		Return([]*model.Transaction{}, nil)

	matches, unmatched := b.oneToOneReconciliation(context.Background(), []*model.Transaction{external}, nil)

	assert.Empty(t, matches, "without matching rules nothing may be auto-matched, even identical transactions")
	assert.Equal(t, []string{"ext_1"}, unmatched)
}

func TestRecon_OneToOne_DriftBoundary(t *testing.T) {
	storeReconEngineConfig()

	// Fraction semantics of matchesGroupAmount: drift 0.01 on internal 101 allows
	// |100 - 101| = 1 <= 1.01 -> match. Internal 101.02 allows 1.0102 < 1.02 -> no match.
	cases := []struct {
		name           string
		internalAmount float64
		wantMatch      bool
	}{
		{"inside drift envelope", 101, true},
		{"just outside drift envelope", 101.02, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockDS := new(mocks.MockDataSource)
			b := &Blnk{datasource: mockDS}

			external := &model.Transaction{TransactionID: "ext_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD"}
			internal := &model.Transaction{TransactionID: "int_1", Amount: tc.internalAmount, CreatedAt: reconBaseTime, Currency: "USD"}
			rules := []model.MatchingRule{{RuleID: "r1", Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
			}}}

			mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, int64(0)).
				Return([]*model.Transaction{internal}, nil).Once()
			mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, mock.Anything).
				Return([]*model.Transaction{}, nil).Maybe()

			matches, unmatched := b.oneToOneReconciliation(context.Background(), []*model.Transaction{external}, rules)
			if tc.wantMatch {
				assert.Len(t, matches, 1)
				assert.Empty(t, unmatched)
			} else {
				assert.Empty(t, matches)
				assert.Equal(t, []string{"ext_1"}, unmatched)
			}
		})
	}
}

func TestRecon_OneToOne_DatasourceErrorLeavesTransactionUnmatched(t *testing.T) {
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	external := &model.Transaction{TransactionID: "ext_1", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD"}
	mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100, int64(0)).
		Return(([]*model.Transaction)(nil), errors.New("query failed")).Once()

	rules := []model.MatchingRule{{RuleID: "r1", Criteria: []model.MatchingCriteria{
		{Field: "amount", Operator: "equals", AllowableDrift: 0},
	}}}

	matches, unmatched := b.oneToOneReconciliation(context.Background(), []*model.Transaction{external}, rules)
	assert.Empty(t, matches)
	assert.Equal(t, []string{"ext_1"}, unmatched, "a failed candidate query must report the transaction as unmatched, never matched")
}

// --- grouped strategies ---

func TestRecon_BuildGroupMap(t *testing.T) {
	b := &Blnk{}
	groups := map[string][]*model.Transaction{
		"g1": {{TransactionID: "a"}},
		"g2": {{TransactionID: "b"}},
	}
	m := b.buildGroupMap(groups)
	assert.Equal(t, map[string]bool{"g1": true, "g2": true}, m)
	assert.Empty(t, b.buildGroupMap(map[string][]*model.Transaction{}))
}

func TestRecon_MatchSingleTransaction_ConsumesGroupAndOrientsIDs(t *testing.T) {
	b := &Blnk{}

	single := &model.Transaction{TransactionID: "single_1", Amount: 300, CreatedAt: reconBaseTime, Currency: "USD"}
	grouped := map[string][]*model.Transaction{
		"g1": {
			{TransactionID: "g1_a", Amount: 100, CreatedAt: reconBaseTime, Currency: "USD"},
			{TransactionID: "g1_b", Amount: 200, CreatedAt: reconBaseTime, Currency: "USD"},
		},
	}
	rules := []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{
		{Field: "amount", Operator: "equals", AllowableDrift: 0},
	}}}

	t.Run("internal grouped (one_to_many orientation)", func(t *testing.T) {
		groupMap := map[string]bool{"g1": true}
		matchChan := make(chan model.Match, 2)
		matched := b.matchSingleTransaction(single, grouped, groupMap, &sync.Mutex{}, rules, false, matchChan)
		require.True(t, matched)
		close(matchChan)

		var got []model.Match
		for m := range matchChan {
			got = append(got, m)
		}
		require.Len(t, got, 2, "every member of the matched group must produce a match record")
		total := 0.0
		for _, m := range got {
			assert.Equal(t, "single_1", m.ExternalTransactionID, "ungrouped side is external when isExternalGrouped=false")
			total += m.Amount
		}
		assert.Equal(t, 300.0, total, "matched amounts must sum to the group total")
		assert.Empty(t, groupMap, "a matched group must be consumed so it cannot double-match")
	})

	t.Run("external grouped (many_to_one orientation)", func(t *testing.T) {
		groupMap := map[string]bool{"g1": true}
		matchChan := make(chan model.Match, 2)
		matched := b.matchSingleTransaction(single, grouped, groupMap, &sync.Mutex{}, rules, true, matchChan)
		require.True(t, matched)
		close(matchChan)

		for m := range matchChan {
			assert.Equal(t, "single_1", m.InternalTransactionID, "ungrouped side is internal when isExternalGrouped=true")
			assert.Contains(t, []string{"g1_a", "g1_b"}, m.ExternalTransactionID)
		}
	})

	t.Run("no matching group leaves transaction unmatched and groups intact", func(t *testing.T) {
		groupMap := map[string]bool{"g1": true}
		matchChan := make(chan model.Match, 2)
		other := &model.Transaction{TransactionID: "single_2", Amount: 42, CreatedAt: reconBaseTime, Currency: "USD"}
		matched := b.matchSingleTransaction(other, grouped, groupMap, &sync.Mutex{}, rules, false, matchChan)
		assert.False(t, matched)
		assert.Equal(t, map[string]bool{"g1": true}, groupMap)
	})
}

// TestRecon_MatchSingleTransaction_ConcurrentClaimIsRaceFree drives many single
// transactions at a shared groupMap from multiple goroutines at once. Each group
// must be claimed at most once (no double-match) and the shared map must not be
// read+written concurrently. Run under -race to assert the claim-and-delete is
// serialized by the supplied mutex.
func TestRecon_MatchSingleTransaction_ConcurrentClaimIsRaceFree(t *testing.T) {
	b := &Blnk{}

	const groups = 25
	const racersPerGroup = 4

	grouped := make(map[string][]*model.Transaction, groups)
	groupMap := make(map[string]bool, groups)
	for g := 0; g < groups; g++ {
		key := fmt.Sprintf("g%d", g)
		grouped[key] = []*model.Transaction{
			{TransactionID: key + "_m", Amount: 300, CreatedAt: reconBaseTime, Currency: "USD"},
		}
		groupMap[key] = true
	}

	rules := []model.MatchingRule{{RuleID: "r", Criteria: []model.MatchingCriteria{
		{Field: "amount", Operator: "equals", AllowableDrift: 0},
	}}}

	// One match record per single-member group claim.
	matchChan := make(chan model.Match, groups*racersPerGroup)
	var mu sync.Mutex
	var groupMapMu sync.Mutex

	var claimed int64
	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		for r := 0; r < racersPerGroup; r++ {
			wg.Add(1)
			go func(g, r int) {
				defer wg.Done()
				single := &model.Transaction{
					TransactionID: fmt.Sprintf("single_%d_%d", g, r),
					Amount:        300,
					CreatedAt:     reconBaseTime,
					Currency:      "USD",
				}
				if b.matchSingleTransaction(single, grouped, groupMap, &groupMapMu, rules, false, matchChan) {
					mu.Lock()
					claimed++
					mu.Unlock()
				}
			}(g, r)
		}
	}
	wg.Wait()
	close(matchChan)

	// Each of the `groups` groups can be claimed by exactly one racer.
	assert.Equal(t, int64(groups), claimed, "every group must be claimed exactly once across concurrent racers")
	assert.Empty(t, groupMap, "all groups must be consumed")

	var records int
	for range matchChan {
		records++
	}
	assert.Equal(t, groups, records, "exactly one match record per single-member group")
}

func TestRecon_OneToManyReportsUnmatchedWhenNoGroupsExist(t *testing.T) {
	// Regression: oneToMany/manyToOne used to break out of the pagination
	// loop when grouping returned zero groups, so the input transactions
	// were neither matched nor reported unmatched — they silently vanished
	// from the reconciliation results.
	storeReconEngineConfig()
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("GroupTransactions", mock.Anything, "reference", 100, int64(0)).
		Return(map[string][]*model.Transaction{}, nil).Once()

	external := []*model.Transaction{{TransactionID: "ext_1", Amount: 10, CreatedAt: reconBaseTime, Currency: "USD"}}
	matches, unmatched := b.oneToManyReconciliation(context.Background(), external, "reference", nil, false)

	assert.Empty(t, matches)
	assert.Equal(t, []string{"ext_1"}, unmatched, "transactions must never silently disappear from reconciliation results")
}
