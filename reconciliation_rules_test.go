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
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
)

// reconValidRule builds a rule that must pass validation.
func reconValidRule() model.MatchingRule {
	return model.MatchingRule{
		Name:        gofakeit.BuzzWord() + " rule",
		Description: gofakeit.Sentence(4),
		Criteria: []model.MatchingCriteria{
			{Field: "amount", Operator: "equals", AllowableDrift: 1},
			{Field: "currency", Operator: "equals"},
		},
	}
}

// reconAssertNoCall asserts the mock never received a call to the given method,
// regardless of arguments (testify's AssertNotCalled only matches exact argument lists).
func reconAssertNoCall(t *testing.T, m *mocks.MockDataSource, method string) {
	t.Helper()
	for _, c := range m.Calls {
		if c.Method == method {
			t.Errorf("expected no calls to %s, but it was called with %v", method, c.Arguments)
		}
	}
}

func TestRecon_ValidateRuleBasics(t *testing.T) {
	b := &Blnk{}

	tests := []struct {
		name    string
		rule    model.MatchingRule
		wantErr string
	}{
		{
			name:    "empty name rejected",
			rule:    model.MatchingRule{Criteria: []model.MatchingCriteria{{Field: "amount", Operator: "equals"}}},
			wantErr: "rule name is required",
		},
		{
			name:    "nil criteria rejected",
			rule:    model.MatchingRule{Name: "r"},
			wantErr: "at least one matching criteria is required",
		},
		{
			name:    "empty criteria slice rejected",
			rule:    model.MatchingRule{Name: "r", Criteria: []model.MatchingCriteria{}},
			wantErr: "at least one matching criteria is required",
		},
		{
			name: "valid rule accepted",
			rule: model.MatchingRule{Name: "r", Criteria: []model.MatchingCriteria{{Field: "amount", Operator: "equals"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := b.validateRuleBasics(&tt.rule)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestRecon_ValidateOperator(t *testing.T) {
	b := &Blnk{}

	for _, op := range []string{"equals", "greater_than", "less_than", "contains"} {
		assert.NoError(t, b.validateOperator(op), "operator %q must be valid", op)
	}

	// Note: "after"/"before" are implemented by matchesGroupDate but are NOT valid
	// operators here; see TestRecon_SuspectedBug_DateOperatorsUnreachable.
	for _, op := range []string{"", "EQUALS", "Equals", "after", "before", "regex", "not_equals", " equals"} {
		assert.Error(t, b.validateOperator(op), "operator %q must be rejected", op)
	}
}

func TestRecon_ValidateField(t *testing.T) {
	b := &Blnk{}

	for _, f := range []string{"amount", "date", "description", "reference", "currency"} {
		assert.NoError(t, b.validateField(f), "field %q must be valid", f)
	}

	for _, f := range []string{"", "Amount", "AMOUNT", "narration", "id", "source", " amount"} {
		assert.Error(t, b.validateField(f), "field %q must be rejected", f)
	}
}

func TestRecon_ValidateDrift(t *testing.T) {
	b := &Blnk{}

	tests := []struct {
		name     string
		criteria model.MatchingCriteria
		wantErr  string
	}{
		// amount + equals: drift is a fraction, bounded [0, 1] (0.01 = 1%)
		{"amount drift 0 (boundary low)", model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0}, ""},
		{"amount drift 1 (boundary high)", model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 1}, ""},
		{"amount drift 0.5 (interior)", model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 0.5}, ""},
		{"amount drift just below 0", model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: -0.000001}, "drift for amount must be between 0 and 1 (fraction, e.g. 0.01 = 1%)"},
		{"amount drift just above 1", model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 1.000001}, "drift for amount must be between 0 and 1 (fraction, e.g. 0.01 = 1%)"},
		{"amount drift -1", model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: -1}, "drift for amount must be between 0 and 1 (fraction, e.g. 0.01 = 1%)"},

		// date + equals: drift is seconds, must be non-negative, unbounded above
		{"date drift 0 (boundary)", model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: 0}, ""},
		{"date drift one year", model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: 31536000}, ""},
		{"date drift just below 0", model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: -0.000001}, "drift for date must be non-negative (seconds)"},
		{"date drift -1", model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: -1}, "drift for date must be non-negative (seconds)"},

		// drift is only validated for operator == "equals"; documented behavior.
		{"amount greater_than negative drift unchecked", model.MatchingCriteria{Field: "amount", Operator: "greater_than", AllowableDrift: -50}, ""},
		{"amount contains drift 1000 unchecked", model.MatchingCriteria{Field: "amount", Operator: "contains", AllowableDrift: 1000}, ""},

		// non-amount/date fields are never drift-checked, even out-of-range
		{"description equals drift 1000 unchecked", model.MatchingCriteria{Field: "description", Operator: "equals", AllowableDrift: 1000}, ""},
		{"currency equals drift -5 unchecked", model.MatchingCriteria{Field: "currency", Operator: "equals", AllowableDrift: -5}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := b.validateDrift(tt.criteria)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestRecon_ValidateCriteria(t *testing.T) {
	b := &Blnk{}

	t.Run("missing field rejected", func(t *testing.T) {
		err := b.validateCriteria(model.MatchingCriteria{Operator: "equals"})
		assert.EqualError(t, err, "field and operator are required for each criteria")
	})

	t.Run("missing operator rejected", func(t *testing.T) {
		err := b.validateCriteria(model.MatchingCriteria{Field: "amount"})
		assert.EqualError(t, err, "field and operator are required for each criteria")
	})

	t.Run("invalid operator rejected", func(t *testing.T) {
		err := b.validateCriteria(model.MatchingCriteria{Field: "amount", Operator: "matches"})
		assert.EqualError(t, err, "invalid operator")
	})

	t.Run("invalid field rejected", func(t *testing.T) {
		err := b.validateCriteria(model.MatchingCriteria{Field: "narration", Operator: "equals"})
		assert.EqualError(t, err, "invalid field")
	})

	t.Run("drift error surfaces", func(t *testing.T) {
		err := b.validateCriteria(model.MatchingCriteria{Field: "amount", Operator: "equals", AllowableDrift: 101})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drift for amount")
	})

	t.Run("every valid field x operator combination passes", func(t *testing.T) {
		// field and operator are validated independently, so all 20 combinations are
		// accepted -- even semantically dead ones like amount+contains (which can
		// never match in matchesGroupAmount). Documented behavior.
		fields := []string{"amount", "date", "description", "reference", "currency"}
		operators := []string{"equals", "greater_than", "less_than", "contains"}
		for _, f := range fields {
			for _, op := range operators {
				err := b.validateCriteria(model.MatchingCriteria{Field: f, Operator: op})
				assert.NoError(t, err, "field=%s operator=%s", f, op)
			}
		}
	})
}

func TestRecon_ValidateRule(t *testing.T) {
	b := &Blnk{}

	t.Run("valid rule passes", func(t *testing.T) {
		rule := reconValidRule()
		assert.NoError(t, b.validateRule(&rule))
	})

	t.Run("basics failure short circuits", func(t *testing.T) {
		rule := reconValidRule()
		rule.Name = ""
		assert.EqualError(t, b.validateRule(&rule), "rule name is required")
	})

	t.Run("second criterion invalid fails whole rule", func(t *testing.T) {
		rule := reconValidRule()
		rule.Criteria = append(rule.Criteria, model.MatchingCriteria{Field: "date", Operator: "equals", AllowableDrift: -1})
		assert.EqualError(t, b.validateRule(&rule), "drift for date must be non-negative (seconds)")
	})
}

func TestRecon_CreateMatchingRule(t *testing.T) {
	ctx := context.Background()

	t.Run("success assigns id and timestamps", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		var recorded *model.MatchingRule
		mockDS.On("RecordMatchingRule", mock.Anything, mock.AnythingOfType("*model.MatchingRule")).
			Run(func(args mock.Arguments) { recorded = args.Get(1).(*model.MatchingRule) }).
			Return(nil).Once()

		input := reconValidRule()
		created, err := b.CreateMatchingRule(ctx, input)
		require.NoError(t, err)
		require.NotNil(t, created)

		assert.True(t, strings.HasPrefix(created.RuleID, "rule_"), "rule ID %q must have rule_ prefix", created.RuleID)
		assert.WithinDuration(t, time.Now(), created.CreatedAt, 5*time.Second)
		assert.WithinDuration(t, time.Now(), created.UpdatedAt, 5*time.Second)
		require.NotNil(t, recorded)
		assert.Equal(t, created.RuleID, recorded.RuleID)
		mockDS.AssertExpectations(t)
	})

	t.Run("validation failure never hits the datasource", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		invalid := reconValidRule()
		invalid.Criteria[0].AllowableDrift = 100.5 // > 100, invalid for amount/equals

		created, err := b.CreateMatchingRule(ctx, invalid)
		require.Error(t, err)
		assert.Nil(t, created)
		reconAssertNoCall(t, mockDS, "RecordMatchingRule")
	})

	t.Run("datasource error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		dbErr := errors.New("insert failed")
		mockDS.On("RecordMatchingRule", mock.Anything, mock.Anything).Return(dbErr).Once()

		created, err := b.CreateMatchingRule(ctx, reconValidRule())
		assert.Nil(t, created)
		assert.ErrorIs(t, err, dbErr)
		mockDS.AssertExpectations(t)
	})
}

func TestRecon_GetMatchingRule(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		want := &model.MatchingRule{RuleID: "rule_1", Name: "r"}
		mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(want, nil).Once()

		got, err := b.GetMatchingRule(ctx, "rule_1")
		require.NoError(t, err)
		assert.Equal(t, want, got)
		mockDS.AssertExpectations(t)
	})

	t.Run("error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		dbErr := errors.New("not found")
		mockDS.On("GetMatchingRule", mock.Anything, "rule_missing").Return((*model.MatchingRule)(nil), dbErr).Once()

		got, err := b.GetMatchingRule(ctx, "rule_missing")
		assert.Nil(t, got)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestRecon_UpdateMatchingRule(t *testing.T) {
	ctx := context.Background()
	originalCreatedAt := time.Date(2020, 6, 1, 12, 0, 0, 0, time.UTC)

	existing := &model.MatchingRule{
		RuleID:    "rule_1",
		Name:      "original",
		CreatedAt: originalCreatedAt,
		UpdatedAt: originalCreatedAt,
		Criteria:  []model.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: 1}},
	}

	t.Run("success preserves original CreatedAt", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(existing, nil).Once()
		var persisted *model.MatchingRule
		mockDS.On("UpdateMatchingRule", mock.Anything, mock.AnythingOfType("*model.MatchingRule")).
			Run(func(args mock.Arguments) { persisted = args.Get(1).(*model.MatchingRule) }).
			Return(nil).Once()

		update := reconValidRule()
		update.RuleID = "rule_1"
		updated, err := b.UpdateMatchingRule(ctx, update)
		require.NoError(t, err)
		require.NotNil(t, updated)

		assert.Equal(t, originalCreatedAt, updated.CreatedAt, "CreatedAt must be preserved from the existing rule")
		assert.True(t, updated.UpdatedAt.After(originalCreatedAt), "UpdatedAt must be refreshed")
		require.NotNil(t, persisted)
		assert.Equal(t, originalCreatedAt, persisted.CreatedAt)
		mockDS.AssertExpectations(t)
	})

	t.Run("lookup error propagates and update is not attempted", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		dbErr := errors.New("rule not found")
		mockDS.On("GetMatchingRule", mock.Anything, "rule_x").Return((*model.MatchingRule)(nil), dbErr).Once()

		update := reconValidRule()
		update.RuleID = "rule_x"
		updated, err := b.UpdateMatchingRule(ctx, update)
		assert.Nil(t, updated)
		assert.ErrorIs(t, err, dbErr)
		reconAssertNoCall(t, mockDS, "UpdateMatchingRule")
	})

	t.Run("invalid update rejected before persisting", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(existing, nil).Once()

		update := model.MatchingRule{RuleID: "rule_1"} // no name, no criteria
		updated, err := b.UpdateMatchingRule(ctx, update)
		assert.Nil(t, updated)
		require.Error(t, err)
		assert.EqualError(t, err, "rule name is required")
		reconAssertNoCall(t, mockDS, "UpdateMatchingRule")
	})

	t.Run("datasource update error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		dbErr := errors.New("update failed")
		mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(existing, nil).Once()
		mockDS.On("UpdateMatchingRule", mock.Anything, mock.Anything).Return(dbErr).Once()

		update := reconValidRule()
		update.RuleID = "rule_1"
		updated, err := b.UpdateMatchingRule(ctx, update)
		assert.Nil(t, updated)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestRecon_DeleteMatchingRule(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		mockDS.On("DeleteMatchingRule", mock.Anything, "rule_1").Return(nil).Once()
		assert.NoError(t, b.DeleteMatchingRule(ctx, "rule_1"))
		mockDS.AssertExpectations(t)
	})

	t.Run("error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		dbErr := errors.New("delete failed")
		mockDS.On("DeleteMatchingRule", mock.Anything, "rule_1").Return(dbErr).Once()
		assert.ErrorIs(t, b.DeleteMatchingRule(ctx, "rule_1"), dbErr)
	})
}

func TestRecon_ListMatchingRules(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		want := []*model.MatchingRule{{RuleID: "rule_1"}, {RuleID: "rule_2"}}
		mockDS.On("GetMatchingRules", mock.Anything).Return(want, nil).Once()

		got, err := b.ListMatchingRules(ctx)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("error propagates", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}
		dbErr := errors.New("list failed")
		mockDS.On("GetMatchingRules", mock.Anything).Return(([]*model.MatchingRule)(nil), dbErr).Once()

		got, err := b.ListMatchingRules(ctx)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestRecon_GetMatchingRulesByIDs(t *testing.T) {
	ctx := context.Background()

	t.Run("empty id list returns no rules and touches nothing", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		rules, err := b.getMatchingRules(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, rules)
		reconAssertNoCall(t, mockDS, "GetMatchingRule")
	})

	t.Run("resolves ids in order", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		r1 := &model.MatchingRule{RuleID: "rule_1", Name: "first"}
		r2 := &model.MatchingRule{RuleID: "rule_2", Name: "second"}
		mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(r1, nil).Once()
		mockDS.On("GetMatchingRule", mock.Anything, "rule_2").Return(r2, nil).Once()

		rules, err := b.getMatchingRules(ctx, []string{"rule_1", "rule_2"})
		require.NoError(t, err)
		require.Len(t, rules, 2)
		assert.Equal(t, "rule_1", rules[0].RuleID)
		assert.Equal(t, "rule_2", rules[1].RuleID)
		mockDS.AssertExpectations(t)
	})

	t.Run("error on any id aborts the whole fetch", func(t *testing.T) {
		mockDS := new(mocks.MockDataSource)
		b := &Blnk{datasource: mockDS}

		dbErr := errors.New("missing rule")
		mockDS.On("GetMatchingRule", mock.Anything, "rule_1").Return(&model.MatchingRule{RuleID: "rule_1"}, nil).Once()
		mockDS.On("GetMatchingRule", mock.Anything, "rule_2").Return((*model.MatchingRule)(nil), dbErr).Once()

		rules, err := b.getMatchingRules(ctx, []string{"rule_1", "rule_2", "rule_3"})
		assert.Nil(t, rules)
		assert.ErrorIs(t, err, dbErr)
		mockDS.AssertExpectations(t)
	})
}
