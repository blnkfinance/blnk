package filter

import (
	"fmt"
	"net/url"
	"testing"
)

func TestResolveOperator(t *testing.T) {
	tests := []struct {
		input    string
		expected Operator
	}{
		{"eq", OpEqual},
		{"EQ", OpEqual},
		{"ne", OpNotEqual},
		{"neq", OpNotEqual},
		{"gt", OpGreaterThan},
		{"gte", OpGreaterThanOrEqual},
		{"gteq", OpGreaterThanOrEqual},
		{"lt", OpLessThan},
		{"lte", OpLessThanOrEqual},
		{"lteq", OpLessThanOrEqual},
		{"in", OpIn},
		{"between", OpBetween},
		{"like", OpLike},
		{"ilike", OpILike},
		{"isnull", OpIsNull},
		{"isnotnull", OpIsNotNull},
		{"invalid", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ResolveOperator(tt.input)
			if result != tt.expected {
				t.Errorf("ResolveOperator(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseFromQuery(t *testing.T) {
	t.Run("parses basic equality filter", func(t *testing.T) {
		params := url.Values{"status_eq": {"APPLIED"}}
		result := ParseFromQuery(params, nil)

		if len(result.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", result.Errors)
		}
		if len(result.Filters.Filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(result.Filters.Filters))
		}

		f := result.Filters.Filters[0]
		if f.Field != "status" || f.Operator != OpEqual || f.Value != "APPLIED" {
			t.Errorf("unexpected filter: %+v", f)
		}
	})

	t.Run("parses IN operator with multiple values", func(t *testing.T) {
		params := url.Values{"currency_in": {"USD,EUR,GBP"}}
		result := ParseFromQuery(params, nil)

		if len(result.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", result.Errors)
		}

		f := result.Filters.Filters[0]
		if f.Operator != OpIn || len(f.Values) != 3 {
			t.Errorf("expected IN with 3 values, got: %+v", f)
		}
	})

	t.Run("parses BETWEEN operator", func(t *testing.T) {
		params := url.Values{"amount_between": {"100|500"}}
		result := ParseFromQuery(params, nil)

		if len(result.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", result.Errors)
		}

		f := result.Filters.Filters[0]
		if f.Operator != OpBetween || len(f.Values) != 2 {
			t.Errorf("expected BETWEEN with 2 values, got: %+v", f)
		}
	})

	t.Run("returns error for invalid BETWEEN format", func(t *testing.T) {
		params := url.Values{"amount_between": {"100"}}
		result := ParseFromQuery(params, nil)

		if len(result.Errors) != 1 {
			t.Fatalf("expected 1 error, got %d", len(result.Errors))
		}
		if result.Errors[0].Param != "amount_between" {
			t.Errorf("expected error for amount_between, got: %s", result.Errors[0].Param)
		}
	})

	t.Run("skips reserved parameters", func(t *testing.T) {
		params := url.Values{
			"limit":     {"10"},
			"offset":    {"0"},
			"sort_by":   {"created_at"},
			"status_eq": {"APPLIED"},
		}
		result := ParseFromQuery(params, nil)

		if len(result.Filters.Filters) != 1 {
			t.Errorf("expected 1 filter (reserved params skipped), got %d", len(result.Filters.Filters))
		}
	})

	t.Run("enforces max filters limit", func(t *testing.T) {
		params := url.Values{}
		for i := 0; i < 25; i++ {
			params[fmt.Sprintf("field%d_eq", i)] = []string{"value"}
		}

		opts := &ParseOptions{MaxFilters: 5}
		result := ParseFromQuery(params, opts)

		if len(result.Filters.Filters) > 5 {
			t.Errorf("expected max 5 filters, got %d", len(result.Filters.Filters))
		}
	})

	t.Run("enforces max IN values limit", func(t *testing.T) {
		values := make([]string, 150)
		for i := range values {
			values[i] = "val"
		}
		params := url.Values{"status_in": {joinStrings(values, ",")}}

		opts := &ParseOptions{MaxInValues: 100}
		result := ParseFromQuery(params, opts)

		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error for exceeding IN values, got %d", len(result.Errors))
		}
	})

	t.Run("parses underscore fields correctly", func(t *testing.T) {
		params := url.Values{"created_at_gte": {"2024-01-01"}}
		result := ParseFromQuery(params, nil)

		if len(result.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", result.Errors)
		}

		f := result.Filters.Filters[0]
		if f.Field != "created_at" || f.Operator != OpGreaterThanOrEqual {
			t.Errorf("expected field=created_at, op=gte, got: %+v", f)
		}
	})

	t.Run("handles isnull operator", func(t *testing.T) {
		params := url.Values{"identity_id_isnull": {"true"}}
		result := ParseFromQuery(params, nil)

		if len(result.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", result.Errors)
		}

		f := result.Filters.Filters[0]
		if f.Operator != OpIsNull {
			t.Errorf("expected OpIsNull, got: %s", f.Operator)
		}
	})
}

func TestValidate(t *testing.T) {
	t.Run("validates known fields for transactions", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "status", Operator: OpEqual, Value: "APPLIED"},
				{Field: "amount", Operator: OpGreaterThan, Value: 1000},
			},
		}

		err := Validate(filters, "transactions")
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
	})

	t.Run("rejects unknown fields", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "unknown_field", Operator: OpEqual, Value: "test"},
			},
		}

		err := Validate(filters, "transactions")
		if err == nil {
			t.Error("expected error for unknown field")
		}
	})

	t.Run("allows meta_data prefix with valid key", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "meta_data.customer_type", Operator: OpEqual, Value: "premium"},
			},
		}

		err := Validate(filters, "transactions")
		if err != nil {
			t.Errorf("expected no error for meta_data field, got: %v", err)
		}
	})

	t.Run("rejects meta_data with invalid key format", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "meta_data.123invalid", Operator: OpEqual, Value: "test"},
			},
		}

		err := Validate(filters, "transactions")
		if err == nil {
			t.Error("expected error for invalid meta_data key")
		}
	})

	t.Run("rejects unsupported table", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "any", Operator: OpEqual, Value: "test"},
			},
		}

		err := Validate(filters, "unknown_table")
		if err == nil {
			t.Error("expected error for unsupported table")
		}
	})

	t.Run("returns nil for nil filters", func(t *testing.T) {
		err := Validate(nil, "transactions")
		if err != nil {
			t.Errorf("expected nil for nil filters, got: %v", err)
		}
	})
}

func TestValidateSortField(t *testing.T) {
	t.Run("allows any filterable field for sorting", func(t *testing.T) {
		fields := []string{"status", "amount", "currency", "created_at", "description", "reference"}
		for _, field := range fields {
			err := ValidateSortField(field, "transactions")
			if err != nil {
				t.Errorf("expected %s to be sortable, got error: %v", field, err)
			}
		}
	})

	t.Run("rejects non-filterable field", func(t *testing.T) {
		err := ValidateSortField("nonexistent_field", "transactions")
		if err == nil {
			t.Error("expected error for non-filterable field")
		}
	})

	t.Run("allows empty sort field", func(t *testing.T) {
		err := ValidateSortField("", "transactions")
		if err != nil {
			t.Errorf("expected no error for empty sort field, got: %v", err)
		}
	})

	t.Run("rejects unsupported table", func(t *testing.T) {
		err := ValidateSortField("any_field", "unknown_table")
		if err == nil {
			t.Error("expected error for unsupported table")
		}
	})
}

func TestValidateSortByForTable(t *testing.T) {
	t.Run("returns nil for nil opts", func(t *testing.T) {
		err := ValidateSortByForTable(nil, "balances")
		if err != nil {
			t.Errorf("expected no error for nil opts, got: %v", err)
		}
	})

	t.Run("returns nil for empty sort_by", func(t *testing.T) {
		opts := &QueryOptions{SortBy: ""}
		err := ValidateSortByForTable(opts, "balances")
		if err != nil {
			t.Errorf("expected no error for empty sort_by, got: %v", err)
		}
	})

	t.Run("allows valid field and normalizes to lowercase", func(t *testing.T) {
		opts := &QueryOptions{SortBy: "Created_At"}
		err := ValidateSortByForTable(opts, "balances")
		if err != nil {
			t.Errorf("expected no error for valid field, got: %v", err)
		}
		if opts.SortBy != "created_at" {
			t.Errorf("expected SortBy normalized to created_at, got: %q", opts.SortBy)
		}
	})

	t.Run("rejects invalid field", func(t *testing.T) {
		opts := &QueryOptions{SortBy: "evil_field"}
		err := ValidateSortByForTable(opts, "balances")
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("rejects SQL injection attempt", func(t *testing.T) {
		opts := &QueryOptions{SortBy: "'; DROP TABLE balances;--"}
		err := ValidateSortByForTable(opts, "balances")
		if err == nil {
			t.Error("expected error for SQL injection attempt")
		}
	})

	t.Run("rejects malformed sort_by", func(t *testing.T) {
		opts := &QueryOptions{SortBy: "balance_id; DELETE FROM balances"}
		err := ValidateSortByForTable(opts, "balances")
		if err == nil {
			t.Error("expected error for malformed sort_by")
		}
	})

	t.Run("validates against correct table", func(t *testing.T) {
		opts := &QueryOptions{SortBy: "status"}
		err := ValidateSortByForTable(opts, "transactions")
		if err != nil {
			t.Errorf("status is valid for transactions, got: %v", err)
		}

		opts2 := &QueryOptions{SortBy: "status"}
		err2 := ValidateSortByForTable(opts2, "balances")
		if err2 == nil {
			t.Error("status is invalid for balances, expected error")
		}
	})
}

func TestGetValidFieldsForTable(t *testing.T) {
	tests := []struct {
		table          string
		expectedFields []string
	}{
		{"transactions", []string{"transaction_id", "amount", "status", "currency", "source", "destination"}},
		{"balances", []string{"balance_id", "ledger_id", "balance", "currency", "inflight_balance"}},
		{"ledgers", []string{"ledger_id", "name", "created_at"}},
		{"identity", []string{"identity_id", "first_name", "last_name", "email_address"}},
		{"accounts", []string{"account_id", "name", "number", "currency"}},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			fields := GetValidFieldsForTable(tt.table)
			for _, f := range tt.expectedFields {
				if !fields[f] {
					t.Errorf("expected field %s to be valid for table %s", f, tt.table)
				}
			}
		})
	}

	t.Run("returns empty for unknown table", func(t *testing.T) {
		fields := GetValidFieldsForTable("unknown")
		if len(fields) != 0 {
			t.Errorf("expected empty map for unknown table, got %d fields", len(fields))
		}
	})
}

func TestBuild(t *testing.T) {
	t.Run("builds equality condition", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "status", Operator: OpEqual, Value: "APPLIED"},
			},
		}

		result, err := Build(filters, "transactions", "t", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.Conditions) != 1 {
			t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
		}
		if result.Conditions[0] != "t.status = $1" {
			t.Errorf("unexpected condition: %s", result.Conditions[0])
		}
		if len(result.Args) != 1 || result.Args[0] != "APPLIED" {
			t.Errorf("unexpected args: %v", result.Args)
		}
	})

	t.Run("builds multiple conditions", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "status", Operator: OpEqual, Value: "APPLIED"},
				{Field: "amount", Operator: OpGreaterThan, Value: int64(1000)},
			},
		}

		result, err := Build(filters, "transactions", "t", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.Conditions) != 2 {
			t.Fatalf("expected 2 conditions, got %d", len(result.Conditions))
		}
		if result.NextArgPos != 3 {
			t.Errorf("expected NextArgPos=3, got %d", result.NextArgPos)
		}
	})

	t.Run("builds BETWEEN condition", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "amount", Operator: OpBetween, Values: []interface{}{100, 500}},
			},
		}

		result, err := Build(filters, "transactions", "", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Conditions[0] != "amount BETWEEN $1 AND $2" {
			t.Errorf("unexpected condition: %s", result.Conditions[0])
		}
	})

	t.Run("builds IS NULL condition", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "identity_id", Operator: OpIsNull},
			},
		}

		result, err := Build(filters, "balances", "b", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Conditions[0] != "b.identity_id IS NULL" {
			t.Errorf("unexpected condition: %s", result.Conditions[0])
		}
		if result.NextArgPos != 1 {
			t.Errorf("IS NULL should not consume args, NextArgPos=%d", result.NextArgPos)
		}
	})

	t.Run("returns empty for nil filters", func(t *testing.T) {
		result, err := Build(nil, "transactions", "t", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.Conditions) != 0 {
			t.Errorf("expected 0 conditions for nil filters, got %d", len(result.Conditions))
		}
	})

	t.Run("returns error for invalid field", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "invalid_field", Operator: OpEqual, Value: "test"},
			},
		}

		_, err := Build(filters, "transactions", "t", 1)
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})
}

func TestBuildWithOptions(t *testing.T) {
	t.Run("builds with sort options", func(t *testing.T) {
		filters := &QueryFilterSet{
			Filters: []QueryFilter{
				{Field: "status", Operator: OpEqual, Value: "APPLIED"},
			},
		}
		opts := &QueryOptions{SortBy: "amount", SortOrder: SortDesc}

		result, err := BuildWithOptions(filters, "transactions", "t", 1, opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.OrderBy != "t.amount DESC" {
			t.Errorf("expected 't.amount DESC', got: %s", result.OrderBy)
		}
	})

	t.Run("uses default sort when no options", func(t *testing.T) {
		result, err := BuildWithOptions(nil, "transactions", "t", 1, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.OrderBy != "t.created_at DESC" {
			t.Errorf("expected default 't.created_at DESC', got: %s", result.OrderBy)
		}
	})

	t.Run("returns error for invalid sort field", func(t *testing.T) {
		opts := &QueryOptions{SortBy: "nonexistent_field"}

		_, err := BuildWithOptions(nil, "transactions", "t", 1, opts)
		if err == nil {
			t.Error("expected error for invalid sort field")
		}
	})
}

func TestBuildOrderBy(t *testing.T) {
	tests := []struct {
		sortBy    string
		sortOrder SortOrder
		alias     string
		expected  string
	}{
		{"created_at", SortDesc, "t", "t.created_at DESC"},
		{"amount", SortAsc, "t", "t.amount ASC"},
		{"status", SortDesc, "", "status DESC"},
		{"name", SortAsc, "", "name ASC"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := BuildOrderBy(tt.sortBy, tt.sortOrder, tt.alias)
			if result != tt.expected {
				t.Errorf("BuildOrderBy(%q, %q, %q) = %q, want %q",
					tt.sortBy, tt.sortOrder, tt.alias, result, tt.expected)
			}
		})
	}
}

func TestQueryOptionsDefaultSortOrder(t *testing.T) {
	tests := []struct {
		input    SortOrder
		expected SortOrder
	}{
		{"", SortDesc},
		{"invalid", SortDesc},
		{SortAsc, SortAsc},
		{SortDesc, SortDesc},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			opts := &QueryOptions{SortOrder: tt.input}
			result := opts.DefaultSortOrder()
			if result != tt.expected {
				t.Errorf("DefaultSortOrder() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Helper function
func joinStrings(s []string, sep string) string {
	result := ""
	for i, v := range s {
		if i > 0 {
			result += sep
		}
		result += v
	}
	return result
}
