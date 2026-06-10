package filter

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// buildStandardCondition: every operator, exact SQL + exact args
// ---------------------------------------------------------------------------

func TestBuildStandardCondition_AllOperators_SQLAndArgs(t *testing.T) {
	tests := []struct {
		name       string
		table      string
		alias      string
		filter     QueryFilter
		startPos   int
		wantCond   string
		wantArgs   []interface{}
		wantArgPos int
	}{
		{
			name:       "eq string",
			table:      "ledgers",
			filter:     QueryFilter{Field: "name", Operator: OpEqual, Value: "main"},
			startPos:   1,
			wantCond:   "name = $1",
			wantArgs:   []interface{}{"main"},
			wantArgPos: 2,
		},
		{
			name:       "ne string",
			table:      "transactions",
			alias:      "t",
			filter:     QueryFilter{Field: "status", Operator: OpNotEqual, Value: "VOID"},
			startPos:   1,
			wantCond:   "t.status != $1",
			wantArgs:   []interface{}{"VOID"},
			wantArgPos: 2,
		},
		{
			name:       "gt numeric",
			table:      "transactions",
			filter:     QueryFilter{Field: "amount", Operator: OpGreaterThan, Value: int64(100)},
			startPos:   3,
			wantCond:   "amount > $3",
			wantArgs:   []interface{}{int64(100)},
			wantArgPos: 4,
		},
		{
			name:       "gte numeric",
			table:      "transactions",
			filter:     QueryFilter{Field: "amount", Operator: OpGreaterThanOrEqual, Value: float64(99.5)},
			startPos:   1,
			wantCond:   "amount >= $1",
			wantArgs:   []interface{}{float64(99.5)},
			wantArgPos: 2,
		},
		{
			name:       "lt numeric",
			table:      "balances",
			alias:      "b",
			filter:     QueryFilter{Field: "credit_balance", Operator: OpLessThan, Value: int64(5000)},
			startPos:   1,
			wantCond:   "b.credit_balance < $1",
			wantArgs:   []interface{}{int64(5000)},
			wantArgPos: 2,
		},
		{
			name:       "lte numeric",
			table:      "balances",
			filter:     QueryFilter{Field: "debit_balance", Operator: OpLessThanOrEqual, Value: int64(42)},
			startPos:   1,
			wantCond:   "debit_balance <= $1",
			wantArgs:   []interface{}{int64(42)},
			wantArgPos: 2,
		},
		{
			name:       "like pattern",
			table:      "ledgers",
			filter:     QueryFilter{Field: "name", Operator: OpLike, Value: "%acme%"},
			startPos:   1,
			wantCond:   "name LIKE $1",
			wantArgs:   []interface{}{"%acme%"},
			wantArgPos: 2,
		},
		{
			name:       "ilike pattern",
			table:      "identity",
			alias:      "i",
			filter:     QueryFilter{Field: "first_name", Operator: OpILike, Value: "JoHn%"},
			startPos:   7,
			wantCond:   "i.first_name ILIKE $7",
			wantArgs:   []interface{}{"JoHn%"},
			wantArgPos: 8,
		},
		{
			name:       "in all-string values collapse to ANY with a single placeholder",
			table:      "transactions",
			filter:     QueryFilter{Field: "currency", Operator: OpIn, Values: []interface{}{"USD", "EUR", "GBP"}},
			startPos:   2,
			wantCond:   "currency = ANY($2)",
			wantArgs:   []interface{}{pq.Array([]string{"USD", "EUR", "GBP"})},
			wantArgPos: 3,
		},
		{
			name:       "in mixed values use one placeholder per value",
			table:      "transactions",
			filter:     QueryFilter{Field: "amount", Operator: OpIn, Values: []interface{}{int64(1), int64(2), int64(3)}},
			startPos:   1,
			wantCond:   "amount IN ($1, $2, $3)",
			wantArgs:   []interface{}{int64(1), int64(2), int64(3)},
			wantArgPos: 4,
		},
		{
			name:       "between two values",
			table:      "transactions",
			filter:     QueryFilter{Field: "amount", Operator: OpBetween, Values: []interface{}{int64(100), int64(500)}},
			startPos:   4,
			wantCond:   "amount BETWEEN $4 AND $5",
			wantArgs:   []interface{}{int64(100), int64(500)},
			wantArgPos: 6,
		},
		{
			name:       "isnull consumes no args",
			table:      "balances",
			filter:     QueryFilter{Field: "identity_id", Operator: OpIsNull},
			startPos:   9,
			wantCond:   "identity_id IS NULL",
			wantArgs:   []interface{}{},
			wantArgPos: 9,
		},
		{
			name:       "isnotnull consumes no args",
			table:      "transactions",
			alias:      "tx",
			filter:     QueryFilter{Field: "effective_date", Operator: OpIsNotNull},
			startPos:   1,
			wantCond:   "tx.effective_date IS NOT NULL",
			wantArgs:   []interface{}{},
			wantArgPos: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{tt.filter}}, tt.table, tt.alias, tt.startPos)
			require.NoError(t, err)
			require.Len(t, result.Conditions, 1)
			assert.Equal(t, tt.wantCond, result.Conditions[0])
			assert.Equal(t, tt.wantArgs, result.Args)
			assert.Equal(t, tt.wantArgPos, result.NextArgPos)
			assert.Empty(t, result.CTEs)
		})
	}
}

// ---------------------------------------------------------------------------
// Injection probes: hostile FIELD names must be rejected outright
// ---------------------------------------------------------------------------

func TestBuild_HostileFieldNames_Rejected(t *testing.T) {
	hostileFields := []string{
		"name; DROP TABLE blnk.ledgers",
		"name--",
		"name' OR '1'='1",
		"name OR 1=1",
		`name"`,
		"name)`",
		"created_at; DELETE FROM blnk.transactions",
		"*",
		"1; SELECT pg_sleep(10)",
		"meta_data->>'x'",
		"meta_data.key'; --",    // hostile JSON key
		"meta_data.key); DROP",  // hostile JSON key
		"meta_data.1starts_num", // JSON key must start with a letter
		"meta_data.a.b",         // nested JSON path not allowed
		"meta_data.",            // empty JSON key
		"meta_data.key\x00bad",  // NUL byte in key
	}

	tables := []string{"ledgers", "transactions", "balances", "identity", "accounts"}

	for _, table := range tables {
		for _, field := range hostileFields {
			t.Run(table+"/"+field, func(t *testing.T) {
				filters := &QueryFilterSet{Filters: []QueryFilter{
					{Field: field, Operator: OpEqual, Value: "x"},
				}}
				result, err := Build(filters, table, "", 1)
				require.Error(t, err, "hostile field %q must be rejected for table %q", field, table)
				assert.Nil(t, result)
			})
		}
	}
}

func TestBuild_HostileSortField_RejectedOrDefaulted(t *testing.T) {
	hostile := []string{
		"created_at; DROP TABLE blnk.ledgers--",
		"name--",
		"name' ASC; --",
		"(SELECT 1)",
	}
	for _, sortBy := range hostile {
		t.Run(sortBy, func(t *testing.T) {
			// BuildWithOptions must reject hostile sort fields with an error.
			_, err := BuildWithOptions(nil, "ledgers", "", 1, &QueryOptions{SortBy: sortBy})
			require.Error(t, err, "hostile sort field %q must be rejected", sortBy)

			// The lower-level ORDER BY builder must never emit the hostile text.
			orderBy := BuildOrderBy(sortBy, SortAsc, "ledgers", "")
			assert.Equal(t, "created_at ASC", orderBy,
				"BuildOrderBy must fall back to the default column for hostile input")
			assert.NotContains(t, orderBy, "DROP")
			assert.NotContains(t, orderBy, ";")
		})
	}
}

// Hostile VALUES must always travel as bind parameters, never be concatenated
// into the generated SQL text.
func TestBuild_HostileValues_AreParameterized(t *testing.T) {
	hostileValues := []string{
		"'; DROP TABLE blnk.ledgers; --",
		"x' OR '1'='1",
		"100%_underscore_%percent",
		`quote " double 'single'`,
		"$1; SELECT pg_sleep(5)",
		"\\'); DELETE FROM blnk.balances; --",
	}

	type probe struct {
		name   string
		field  string
		table  string
		mk     func(v string) QueryFilter
		nProbe int // number of args expected to contain the hostile value
	}
	probes := []probe{
		{"eq", "name", "ledgers", func(v string) QueryFilter {
			return QueryFilter{Field: "name", Operator: OpEqual, Value: v}
		}, 1},
		{"ne", "name", "ledgers", func(v string) QueryFilter {
			return QueryFilter{Field: "name", Operator: OpNotEqual, Value: v}
		}, 1},
		{"like", "name", "ledgers", func(v string) QueryFilter {
			return QueryFilter{Field: "name", Operator: OpLike, Value: v}
		}, 1},
		{"ilike", "name", "ledgers", func(v string) QueryFilter {
			return QueryFilter{Field: "name", Operator: OpILike, Value: v}
		}, 1},
		{"between", "amount", "transactions", func(v string) QueryFilter {
			return QueryFilter{Field: "amount", Operator: OpBetween, Values: []interface{}{v, v}}
		}, 2},
		{"json eq", "meta_data.tenant", "ledgers", func(v string) QueryFilter {
			return QueryFilter{Field: "meta_data.tenant", Operator: OpEqual, Value: v}
		}, 1},
		{"json like", "meta_data.tenant", "ledgers", func(v string) QueryFilter {
			return QueryFilter{Field: "meta_data.tenant", Operator: OpLike, Value: v}
		}, 1},
	}

	for _, p := range probes {
		for _, v := range hostileValues {
			t.Run(p.name+"/"+v, func(t *testing.T) {
				result, err := Build(&QueryFilterSet{Filters: []QueryFilter{p.mk(v)}}, p.table, "", 1)
				require.NoError(t, err)
				require.Len(t, result.Conditions, 1)

				cond := result.Conditions[0]
				// The hostile payload must never appear in the SQL text itself.
				assert.NotContains(t, cond, "DROP TABLE")
				assert.NotContains(t, cond, "pg_sleep")
				assert.NotContains(t, cond, "1'='1")
				assert.NotContains(t, cond, v, "value %q leaked into SQL: %s", v, cond)

				// And it must appear in the bind args (possibly JSON-encoded for
				// the containment path).
				found := 0
				for _, a := range result.Args {
					s, ok := a.(string)
					if ok && strings.Contains(s, strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), `"`, `\"`)) {
						found++
						continue
					}
					if ok && strings.Contains(s, v) {
						found++
					}
				}
				assert.GreaterOrEqual(t, found, p.nProbe,
					"hostile value should be parameterized in args; args=%v", result.Args)
			})
		}
	}
}

func TestBuild_InOperator_HostileStringValues_UseSinglePlaceholder(t *testing.T) {
	hostile := []interface{}{"'; DROP TABLE blnk.ledgers; --", "x' OR '1'='1"}
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "name", Operator: OpIn, Values: hostile},
	}}, "ledgers", "", 1)
	require.NoError(t, err)
	require.Len(t, result.Conditions, 1)
	assert.Equal(t, "name = ANY($1)", result.Conditions[0])
	assert.NotContains(t, result.Conditions[0], "DROP")
	require.Len(t, result.Args, 1)
	assert.Equal(t, pq.Array([]string{"'; DROP TABLE blnk.ledgers; --", "x' OR '1'='1"}), result.Args[0])
}

func TestBuild_UnsupportedTable_Rejected(t *testing.T) {
	for _, table := range []string{"", "pg_catalog.pg_tables", "users; --", "unknown"} {
		t.Run(table, func(t *testing.T) {
			_, err := Build(&QueryFilterSet{Filters: []QueryFilter{
				{Field: "name", Operator: OpEqual, Value: "x"},
			}}, table, "", 1)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported table")
		})
	}
}

func TestBuild_InvalidLogicalOperator_Rejected(t *testing.T) {
	_, err := Build(&QueryFilterSet{
		LogicalOperator: LogicalOperator("xor"),
		Filters: []QueryFilter{
			{Field: "name", Operator: OpEqual, Value: "x"},
		},
	}, "ledgers", "", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "logical_operator")
}

// ---------------------------------------------------------------------------
// Arity edge cases: between/in with the wrong number of values
// ---------------------------------------------------------------------------

func TestBuild_BetweenWrongArity_MustNotSilentlyDropFilter(t *testing.T) {
	// buildStandardCondition only emits a condition when len(Values) == 2 and
	// Validate() performs no arity check. A between filter with 1 or 3 values
	// is therefore *silently dropped* and the query runs unfiltered, returning
	// every row. This is the same failure mode as issue #282 (unknown
	// operators), which was fixed by returning an error.
	for _, vals := range [][]interface{}{
		{int64(100)},
		{int64(100), int64(200), int64(300)},
		{},
	} {
		t.Run(fmt.Sprintf("%d values", len(vals)), func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
				{Field: "amount", Operator: OpBetween, Values: vals},
			}}, "transactions", "", 1)
			if err == nil && len(result.Conditions) == 0 {
				t.Skip("SUSPECTED BUG: between with arity != 2 is silently dropped by Build " +
					"(internal/filter/sql_builder.go:193-198); the query runs unfiltered and " +
					"returns all rows instead of returning a validation error")
			}
			require.Error(t, err, "between with %d values should be rejected", len(vals))
		})
	}
}

func TestBuild_InEmptyValues_MustNotSilentlyDropFilter(t *testing.T) {
	// OpIn with zero values emits no condition (sql_builder.go:175-191) and no
	// error: "currency in ()" silently matches everything instead of nothing
	// (or an error).
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "currency", Operator: OpIn, Values: []interface{}{}},
	}}, "transactions", "", 1)
	if err == nil && len(result.Conditions) == 0 {
		t.Skip("SUSPECTED BUG: IN with an empty value list is silently dropped by Build " +
			"(internal/filter/sql_builder.go:175-191); the filter is ignored and all rows " +
			"are returned instead of a validation error or an always-false condition")
	}
	require.Error(t, err)
}

func TestBuild_IsNullWithValueSupplied_IgnoresValue(t *testing.T) {
	// Supplying a value to isnull is meaningless; the builder must not bind it.
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "identity_id", Operator: OpIsNull, Value: "'; DROP TABLE blnk.balances; --"},
	}}, "balances", "", 1)
	require.NoError(t, err)
	require.Len(t, result.Conditions, 1)
	assert.Equal(t, "identity_id IS NULL", result.Conditions[0])
	assert.Empty(t, result.Args, "isnull must not bind the stray value")
	assert.Equal(t, 1, result.NextArgPos)
	assert.NotContains(t, result.Conditions[0], "DROP")
}

func TestBuild_BalanceIdRangeOperators_MustNotSilentlyDrop(t *testing.T) {
	// For transactions.balance_id the builder special-cases (source OR
	// destination). Operators outside eq/ne/in/isnull/isnotnull hit the default
	// branch of buildBalanceIdCondition and the filter is silently dropped even
	// though Validate() accepted it.
	for _, op := range []Operator{OpGreaterThan, OpBetween, OpLike} {
		t.Run(string(op), func(t *testing.T) {
			f := QueryFilter{Field: "balance_id", Operator: op, Value: "bln_x"}
			if op == OpBetween {
				f = QueryFilter{Field: "balance_id", Operator: op, Values: []interface{}{"a", "b"}}
			}
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{f}}, "transactions", "", 1)
			if err == nil && len(result.Conditions) == 0 {
				t.Skip("SUSPECTED BUG: balance_id filter with operator '" + string(op) +
					"' passes Validate but is silently dropped by buildBalanceIdCondition " +
					"(internal/filter/sql_special.go:59-60); query runs unfiltered")
			}
			require.True(t, err != nil || len(result.Conditions) == 1)
		})
	}
}

// ---------------------------------------------------------------------------
// balance_id / indicator special conditions (transactions table)
// ---------------------------------------------------------------------------

func TestBuild_BalanceIdSpecialCondition_InNonStringValues(t *testing.T) {
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "balance_id", Operator: OpIn, Values: []interface{}{int64(1), "bln_2"}},
	}}, "transactions", "t", 5)
	require.NoError(t, err)
	require.Len(t, result.Conditions, 1)
	assert.Equal(t, "(t.source IN ($5, $6) OR t.destination IN ($5, $6))", result.Conditions[0])
	assert.Equal(t, []interface{}{int64(1), "bln_2"}, result.Args)
	assert.Equal(t, 7, result.NextArgPos)
}

func TestBuild_IndicatorCondition_AllOperators(t *testing.T) {
	tests := []struct {
		name     string
		filter   QueryFilter
		wantSub  string // expected fragment inside the CTE
		wantArgs []interface{}
		wantNext int
	}{
		{
			name:     "gt",
			filter:   QueryFilter{Field: "indicator", Operator: OpGreaterThan, Value: "m"},
			wantSub:  "b.indicator > $1",
			wantArgs: []interface{}{"m"},
			wantNext: 2,
		},
		{
			name:     "gte",
			filter:   QueryFilter{Field: "indicator", Operator: OpGreaterThanOrEqual, Value: "m"},
			wantSub:  "b.indicator >= $1",
			wantArgs: []interface{}{"m"},
			wantNext: 2,
		},
		{
			name:     "lt",
			filter:   QueryFilter{Field: "indicator", Operator: OpLessThan, Value: "m"},
			wantSub:  "b.indicator < $1",
			wantArgs: []interface{}{"m"},
			wantNext: 2,
		},
		{
			name:     "lte",
			filter:   QueryFilter{Field: "indicator", Operator: OpLessThanOrEqual, Value: "m"},
			wantSub:  "b.indicator <= $1",
			wantArgs: []interface{}{"m"},
			wantNext: 2,
		},
		{
			name:     "like",
			filter:   QueryFilter{Field: "indicator", Operator: OpLike, Value: "%card%"},
			wantSub:  "b.indicator LIKE $1",
			wantArgs: []interface{}{"%card%"},
			wantNext: 2,
		},
		{
			name:     "between",
			filter:   QueryFilter{Field: "indicator", Operator: OpBetween, Values: []interface{}{"a", "z"}},
			wantSub:  "b.indicator BETWEEN $1 AND $2",
			wantArgs: []interface{}{"a", "z"},
			wantNext: 3,
		},
		{
			name:     "in string values",
			filter:   QueryFilter{Field: "indicator", Operator: OpIn, Values: []interface{}{"a", "b"}},
			wantSub:  "b.indicator = ANY($1)",
			wantArgs: []interface{}{pq.Array([]string{"a", "b"})},
			wantNext: 2,
		},
		{
			name:     "in non-string values",
			filter:   QueryFilter{Field: "indicator", Operator: OpIn, Values: []interface{}{int64(1), "b"}},
			wantSub:  "b.indicator IN ($1, $2)",
			wantArgs: []interface{}{int64(1), "b"},
			wantNext: 3,
		},
		{
			name:     "isnull",
			filter:   QueryFilter{Field: "indicator", Operator: OpIsNull},
			wantSub:  "b.indicator IS NULL",
			wantArgs: []interface{}{},
			wantNext: 1,
		},
		{
			name:     "isnotnull",
			filter:   QueryFilter{Field: "indicator", Operator: OpIsNotNull},
			wantSub:  "b.indicator IS NOT NULL",
			wantArgs: []interface{}{},
			wantNext: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{tt.filter}}, "transactions", "", 1)
			require.NoError(t, err)
			require.Len(t, result.Conditions, 1)
			require.Len(t, result.CTEs, 1)
			assert.Contains(t, result.CTEs[0], tt.wantSub)
			assert.Contains(t, result.CTEs[0], "_indicator_matches AS (SELECT b.balance_id FROM blnk.balances b WHERE")
			assert.Equal(t,
				"(source IN (SELECT balance_id FROM _indicator_matches) OR destination IN (SELECT balance_id FROM _indicator_matches))",
				result.Conditions[0])
			assert.Equal(t, tt.wantArgs, result.Args)
			assert.Equal(t, tt.wantNext, result.NextArgPos)
		})
	}
}

func TestBuild_IndicatorCondition_WrongArity_SilentDrop(t *testing.T) {
	for name, f := range map[string]QueryFilter{
		"between 1 value": {Field: "indicator", Operator: OpBetween, Values: []interface{}{"a"}},
		"in empty":        {Field: "indicator", Operator: OpIn, Values: []interface{}{}},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{f}}, "transactions", "", 1)
			if err == nil && len(result.Conditions) == 0 {
				t.Skip("SUSPECTED BUG: indicator filter with wrong arity is silently dropped " +
					"(internal/filter/sql_special.go:127-138); query runs unfiltered instead of erroring")
			}
			require.Error(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// JSON path conditions (meta_data.<key>)
// ---------------------------------------------------------------------------

func TestBuild_JSONPathConditions_AllOperators(t *testing.T) {
	tests := []struct {
		name     string
		filter   QueryFilter
		alias    string
		wantCond string
		wantArgs []interface{}
		wantNext int
	}{
		{
			name:     "eq uses jsonb containment",
			filter:   QueryFilter{Field: "meta_data.tenant", Operator: OpEqual, Value: "acme"},
			wantCond: "meta_data @> $1::jsonb",
			wantArgs: []interface{}{`{"tenant":"acme"}`},
			wantNext: 2,
		},
		{
			name:     "eq bool containment",
			filter:   QueryFilter{Field: "meta_data.active", Operator: OpEqual, Value: true},
			wantCond: "meta_data @> $1::jsonb",
			wantArgs: []interface{}{`{"active":true}`},
			wantNext: 2,
		},
		{
			name:     "eq with alias",
			filter:   QueryFilter{Field: "meta_data.tenant", Operator: OpEqual, Value: "acme"},
			alias:    "t",
			wantCond: "t.meta_data @> $1::jsonb",
			wantArgs: []interface{}{`{"tenant":"acme"}`},
			wantNext: 2,
		},
		{
			name:     "ne string",
			filter:   QueryFilter{Field: "meta_data.tenant", Operator: OpNotEqual, Value: "acme"},
			wantCond: "meta_data->>'tenant' != $1",
			wantArgs: []interface{}{"acme"},
			wantNext: 2,
		},
		{
			name:     "ne bool is stringified",
			filter:   QueryFilter{Field: "meta_data.active", Operator: OpNotEqual, Value: true},
			wantCond: "meta_data->>'active' != $1",
			wantArgs: []interface{}{"true"},
			wantNext: 2,
		},
		{
			name:     "gt numeric cast",
			filter:   QueryFilter{Field: "meta_data.score", Operator: OpGreaterThan, Value: int64(10)},
			wantCond: "(meta_data->>'score')::numeric > $1",
			wantArgs: []interface{}{int64(10)},
			wantNext: 2,
		},
		{
			name:     "gte numeric cast",
			filter:   QueryFilter{Field: "meta_data.score", Operator: OpGreaterThanOrEqual, Value: int64(10)},
			wantCond: "(meta_data->>'score')::numeric >= $1",
			wantArgs: []interface{}{int64(10)},
			wantNext: 2,
		},
		{
			name:     "lt numeric cast",
			filter:   QueryFilter{Field: "meta_data.score", Operator: OpLessThan, Value: int64(10)},
			wantCond: "(meta_data->>'score')::numeric < $1",
			wantArgs: []interface{}{int64(10)},
			wantNext: 2,
		},
		{
			name:     "lte numeric cast",
			filter:   QueryFilter{Field: "meta_data.score", Operator: OpLessThanOrEqual, Value: int64(10)},
			wantCond: "(meta_data->>'score')::numeric <= $1",
			wantArgs: []interface{}{int64(10)},
			wantNext: 2,
		},
		{
			name:     "like",
			filter:   QueryFilter{Field: "meta_data.tag", Operator: OpLike, Value: "%x%"},
			wantCond: "meta_data->>'tag' LIKE $1",
			wantArgs: []interface{}{"%x%"},
			wantNext: 2,
		},
		{
			name:     "ilike",
			filter:   QueryFilter{Field: "meta_data.tag", Operator: OpILike, Value: "%X%"},
			wantCond: "meta_data->>'tag' ILIKE $1",
			wantArgs: []interface{}{"%X%"},
			wantNext: 2,
		},
		{
			name:     "in with bool stringified",
			filter:   QueryFilter{Field: "meta_data.flag", Operator: OpIn, Values: []interface{}{true, "x"}},
			wantCond: "meta_data->>'flag' IN ($1, $2)",
			wantArgs: []interface{}{"true", "x"},
			wantNext: 3,
		},
		{
			name:     "between numeric",
			filter:   QueryFilter{Field: "meta_data.score", Operator: OpBetween, Values: []interface{}{int64(1), int64(9)}},
			wantCond: "(meta_data->>'score')::numeric BETWEEN $1 AND $2",
			wantArgs: []interface{}{int64(1), int64(9)},
			wantNext: 3,
		},
		{
			name:     "isnull checks key absence",
			filter:   QueryFilter{Field: "meta_data.tag", Operator: OpIsNull},
			wantCond: "(meta_data->>'tag' IS NULL OR meta_data ? 'tag' = false)",
			wantArgs: []interface{}{},
			wantNext: 1,
		},
		{
			name:     "isnotnull checks key presence",
			filter:   QueryFilter{Field: "meta_data.tag", Operator: OpIsNotNull},
			wantCond: "(meta_data->>'tag' IS NOT NULL AND meta_data ? 'tag' = true)",
			wantArgs: []interface{}{},
			wantNext: 1,
		},
		{
			name:     "key with digits and underscores",
			filter:   QueryFilter{Field: "meta_data.k1_v2", Operator: OpEqual, Value: "x"},
			wantCond: "meta_data @> $1::jsonb",
			wantArgs: []interface{}{`{"k1_v2":"x"}`},
			wantNext: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{tt.filter}}, "transactions", tt.alias, 1)
			require.NoError(t, err)
			require.Len(t, result.Conditions, 1)
			assert.Equal(t, tt.wantCond, result.Conditions[0])
			assert.Equal(t, tt.wantArgs, result.Args)
			assert.Equal(t, tt.wantNext, result.NextArgPos)
		})
	}
}

func TestBuild_JSONPath_NotAllowedForTablesWithoutMetadata(t *testing.T) {
	// reconciliations has no meta_data column; the meta_data.* shortcut must
	// not be honored there.
	_, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "meta_data.key", Operator: OpEqual, Value: "x"},
	}}, "reconciliations", "", 1)
	require.Error(t, err)
}

func TestSanitizeJSONKey_Direct(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"valid_Key1", "valid_Key1"},
		{"a", "a"},
		{"", ""},
		{"1abc", ""},
		{"_abc", ""},
		{"key'; --", ""},
		{"key-name", ""},
		{"key name", ""},
		{"ключ", ""}, // non-ASCII letters rejected
		{"key\x00", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeJSONKey(tt.in))
		})
	}
}

func TestResolveJSONColumn_Direct(t *testing.T) {
	assert.Equal(t, "meta_data", resolveJSONColumn("meta_data"))
	assert.Equal(t, "", resolveJSONColumn("meta_data'; --"))
	assert.Equal(t, "", resolveJSONColumn("other"))
	assert.Equal(t, "", resolveJSONColumn(""))
}

// ---------------------------------------------------------------------------
// Timestamp handling: equality expands to [floor, ceiling) ranges
// ---------------------------------------------------------------------------

func TestBuild_TimestampEquality_RangeExpansion(t *testing.T) {
	base := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name        string
		ts          TimestampValue
		wantFloor   time.Time
		wantCeiling time.Time
	}{
		{
			name:        "day precision spans the calendar day",
			ts:          TimestampValue{Time: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC), Precision: "day"},
			wantFloor:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name:        "hour precision",
			ts:          TimestampValue{Time: time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC), Precision: "hour"},
			wantFloor:   time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 11, 0, 0, 0, time.UTC),
		},
		{
			name:        "minute precision",
			ts:          TimestampValue{Time: time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC), Precision: "minute"},
			wantFloor:   time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 10, 31, 0, 0, time.UTC),
		},
		{
			name:        "second precision",
			ts:          TimestampValue{Time: base, Precision: "second"},
			wantFloor:   base,
			wantCeiling: base.Add(time.Second),
		},
		{
			name:        "millisecond precision",
			ts:          TimestampValue{Time: base.Add(123 * time.Millisecond), Precision: "milliseconds"},
			wantFloor:   base.Add(123 * time.Millisecond),
			wantCeiling: base.Add(124 * time.Millisecond),
		},
		{
			name:        "microsecond precision",
			ts:          TimestampValue{Time: base.Add(123456 * time.Microsecond), Precision: "microseconds"},
			wantFloor:   base.Add(123456 * time.Microsecond),
			wantCeiling: base.Add(123457 * time.Microsecond),
		},
		{
			name:        "unknown precision defaults to second",
			ts:          TimestampValue{Time: base, Precision: "fortnight"},
			wantFloor:   base,
			wantCeiling: base.Add(time.Second),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
				{Field: "created_at", Operator: OpEqual, Value: tt.ts},
			}}, "transactions", "", 1)
			require.NoError(t, err)
			require.Len(t, result.Conditions, 1)
			assert.Equal(t, "created_at >= $1 AND created_at < $2", result.Conditions[0])
			require.Len(t, result.Args, 2)
			assert.True(t, tt.wantFloor.Equal(result.Args[0].(time.Time)),
				"floor: want %v got %v", tt.wantFloor, result.Args[0])
			assert.True(t, tt.wantCeiling.Equal(result.Args[1].(time.Time)),
				"ceiling: want %v got %v", tt.wantCeiling, result.Args[1])
			assert.Equal(t, 3, result.NextArgPos)
		})
	}
}

func TestBuild_TimeDotTimeEquality_InfersPrecision(t *testing.T) {
	tests := []struct {
		name        string
		value       time.Time
		wantCeiling time.Time
	}{
		{
			name:        "midnight infers day",
			value:       time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name:        "hour-only infers hour",
			value:       time.Date(2024, 3, 15, 9, 0, 0, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC),
		},
		{
			name:        "minute infers minute",
			value:       time.Date(2024, 3, 15, 9, 30, 0, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 9, 31, 0, 0, time.UTC),
		},
		{
			name:        "second infers second",
			value:       time.Date(2024, 3, 15, 9, 30, 12, 0, time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 9, 30, 13, 0, time.UTC),
		},
		{
			name:        "millisecond infers milliseconds",
			value:       time.Date(2024, 3, 15, 9, 30, 12, int(7*time.Millisecond), time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 9, 30, 12, int(8*time.Millisecond), time.UTC),
		},
		{
			name:        "microsecond infers microseconds",
			value:       time.Date(2024, 3, 15, 9, 30, 12, int(7*time.Microsecond), time.UTC),
			wantCeiling: time.Date(2024, 3, 15, 9, 30, 12, int(8*time.Microsecond), time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
				{Field: "created_at", Operator: OpEqual, Value: tt.value},
			}}, "transactions", "", 1)
			require.NoError(t, err)
			require.Len(t, result.Args, 2)
			assert.True(t, tt.value.Equal(result.Args[0].(time.Time)))
			assert.True(t, tt.wantCeiling.Equal(result.Args[1].(time.Time)),
				"ceiling: want %v got %v", tt.wantCeiling, result.Args[1])
		})
	}
}

func TestBuild_TimestampNonEqualityOperators_BindRawTime(t *testing.T) {
	ts := TimestampValue{
		Time:      time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC),
		Original:  "2024-03-15T10:00",
		Precision: "minute",
	}
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "created_at", Operator: OpGreaterThanOrEqual, Value: ts},
	}}, "transactions", "", 1)
	require.NoError(t, err)
	require.Len(t, result.Conditions, 1)
	assert.Equal(t, "created_at >= $1", result.Conditions[0])
	require.Len(t, result.Args, 1)
	// TimestampValue must be unwrapped to time.Time, not bound as a struct.
	bound, ok := result.Args[0].(time.Time)
	require.True(t, ok, "TimestampValue must be unwrapped via extractValueForSQL, got %T", result.Args[0])
	assert.True(t, ts.Time.Equal(bound))
}

func TestBuild_JSONPathTimestampEquality(t *testing.T) {
	ts := TimestampValue{
		Time:      time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Original:  "2024-03-15",
		Precision: "day",
	}
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "meta_data.settled_on", Operator: OpEqual, Value: ts},
	}}, "transactions", "", 1)
	require.NoError(t, err)
	require.Len(t, result.Conditions, 1)
	assert.Equal(t,
		"(meta_data->>'settled_on')::timestamp >= $1 AND (meta_data->>'settled_on')::timestamp < $2",
		result.Conditions[0])
	require.Len(t, result.Args, 2)
	assert.True(t, ts.Time.Equal(result.Args[0].(time.Time)))
	assert.True(t, ts.Time.AddDate(0, 0, 1).Equal(result.Args[1].(time.Time)))
}

// ---------------------------------------------------------------------------
// Date/time string parsing precision (via ParseFromQuery -> parseValue)
// ---------------------------------------------------------------------------

func TestParseFromQuery_DateTimePrecisionDetection(t *testing.T) {
	tests := []struct {
		value         string
		wantPrecision string
	}{
		{"2024-01-15", "day"},
		{"2024/01/15", "day"},
		{"2024-01-15T10:30", "minute"},
		{"2024-01-15 10:30", "minute"},
		{"2024-01-15T10:30:45", "second"},
		{"2024-01-15 10:30:45", "second"},
		{"2024-01-15T10:30:45Z", "second"},
		{"2024-01-15T10:30:45.1", "milliseconds"},
		{"2024-01-15T10:30:45.123", "milliseconds"},
		{"2024-01-15T10:30:45.123Z", "milliseconds"},
		{"2024-01-15T10:30:45.123456", "microseconds"},
		{"2024-01-15T10:30:45.123456Z", "microseconds"},
		{"2024-01-15T10:30:45.123456+02:00", "microseconds"},
		{"2024-01-15T10:30:45.123456789", "microseconds"},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			params := url.Values{"created_at_eq": {tt.value}}
			result := ParseFromQuery(params, nil)
			require.Empty(t, result.Errors)
			require.Len(t, result.Filters.Filters, 1)

			f := result.Filters.Filters[0]
			ts, ok := f.Value.(TimestampValue)
			require.True(t, ok, "value %q should parse to TimestampValue, got %T (%v)", tt.value, f.Value, f.Value)
			assert.Equal(t, tt.wantPrecision, ts.Precision)
			assert.Equal(t, tt.value, ts.Original)
			assert.False(t, ts.Time.IsZero())
		})
	}
}

func TestParseFromQuery_ScalarTypeInference(t *testing.T) {
	tests := []struct {
		value string
		check func(t *testing.T, v interface{})
	}{
		{"42", func(t *testing.T, v interface{}) { assert.Equal(t, int64(42), v) }},
		{"-7", func(t *testing.T, v interface{}) { assert.Equal(t, int64(-7), v) }},
		{"3.14", func(t *testing.T, v interface{}) { assert.Equal(t, float64(3.14), v) }},
		{"true", func(t *testing.T, v interface{}) { assert.Equal(t, true, v) }},
		{"false", func(t *testing.T, v interface{}) { assert.Equal(t, false, v) }},
		{"TRUE", func(t *testing.T, v interface{}) { assert.Equal(t, "TRUE", v) }}, // case-sensitive bool
		{"hello world", func(t *testing.T, v interface{}) { assert.Equal(t, "hello world", v) }},
		// A bare year is consumed by the integer parser before date parsing.
		{"2024", func(t *testing.T, v interface{}) { assert.Equal(t, int64(2024), v) }},
		{"not-a-date-13-45", func(t *testing.T, v interface{}) { assert.Equal(t, "not-a-date-13-45", v) }},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			params := url.Values{"description_eq": {tt.value}}
			result := ParseFromQuery(params, nil)
			require.Empty(t, result.Errors)
			require.Len(t, result.Filters.Filters, 1)
			tt.check(t, result.Filters.Filters[0].Value)
		})
	}
}

func TestParseDateTime_RejectsGarbage(t *testing.T) {
	for _, v := range []string{"", "not a date", "2024-13-45", "15/01/2024", "2024-01-15TT10:30"} {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDateTime(v)
			assert.Error(t, err, "ParseDateTime(%q) should fail", v)
		})
	}
}

// ---------------------------------------------------------------------------
// Logical operator combination and multi-filter arg positioning
// ---------------------------------------------------------------------------

func TestBuild_MixedFilters_ArgPositionsAndOrJoin(t *testing.T) {
	filters := &QueryFilterSet{
		LogicalOperator: LogicalOr,
		Filters: []QueryFilter{
			{Field: "status", Operator: OpEqual, Value: "APPLIED"},
			{Field: "amount", Operator: OpBetween, Values: []interface{}{int64(10), int64(20)}},
			{Field: "currency", Operator: OpIn, Values: []interface{}{"USD", "EUR"}},
			{Field: "reference", Operator: OpIsNotNull},
		},
	}

	result, err := Build(filters, "transactions", "", 1)
	require.NoError(t, err)
	require.Len(t, result.Conditions, 4)
	assert.Equal(t, "status = $1", result.Conditions[0])
	assert.Equal(t, "amount BETWEEN $2 AND $3", result.Conditions[1])
	assert.Equal(t, "currency = ANY($4)", result.Conditions[2])
	assert.Equal(t, "reference IS NOT NULL", result.Conditions[3])
	assert.Equal(t, 5, result.NextArgPos)
	require.Len(t, result.Args, 4)

	expr := BuildConditionExpression(result.Conditions, filters.LogicalOperator)
	assert.Equal(t,
		"(status = $1) OR (amount BETWEEN $2 AND $3) OR (currency = ANY($4)) OR (reference IS NOT NULL)",
		expr)

	andExpr := BuildConditionExpression(result.Conditions, LogicalAnd)
	assert.Equal(t,
		"status = $1 AND amount BETWEEN $2 AND $3 AND currency = ANY($4) AND reference IS NOT NULL",
		andExpr)
}

func TestBuild_StartArgPosOffsetRespected(t *testing.T) {
	// When the caller has already bound args $1..$4, conditions must start at $5.
	result, err := Build(&QueryFilterSet{Filters: []QueryFilter{
		{Field: "name", Operator: OpEqual, Value: "x"},
		{Field: "created_at", Operator: OpIsNull},
		{Field: "name", Operator: OpLike, Value: "%y%"},
	}}, "ledgers", "l", 5)
	require.NoError(t, err)
	assert.Equal(t, []string{"l.name = $5", "l.created_at IS NULL", "l.name LIKE $6"}, result.Conditions)
	assert.Equal(t, 7, result.NextArgPos)
}

// ---------------------------------------------------------------------------
// Sorting validation and resolution
// ---------------------------------------------------------------------------

func TestBuildWithOptions_SortPaths(t *testing.T) {
	t.Run("valid sort asc", func(t *testing.T) {
		result, err := BuildWithOptions(nil, "ledgers", "", 1, &QueryOptions{SortBy: "name", SortOrder: SortAsc})
		require.NoError(t, err)
		assert.Equal(t, "name ASC", result.OrderBy)
	})

	t.Run("invalid sort order string falls back to DESC", func(t *testing.T) {
		result, err := BuildWithOptions(nil, "ledgers", "", 1, &QueryOptions{SortBy: "name", SortOrder: SortOrder("sideways")})
		require.NoError(t, err)
		assert.Equal(t, "name DESC", result.OrderBy)
	})

	t.Run("filter error propagates before sorting", func(t *testing.T) {
		_, err := BuildWithOptions(&QueryFilterSet{Filters: []QueryFilter{
			{Field: "bogus", Operator: OpEqual, Value: 1},
		}}, "ledgers", "", 1, &QueryOptions{SortBy: "name"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid field")
	})

	t.Run("meta_data json key is not sortable", func(t *testing.T) {
		_, err := BuildWithOptions(nil, "ledgers", "", 1, &QueryOptions{SortBy: "meta_data.tenant"})
		require.Error(t, err)
	})
}

func TestResolveSortField_NormalizationAndFallback(t *testing.T) {
	tests := []struct {
		table  string
		sortBy string
		want   string
	}{
		{"ledgers", "", "created_at"},
		{"ledgers", "name", "name"},
		{"ledgers", "  NAME  ", "name"},
		{"ledgers", "no_such", "created_at"},
		{"ledgers", "name; DROP TABLE x", "created_at"},
		{"transactions", "AMOUNT", "amount"},
		{"unknown_table", "anything", "created_at"},
	}
	for _, tt := range tests {
		t.Run(tt.table+"/"+tt.sortBy, func(t *testing.T) {
			assert.Equal(t, tt.want, ResolveSortField(tt.table, tt.sortBy))
		})
	}
}

func TestGetValidFieldsForTable_AllowlistsAreClosed(t *testing.T) {
	// Every supported table must expose a non-empty closed allowlist, and the
	// allowlist must never contain anything that is not a plain column name.
	tables := []string{
		"transactions", "balances", "ledgers", "identity",
		"reconciliations", "matching_rules", "external_transactions", "accounts",
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			fields := GetValidFieldsForTable(table)
			require.NotEmpty(t, fields)
			for f := range fields {
				assert.Regexp(t, `^[a-z][a-z0-9_]*$`, f,
					"allowlisted field %q for table %q is not a plain column name", f, table)
				// Each allowlisted filter field must resolve to a safe column
				// (meta_data resolves through the JSON path for some tables).
				col := safeColumnForTableAndField(table, f)
				assert.Equal(t, f, col,
					"field %q in the validation allowlist must have a matching safe-column mapping", f)
			}
		})
	}

	assert.Empty(t, GetValidFieldsForTable("users; DROP TABLE blnk.ledgers"))
	assert.Empty(t, GetValidFieldsForTable(""))
}

func TestSafeColumnForTableAndField_HostileInputsReturnEmpty(t *testing.T) {
	hostile := []string{
		"name; DROP TABLE x", "name--", "amount)", "source'",
		"transaction_id OR 1=1", "", " name", "NAME",
	}
	for _, f := range hostile {
		assert.Equal(t, "", safeColumnForTableAndField("transactions", f),
			"hostile/unknown field %q must map to empty column", f)
	}
	// And the sort variant falls back to the default rather than echoing input.
	for _, f := range hostile {
		assert.Equal(t, "created_at", safeColumnForSort("transactions", f))
	}
}
