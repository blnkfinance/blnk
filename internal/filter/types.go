package filter

import "time"

// Operator represents supported filter operators.
type Operator string

const (
	OpEqual              Operator = "eq"
	OpNotEqual           Operator = "ne"
	OpGreaterThan        Operator = "gt"
	OpGreaterThanOrEqual Operator = "gte"
	OpLessThan           Operator = "lt"
	OpLessThanOrEqual    Operator = "lte"
	OpIn                 Operator = "in"
	OpBetween            Operator = "between"
	OpLike               Operator = "like"
	OpILike              Operator = "ilike"
	OpIsNull             Operator = "isnull"
	OpIsNotNull          Operator = "isnotnull"
)

// TimestampValue represents a parsed timestamp with its original string format.
type TimestampValue struct {
	Time      time.Time
	Original  string
	Precision string
}

type QueryFilter struct {
	Field    string        `json:"field"`
	Operator Operator      `json:"operator"`
	Value    interface{}   `json:"value,omitempty"`
	Values   []interface{} `json:"values,omitempty"`
}

type QueryFilterSet struct {
	Filters []QueryFilter `json:"filters"`
}

// SortOrder represents the sort direction.
type SortOrder string

const (
	SortAsc  SortOrder = "asc"
	SortDesc SortOrder = "desc"
)

// QueryOptions contains sorting and pagination options for queries.
type QueryOptions struct {
	SortBy       string    `json:"sort_by,omitempty"`
	SortOrder    SortOrder `json:"sort_order,omitempty"`
	IncludeCount bool      `json:"include_count,omitempty"`
}

// DefaultSortOrder returns desc if empty, otherwise validates and returns the order.
func (o *QueryOptions) DefaultSortOrder() SortOrder {
	if o.SortOrder == "" || (o.SortOrder != SortAsc && o.SortOrder != SortDesc) {
		return SortDesc
	}
	return o.SortOrder
}

type BuildResult struct {
	CTEs       []string
	Conditions []string
	Args       []interface{}
	NextArgPos int
	OrderBy    string // The ORDER BY clause (without "ORDER BY" prefix)
}

type ParseOptions struct {
	MaxFilters  int // default 20
	MaxInValues int // default 100
	MaxCharLen  int // default 1000
}

type ParseError struct {
	Param   string
	Message string
}

type ParseResult struct {
	Filters *QueryFilterSet
	Errors  []ParseError
}
