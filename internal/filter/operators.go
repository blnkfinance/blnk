package filter

import "strings"

func ResolveOperator(s string) Operator {
	switch strings.ToLower(s) {
	case "eq":
		return OpEqual
	case "ne", "neq":
		return OpNotEqual
	case "gt":
		return OpGreaterThan
	case "gte", "gteq":
		return OpGreaterThanOrEqual
	case "lt":
		return OpLessThan
	case "lte", "lteq":
		return OpLessThanOrEqual
	case "in":
		return OpIn
	case "between":
		return OpBetween
	case "like":
		return OpLike
	case "ilike":
		return OpILike
	case "isnull":
		return OpIsNull
	case "isnotnull":
		return OpIsNotNull
	default:
		return ""
	}
}

func ResolveLogicalOperator(s string) LogicalOperator {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "and":
		return LogicalAnd
	case "or":
		return LogicalOr
	default:
		return ""
	}
}

func IsValidLogicalOperator(op LogicalOperator) bool {
	return op == "" || op == LogicalAnd || op == LogicalOr
}

// IsValidOperator reports whether op is one of the supported filter operators.
func IsValidOperator(op Operator) bool {
	switch op {
	case OpEqual, OpNotEqual,
		OpGreaterThan, OpGreaterThanOrEqual,
		OpLessThan, OpLessThanOrEqual,
		OpIn, OpBetween,
		OpLike, OpILike,
		OpIsNull, OpIsNotNull:
		return true
	}
	return false
}
