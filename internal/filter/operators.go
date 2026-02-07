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
