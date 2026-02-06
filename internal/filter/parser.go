package filter

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	defaultMaxFilters  = 20
	defaultMaxInValues = 100
	defaultMaxCharLen  = 1000
)

// ParseFromQuery parses URL query parameters into a ParseResult.
// Returns errors for invalid params rather than silently dropping them.
func ParseFromQuery(queryParams url.Values, opts *ParseOptions) *ParseResult {
	maxFilters := defaultMaxFilters
	maxInValues := defaultMaxInValues
	maxCharLen := defaultMaxCharLen
	if opts != nil {
		if opts.MaxFilters > 0 {
			maxFilters = opts.MaxFilters
		}
		if opts.MaxInValues > 0 {
			maxInValues = opts.MaxInValues
		}
		if opts.MaxCharLen > 0 {
			maxCharLen = opts.MaxCharLen
		}
	}

	result := &ParseResult{
		Filters: &QueryFilterSet{
			Filters: make([]QueryFilter, 0),
		},
		Errors: make([]ParseError, 0),
	}

	filterCount := 0
	for key, values := range queryParams {
		if len(values) == 0 {
			continue
		}

		if isReservedParam(key) {
			continue
		}

		// Parse field_operator format
		parts := strings.Split(key, "_")
		if len(parts) < 2 {
			continue
		}

		// Extract operator (last part) and field (everything before)
		operatorStr := parts[len(parts)-1]
		field := strings.Join(parts[:len(parts)-1], "_")

		operator := ResolveOperator(operatorStr)
		if operator == "" {
			// Not a recognized operator suffix — skip silently
			// (could be a regular query param like "some_param=value")
			continue
		}

		// Enforce max filters
		if filterCount >= maxFilters {
			result.Errors = append(result.Errors, ParseError{
				Param:   key,
				Message: fmt.Sprintf("exceeded maximum number of filters (%d)", maxFilters),
			})
			continue
		}

		value := values[0]

		// Enforce max char length
		if len(value) > maxCharLen {
			result.Errors = append(result.Errors, ParseError{
				Param:   key,
				Message: fmt.Sprintf("value exceeds maximum length (%d chars)", maxCharLen),
			})
			continue
		}

		f := QueryFilter{
			Field:    field,
			Operator: operator,
		}

		switch operator {
		case OpBetween:
			betweenValues := strings.Split(value, "|")
			if len(betweenValues) != 2 {
				result.Errors = append(result.Errors, ParseError{
					Param:   key,
					Message: "between operator requires exactly 2 pipe-separated values (value1|value2)",
				})
				continue
			}
			f.Values = []interface{}{
				parseValue(betweenValues[0]),
				parseValue(betweenValues[1]),
			}

		case OpIn:
			inValues := strings.Split(value, ",")
			if len(inValues) > maxInValues {
				result.Errors = append(result.Errors, ParseError{
					Param:   key,
					Message: fmt.Sprintf("IN operator exceeds maximum values (%d)", maxInValues),
				})
				continue
			}
			f.Values = make([]interface{}, len(inValues))
			for i, v := range inValues {
				f.Values[i] = parseValue(strings.TrimSpace(v))
			}

		case OpIsNull, OpIsNotNull:
			// No value needed for null checks

		default:
			f.Value = parseValue(value)
		}

		result.Filters.Filters = append(result.Filters.Filters, f)
		filterCount++
	}

	return result
}
