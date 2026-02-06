package filter

import (
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

func Build(filters *QueryFilterSet, table string, alias string, startArgPos int) (*BuildResult, error) {
	if filters == nil || len(filters.Filters) == 0 {
		return &BuildResult{
			CTEs:       nil,
			Conditions: []string{},
			Args:       []interface{}{},
			NextArgPos: startArgPos,
		}, nil
	}

	if err := Validate(filters, table); err != nil {
		return nil, err
	}

	result := &BuildResult{
		CTEs:       nil,
		Conditions: make([]string, 0, len(filters.Filters)),
		Args:       make([]interface{}, 0),
		NextArgPos: startArgPos,
	}

	argPos := startArgPos

	for _, f := range filters.Filters {
		var cond string
		var args []interface{}
		var nextArgPos int
		var ctes []string

		if strings.Contains(f.Field, ".") && strings.HasPrefix(f.Field, "meta_data") {
			cond, args, nextArgPos = buildJSONPathCondition(f, alias, argPos)
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
				result.Args = append(result.Args, args...)
				argPos = nextArgPos
			}
			continue
		}

		if f.Field == "balance_id" && table == "transactions" {
			cond, args, nextArgPos, ctes = buildBalanceIdCondition(f, alias, argPos)
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
				result.Args = append(result.Args, args...)
				argPos = nextArgPos
				if len(ctes) > 0 {
					result.CTEs = append(result.CTEs, ctes...)
				}
			}
			continue
		}

		if f.Field == "indicator" && table == "transactions" {
			cond, args, nextArgPos, ctes = buildIndicatorCondition(f, alias, argPos)
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
				result.Args = append(result.Args, args...)
				argPos = nextArgPos
				if len(ctes) > 0 {
					result.CTEs = append(result.CTEs, ctes...)
				}
			}
			continue
		}

		cond, args, nextArgPos = buildStandardCondition(f, alias, argPos)
		if cond != "" {
			result.Conditions = append(result.Conditions, cond)
			result.Args = append(result.Args, args...)
			argPos = nextArgPos
		}
	}

	result.NextArgPos = argPos
	return result, nil
}

func buildStandardCondition(f QueryFilter, tableAlias string, argPosition int) (condition string, args []interface{}, newArgPosition int) {
	fieldName := f.Field
	if tableAlias != "" {
		fieldName = fmt.Sprintf("%s.%s", tableAlias, f.Field)
	}

	switch f.Operator {
	case OpEqual:
		if tsVal, ok := f.Value.(TimestampValue); ok {
			floor, ceiling := computeTimestampRange(tsVal)
			condition = fmt.Sprintf("%s >= $%d AND %s < $%d", fieldName, argPosition, fieldName, argPosition+1)
			args = []interface{}{floor, ceiling}
			newArgPosition = argPosition + 2
			return
		}
		if timeVal, ok := f.Value.(time.Time); ok {
			tsVal := TimestampValue{Time: timeVal, Precision: getDatePrecisionFromTime(timeVal)}
			floor, ceiling := computeTimestampRange(tsVal)
			condition = fmt.Sprintf("%s >= $%d AND %s < $%d", fieldName, argPosition, fieldName, argPosition+1)
			args = []interface{}{floor, ceiling}
			newArgPosition = argPosition + 2
			return
		}

		condition = fmt.Sprintf("%s = $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpNotEqual:
		condition = fmt.Sprintf("%s != $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThan:
		condition = fmt.Sprintf("%s > $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThanOrEqual:
		condition = fmt.Sprintf("%s >= $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThan:
		condition = fmt.Sprintf("%s < $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThanOrEqual:
		condition = fmt.Sprintf("%s <= $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLike:
		condition = fmt.Sprintf("%s LIKE $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpILike:
		condition = fmt.Sprintf("%s ILIKE $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpIn:
		if len(f.Values) > 0 {
			if isStringArray(f.Values) {
				condition = fmt.Sprintf("%s = ANY($%d)", fieldName, argPosition)
				args = []interface{}{pq.Array(convertToStringArray(f.Values))}
			} else {
				placeholders := make([]string, len(f.Values))
				args = make([]interface{}, len(f.Values))
				for i, val := range f.Values {
					placeholders[i] = fmt.Sprintf("$%d", argPosition+i)
					args[i] = extractValueForSQL(val)
				}
				condition = fmt.Sprintf("%s IN (%s)", fieldName, strings.Join(placeholders, ", "))
			}
			newArgPosition = argPosition + len(f.Values)
		}

	case OpBetween:
		if len(f.Values) == 2 {
			condition = fmt.Sprintf("%s BETWEEN $%d AND $%d", fieldName, argPosition, argPosition+1)
			args = []interface{}{extractValueForSQL(f.Values[0]), extractValueForSQL(f.Values[1])}
			newArgPosition = argPosition + 2
		}

	case OpIsNull:
		condition = fmt.Sprintf("%s IS NULL", fieldName)
		args = []interface{}{}
		newArgPosition = argPosition

	case OpIsNotNull:
		condition = fmt.Sprintf("%s IS NOT NULL", fieldName)
		args = []interface{}{}
		newArgPosition = argPosition

	default:
		return "", nil, argPosition
	}

	return condition, args, newArgPosition
}

// BuildWithOptions builds filter conditions and includes sorting options.
// It validates both filters and sort options, returning an error if either is invalid.
func BuildWithOptions(filters *QueryFilterSet, table string, alias string, startArgPos int, opts *QueryOptions) (*BuildResult, error) {
	// First build the filter conditions
	result, err := Build(filters, table, alias, startArgPos)
	if err != nil {
		return nil, err
	}

	order := SortDesc
	sortBy := ""
	if opts != nil {
		order = opts.DefaultSortOrder()
		sortBy = opts.SortBy
		if sortBy != "" {
			if err := ValidateSortField(sortBy, table); err != nil {
				return nil, err
			}
		}
	}
	result.OrderBy = BuildOrderBy(sortBy, order, table, alias)

	return result, nil
}

// BuildOrderBy constructs an ORDER BY clause. It resolves sortBy against the table's
// allowlist and uses only allowlist-derived column names, never raw input.
func BuildOrderBy(sortBy string, sortOrder SortOrder, table string, alias string) string {
	safeField := GetDefaultSortField(table)
	if sortBy != "" {
		normalized := strings.ToLower(strings.TrimSpace(sortBy))
		allowed := GetValidFieldsForTable(table)
		if allowed[normalized] {
			safeField = normalized
		}
	}
	fieldName := safeField
	if alias != "" {
		fieldName = fmt.Sprintf("%s.%s", alias, safeField)
	}

	direction := "DESC"
	if sortOrder == SortAsc {
		direction = "ASC"
	}

	return fmt.Sprintf("%s %s", fieldName, direction)
}
