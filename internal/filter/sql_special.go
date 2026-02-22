package filter

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

func buildBalanceIdCondition(f QueryFilter, tableAlias string, argPosition int) (condition string, args []interface{}, newArgPosition int, ctes []string) {
	srcField := "source"
	dstField := "destination"
	if tableAlias != "" {
		srcField = tableAlias + ".source"
		dstField = tableAlias + ".destination"
	}

	switch f.Operator {
	case OpEqual:
		condition = fmt.Sprintf("(%s = $%d OR %s = $%d)", srcField, argPosition, dstField, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpNotEqual:
		condition = fmt.Sprintf("(%s != $%d AND %s != $%d)", srcField, argPosition, dstField, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpIn:
		if len(f.Values) > 0 {
			if isStringArray(f.Values) {
				condition = fmt.Sprintf("(%s = ANY($%d) OR %s = ANY($%d))", srcField, argPosition, dstField, argPosition)
				args = []interface{}{pq.Array(convertToStringArray(f.Values))}
				newArgPosition = argPosition + 1
			} else {
				placeholders := make([]string, len(f.Values))
				args = make([]interface{}, len(f.Values))
				for i, val := range f.Values {
					placeholders[i] = fmt.Sprintf("$%d", argPosition+i)
					args[i] = extractValueForSQL(val)
				}
				ph := strings.Join(placeholders, ", ")
				condition = fmt.Sprintf("(%s IN (%s) OR %s IN (%s))",
					srcField, ph, dstField, ph)
				newArgPosition = argPosition + len(f.Values)
			}
		}

	case OpIsNull:
		condition = fmt.Sprintf("(%s IS NULL AND %s IS NULL)", srcField, dstField)
		args = []interface{}{}
		newArgPosition = argPosition

	case OpIsNotNull:
		condition = fmt.Sprintf("(%s IS NOT NULL OR %s IS NOT NULL)", srcField, dstField)
		args = []interface{}{}
		newArgPosition = argPosition

	default:
		return "", nil, argPosition, nil
	}

	return condition, args, newArgPosition, nil
}

func buildIndicatorCondition(f QueryFilter, tableAlias string, argPosition int) (condition string, args []interface{}, newArgPosition int, ctes []string) {
	var subqueryCondition string
	var subqueryArgs []interface{}

	switch f.Operator {
	case OpEqual:
		subqueryCondition = fmt.Sprintf("b.indicator = $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpNotEqual:
		subqueryCondition = fmt.Sprintf("b.indicator != $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThan:
		subqueryCondition = fmt.Sprintf("b.indicator > $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThanOrEqual:
		subqueryCondition = fmt.Sprintf("b.indicator >= $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThan:
		subqueryCondition = fmt.Sprintf("b.indicator < $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThanOrEqual:
		subqueryCondition = fmt.Sprintf("b.indicator <= $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLike:
		subqueryCondition = fmt.Sprintf("b.indicator LIKE $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpILike:
		subqueryCondition = fmt.Sprintf("b.indicator ILIKE $%d", argPosition)
		subqueryArgs = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpIn:
		if len(f.Values) > 0 {
			if isStringArray(f.Values) {
				subqueryCondition = fmt.Sprintf("b.indicator = ANY($%d)", argPosition)
				subqueryArgs = []interface{}{pq.Array(convertToStringArray(f.Values))}
				newArgPosition = argPosition + 1
			} else {
				placeholders := make([]string, len(f.Values))
				subqueryArgs = make([]interface{}, len(f.Values))
				for i, val := range f.Values {
					placeholders[i] = fmt.Sprintf("$%d", argPosition+i)
					subqueryArgs[i] = extractValueForSQL(val)
				}
				subqueryCondition = fmt.Sprintf("b.indicator IN (%s)", strings.Join(placeholders, ", "))
				newArgPosition = argPosition + len(f.Values)
			}
		} else {
			return "", nil, argPosition, nil
		}

	case OpBetween:
		if len(f.Values) == 2 {
			subqueryCondition = fmt.Sprintf("b.indicator BETWEEN $%d AND $%d", argPosition, argPosition+1)
			subqueryArgs = []interface{}{extractValueForSQL(f.Values[0]), extractValueForSQL(f.Values[1])}
			newArgPosition = argPosition + 2
		} else {
			return "", nil, argPosition, nil
		}

	case OpIsNull:
		subqueryCondition = "b.indicator IS NULL"
		subqueryArgs = []interface{}{}
		newArgPosition = argPosition

	case OpIsNotNull:
		subqueryCondition = "b.indicator IS NOT NULL"
		subqueryArgs = []interface{}{}
		newArgPosition = argPosition

	default:
		return "", nil, argPosition, nil
	}

	cte := fmt.Sprintf("_indicator_matches AS (SELECT b.balance_id FROM blnk.balances b WHERE %s)", subqueryCondition)
	ctes = []string{cte}

	srcField := "source"
	dstField := "destination"
	if tableAlias != "" {
		srcField = tableAlias + ".source"
		dstField = tableAlias + ".destination"
	}
	condition = fmt.Sprintf("(%s IN (SELECT balance_id FROM _indicator_matches) OR %s IN (SELECT balance_id FROM _indicator_matches))",
		srcField, dstField)
	args = subqueryArgs

	return condition, args, newArgPosition, ctes
}
