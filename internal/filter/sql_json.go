package filter

import (
	"fmt"
	"strings"
	"time"
)

func buildJSONPathCondition(f QueryFilter, tableAlias string, argPosition int) (condition string, args []interface{}, newArgPosition int) {
	parts := strings.SplitN(f.Field, ".", 2)
	if len(parts) != 2 {
		return "", nil, argPosition
	}

	jsonCol := parts[0]
	jsonKey := parts[1]

	if tableAlias != "" {
		jsonCol = fmt.Sprintf("%s.%s", tableAlias, jsonCol)
	}

	switch f.Operator {
	case OpEqual:
		if tsVal, ok := f.Value.(TimestampValue); ok {
			floor, ceiling := computeTimestampRange(tsVal)
			condition = fmt.Sprintf("(%s->>'%s')::timestamp >= $%d AND (%s->>'%s')::timestamp < $%d",
				jsonCol, jsonKey, argPosition, jsonCol, jsonKey, argPosition+1)
			args = []interface{}{floor, ceiling}
			newArgPosition = argPosition + 2
			return
		}
		if timeVal, ok := f.Value.(time.Time); ok {
			tsVal := TimestampValue{Time: timeVal, Precision: getDatePrecisionFromTime(timeVal)}
			floor, ceiling := computeTimestampRange(tsVal)
			condition = fmt.Sprintf("(%s->>'%s')::timestamp >= $%d AND (%s->>'%s')::timestamp < $%d",
				jsonCol, jsonKey, argPosition, jsonCol, jsonKey, argPosition+1)
			args = []interface{}{floor, ceiling}
			newArgPosition = argPosition + 2
			return
		}

		jsonBytes, err := buildContainmentJSON(jsonKey, f.Value)
		if err != nil {
			return "", nil, argPosition
		}
		condition = fmt.Sprintf("%s @> $%d::jsonb", jsonCol, argPosition)
		args = []interface{}{string(jsonBytes)}
		newArgPosition = argPosition + 1

	case OpNotEqual:
		if boolVal, ok := f.Value.(bool); ok {
			condition = fmt.Sprintf("%s->>'%s' != $%d", jsonCol, jsonKey, argPosition)
			args = []interface{}{fmt.Sprintf("%t", boolVal)}
		} else {
			condition = fmt.Sprintf("%s->>'%s' != $%d", jsonCol, jsonKey, argPosition)
			args = []interface{}{extractValueForSQL(f.Value)}
		}
		newArgPosition = argPosition + 1

	case OpGreaterThan:
		condition = fmt.Sprintf("(%s->>'%s')::numeric > $%d", jsonCol, jsonKey, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThanOrEqual:
		condition = fmt.Sprintf("(%s->>'%s')::numeric >= $%d", jsonCol, jsonKey, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThan:
		condition = fmt.Sprintf("(%s->>'%s')::numeric < $%d", jsonCol, jsonKey, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThanOrEqual:
		condition = fmt.Sprintf("(%s->>'%s')::numeric <= $%d", jsonCol, jsonKey, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLike:
		condition = fmt.Sprintf("%s->>'%s' LIKE $%d", jsonCol, jsonKey, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpILike:
		condition = fmt.Sprintf("%s->>'%s' ILIKE $%d", jsonCol, jsonKey, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpIn:
		if len(f.Values) > 0 {
			placeholders := make([]string, len(f.Values))
			args = make([]interface{}, len(f.Values))
			for i, val := range f.Values {
				placeholders[i] = fmt.Sprintf("$%d", argPosition+i)
				if boolVal, ok := val.(bool); ok {
					args[i] = fmt.Sprintf("%t", boolVal)
				} else {
					args[i] = extractValueForSQL(val)
				}
			}
			condition = fmt.Sprintf("%s->>'%s' IN (%s)", jsonCol, jsonKey, strings.Join(placeholders, ", "))
			newArgPosition = argPosition + len(f.Values)
		}

	case OpBetween:
		if len(f.Values) == 2 {
			condition = fmt.Sprintf("(%s->>'%s')::numeric BETWEEN $%d AND $%d", jsonCol, jsonKey, argPosition, argPosition+1)
			args = []interface{}{extractValueForSQL(f.Values[0]), extractValueForSQL(f.Values[1])}
			newArgPosition = argPosition + 2
		}

	case OpIsNull:
		condition = fmt.Sprintf("(%s->>'%s' IS NULL OR %s ? '%s' = false)", jsonCol, jsonKey, jsonCol, jsonKey)
		args = []interface{}{}
		newArgPosition = argPosition

	case OpIsNotNull:
		condition = fmt.Sprintf("(%s->>'%s' IS NOT NULL AND %s ? '%s' = true)", jsonCol, jsonKey, jsonCol, jsonKey)
		args = []interface{}{}
		newArgPosition = argPosition

	default:
		return "", nil, argPosition
	}

	return condition, args, newArgPosition
}
