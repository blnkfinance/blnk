package filter

import (
	"encoding/json"
	"fmt"
)

func isStringArray(values []interface{}) bool {
	for _, v := range values {
		if _, ok := v.(string); !ok {
			return false
		}
	}
	return true
}

func convertToStringArray(values []interface{}) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}

func extractValueForSQL(value interface{}) interface{} {
	if tsVal, ok := value.(TimestampValue); ok {
		return tsVal.Time
	}
	return value
}

func buildContainmentJSON(key string, value interface{}) ([]byte, error) {
	m := map[string]interface{}{key: value}
	return json.Marshal(m)
}
