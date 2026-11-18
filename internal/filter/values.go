package filter

import (
	"strconv"
	"strings"
)

func parseValue(value string) interface{} {
	// Keep values like "0123456789" or "+2348012345678" as strings so the leading 0 or + isn't lost.
	if !isIdentifierLike(value) {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}

		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}

	if value == "true" {
		return true
	}
	if value == "false" {
		return false
	}

	if timeVal, err := ParseDateTime(value); err == nil {
		return TimestampValue{
			Time:      timeVal,
			Original:  value,
			Precision: getDatePrecisionFromString(value),
		}
	}

	return value
}

func isIdentifierLike(value string) bool {
	if strings.HasPrefix(value, "+") {
		return true
	}
	return len(value) > 1 && value[0] == '0' && value[1] >= '0' && value[1] <= '9'
}
