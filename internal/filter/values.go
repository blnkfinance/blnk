package filter

import (
	"strconv"
)

func parseValue(value string) interface{} {
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}

	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
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
