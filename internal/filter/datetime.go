package filter

import (
	"fmt"
	"strings"
	"time"
)

func ParseDateTime(value string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,                // 2006-01-02T15:04:05.999999999Z07:00
		time.RFC3339,                    // 2006-01-02T15:04:05Z07:00
		"2006-01-02T15:04:05.999999999", // Nanoseconds without timezone
		"2006-01-02T15:04:05.999999",    // Microseconds without timezone
		"2006-01-02T15:04:05.999",       // Milliseconds without timezone
		"2006-01-02T15:04:05",           // Seconds without timezone
		"2006-01-02T15:04",              // Minutes without timezone
		"2006-01-02 15:04:05.999999999", // Nanoseconds with space
		"2006-01-02 15:04:05.999999",    // Microseconds with space
		"2006-01-02 15:04:05.999",       // Milliseconds with space
		"2006-01-02 15:04:05",           // Seconds with space
		"2006-01-02 15:04",              // Minutes with space
		"2006-01-02",                    // Date only
		"2006/01/02",                    // Date only, slash separated
	}

	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", value)
}

func getDatePrecisionFromString(dateStr string) string {
	if strings.Contains(dateStr, ".") {
		parts := strings.Split(dateStr, ".")
		if len(parts) > 1 {
			fractionalPart := parts[1]
			fractionalPart = strings.TrimSuffix(fractionalPart, "Z")
			if idx := strings.IndexAny(fractionalPart, "+-"); idx != -1 {
				fractionalPart = fractionalPart[:idx]
			}

			digitCount := len(fractionalPart)
			if digitCount >= 6 {
				return "microseconds"
			} else if digitCount >= 3 {
				return "milliseconds"
			} else if digitCount > 0 {
				return "milliseconds"
			}
		}
	}

	if strings.Contains(dateStr, ":") {
		colonCount := strings.Count(dateStr, ":")
		if colonCount >= 2 {
			return "second"
		} else if colonCount == 1 {
			return "minute"
		}
	}

	if strings.Contains(dateStr, "T") || strings.Contains(dateStr, " ") {
		return "hour"
	}

	return "day"
}

func getDatePrecisionFromTime(t time.Time) string {
	if t.Nanosecond()%1000000 != 0 && t.Nanosecond()%1000 != 0 {
		return "microseconds"
	}
	if t.Nanosecond()%1000000 != 0 {
		return "microseconds"
	}
	if t.Nanosecond() != 0 {
		return "milliseconds"
	}
	if t.Second() != 0 {
		return "second"
	}
	if t.Minute() != 0 {
		return "minute"
	}
	if t.Hour() != 0 {
		return "hour"
	}
	return "day"
}

func computeTimestampRange(ts TimestampValue) (floor time.Time, ceiling time.Time) {
	t := ts.Time
	switch ts.Precision {
	case "microseconds":
		floor = t.Truncate(time.Microsecond)
		ceiling = floor.Add(time.Microsecond)
	case "milliseconds":
		floor = t.Truncate(time.Millisecond)
		ceiling = floor.Add(time.Millisecond)
	case "second":
		floor = t.Truncate(time.Second)
		ceiling = floor.Add(time.Second)
	case "minute":
		floor = t.Truncate(time.Minute)
		ceiling = floor.Add(time.Minute)
	case "hour":
		floor = t.Truncate(time.Hour)
		ceiling = floor.Add(time.Hour)
	case "day":
		y, m, d := t.Date()
		floor = time.Date(y, m, d, 0, 0, 0, 0, t.Location())
		ceiling = floor.AddDate(0, 0, 1)
	default:
		floor = t.Truncate(time.Second)
		ceiling = floor.Add(time.Second)
	}
	return floor, ceiling
}
