package monitoringexporter

import (
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"
)

const redacted = "[REDACTED]"

var sensitiveKeyParts = []string{
	"authorization",
	"token",
	"secret",
	"password",
	"api_key",
	"apikey",
	"credential",
	"dsn",
	"url",
	"uri",
	"endpoint",
	"callback",
	"metadata",
	"webhook",
}

var (
	absoluteURLPattern = regexp.MustCompile(`https?://[^\s"'<>]+`)
	queryValuePattern  = regexp.MustCompile(`([?&][^=\s&]+)=([^&\s]+)`)
	bearerPattern      = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+`)
)

func RedactFields(fields logrus.Fields) map[string]interface{} {
	if len(fields) == 0 {
		return nil
	}

	out := make(map[string]interface{}, len(fields))
	for key, value := range fields {
		if IsSensitiveKey(key) {
			out[key] = redacted
			continue
		}
		out[key] = RedactValue(value)
	}
	return out
}

func RedactValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case error:
		return RedactString(v.Error())
	case string:
		return RedactString(v)
	case fmt.Stringer:
		return RedactString(v.String())
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, nested := range v {
			if IsSensitiveKey(key) {
				out[key] = redacted
				continue
			}
			out[key] = RedactValue(nested)
		}
		return out
	case map[string]string:
		out := make(map[string]interface{}, len(v))
		for key, nested := range v {
			if IsSensitiveKey(key) {
				out[key] = redacted
				continue
			}
			out[key] = RedactString(nested)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, nested := range v {
			out[i] = RedactValue(nested)
		}
		return out
	case []string:
		out := make([]interface{}, len(v))
		for i, nested := range v {
			out[i] = RedactString(nested)
		}
		return out
	default:
		return redactReflectValue(value)
	}
}

func RedactString(value string) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer "+redacted)
	value = absoluteURLPattern.ReplaceAllStringFunc(value, redactURL)
	value = queryValuePattern.ReplaceAllString(value, "$1"+redacted)
	if len(value) <= 512 {
		return value
	}
	return value[:512] + "...[truncated]"
}

func redactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return redacted
	}
	if parsed.User != nil {
		parsed.User = url.User(redacted)
	}
	if parsed.RawQuery != "" {
		query := parsed.Query()
		for key := range query {
			query.Set(key, redacted)
		}
		parsed.RawQuery = query.Encode()
	}
	return parsed.String()
}

func IsSensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(key), "-", "_"))
	for _, part := range sensitiveKeyParts {
		if strings.Contains(normalized, part) {
			return true
		}
	}
	return false
}

func redactReflectValue(value interface{}) interface{} {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return value
		}
		out := make(map[string]interface{}, rv.Len())
		for _, key := range rv.MapKeys() {
			keyString := key.String()
			if IsSensitiveKey(keyString) {
				out[keyString] = redacted
				continue
			}
			out[keyString] = RedactValue(rv.MapIndex(key).Interface())
		}
		return out
	case reflect.Slice, reflect.Array:
		out := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = RedactValue(rv.Index(i).Interface())
		}
		return out
	default:
		return value
	}
}
