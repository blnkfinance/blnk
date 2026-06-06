package monitoringexporter

import (
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestRedactFields(t *testing.T) {
	fields := logrus.Fields{
		"Authorization": "Bearer secret",
		"api_key":       "key",
		"hook_url":      "https://secret@example.com/hook?token=abc",
		"endpoint":      "https://observe.blnk.cloud/path?write_key=abc",
		"metadata": map[string]interface{}{
			"customer": "Jane",
		},
		"safe": "value",
		"nested": map[string]interface{}{
			"password": "pass",
			"status":   "ok",
		},
	}

	out := RedactFields(fields)
	if out["Authorization"] != redacted {
		t.Fatalf("expected Authorization to be redacted")
	}
	if out["api_key"] != redacted {
		t.Fatalf("expected api_key to be redacted")
	}
	if out["hook_url"] != redacted {
		t.Fatalf("expected hook_url to be redacted")
	}
	if out["endpoint"] != redacted {
		t.Fatalf("expected endpoint to be redacted")
	}
	if out["metadata"] != redacted {
		t.Fatalf("expected metadata to be redacted")
	}
	if out["safe"] != "value" {
		t.Fatalf("expected safe field to remain")
	}

	nested := out["nested"].(map[string]interface{})
	if nested["password"] != redacted {
		t.Fatalf("expected nested password to be redacted")
	}
	if nested["status"] != "ok" {
		t.Fatalf("expected nested status to remain")
	}
}

func TestRedactStringTruncatesLongValues(t *testing.T) {
	long := strings.Repeat("a", 600)
	got := RedactString(long)
	if len(got) >= len(long) {
		t.Fatalf("expected long string to be truncated")
	}
	if !strings.Contains(got, "[truncated]") {
		t.Fatalf("expected truncation marker")
	}
}

func TestRedactStringSanitizesSecretsInMessages(t *testing.T) {
	input := `GET /transactions?token=abc&safe=ok https://user:pass@example.com/cb?signature=secret Authorization: Bearer live_token`
	got := RedactString(input)

	for _, leaked := range []string{"token=abc", "safe=ok", "user:pass", "signature=secret", "live_token"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("expected %q to be redacted from %q", leaked, got)
		}
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("expected redaction marker in %q", got)
	}
}

func TestRedactValueHandlesStringMapsAndSlices(t *testing.T) {
	input := logrus.Fields{
		"headers": map[string]string{
			"Authorization": "Bearer secret",
			"X-Request-ID":  "req_123",
		},
		"items": []interface{}{
			map[string]string{"callback_url": "https://example.com/cb?token=abc"},
			"https://example.com/path?token=abc",
		},
	}

	out := RedactFields(input)

	headers := out["headers"].(map[string]interface{})
	if headers["Authorization"] != redacted {
		t.Fatalf("expected nested Authorization to be redacted: %+v", headers)
	}
	if headers["X-Request-ID"] != "req_123" {
		t.Fatalf("expected safe string map value to remain: %+v", headers)
	}

	items := out["items"].([]interface{})
	first := items[0].(map[string]interface{})
	if first["callback_url"] != redacted {
		t.Fatalf("expected nested callback_url to be redacted: %+v", first)
	}
	if strings.Contains(items[1].(string), "token=abc") {
		t.Fatalf("expected URL query value in slice to be redacted: %s", items[1].(string))
	}
}
