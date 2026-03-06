package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/gin-gonic/gin"
)

func TestParseFiltersFromBodyLogicalOperator(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("accepts logical_operator or", func(t *testing.T) {
		body := `{
			"filters":[{"field":"status","operator":"eq","value":"APPLIED"}],
			"logical_operator":"or"
		}`
		c := buildJSONContext(body)

		filters, _, _, _, err := ParseFiltersFromBody(c, "transactions")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if filters.LogicalOperator != filter.LogicalOr {
			t.Fatalf("expected logical_operator to be or, got %q", filters.LogicalOperator)
		}
	})

	t.Run("defaults logical_operator to and", func(t *testing.T) {
		body := `{
			"filters":[{"field":"status","operator":"eq","value":"APPLIED"}]
		}`
		c := buildJSONContext(body)

		filters, _, _, _, err := ParseFiltersFromBody(c, "transactions")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if filters.LogicalOperator != filter.LogicalAnd {
			t.Fatalf("expected logical_operator to default to and, got %q", filters.LogicalOperator)
		}
	})

	t.Run("rejects invalid logical_operator", func(t *testing.T) {
		body := `{
			"filters":[{"field":"status","operator":"eq","value":"APPLIED"}],
			"logical_operator":"xor"
		}`
		c := buildJSONContext(body)

		_, _, _, _, err := ParseFiltersFromBody(c, "transactions")
		if err == nil {
			t.Fatalf("expected error for invalid logical_operator")
		}
	})
}

func TestHasFiltersTreatsParseErrorsAsFilters(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/balances", nil)
	req.URL.RawQuery = url.Values{"logical_operator": {"xor"}}.Encode()
	c.Request = req

	if !HasFilters(c) {
		t.Fatalf("expected HasFilters to return true when query has parse errors")
	}
}

func buildJSONContext(body string) *gin.Context {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, "/filter", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req
	return c
}
