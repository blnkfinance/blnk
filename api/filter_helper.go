/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/gin-gonic/gin"
)

// ParseFiltersFromContext parses query parameters into a QueryFilterSet using the filter package.
// It handles all filter parameters in the format: field_operator=value
//
// Supported operators:
//   - eq: Equal (e.g., status_eq=APPLIED)
//   - ne: Not equal (e.g., status_ne=VOID)
//   - gt: Greater than (e.g., amount_gt=1000)
//   - gte: Greater than or equal (e.g., amount_gte=1000)
//   - lt: Less than (e.g., amount_lt=5000)
//   - lte: Less than or equal (e.g., amount_lte=5000)
//   - in: In set (e.g., currency_in=USD,EUR,GBP)
//   - between: Between range (e.g., created_at_between=2024-01-01|2024-12-31)
//   - like: Pattern match (e.g., name_like=%USD%)
//   - ilike: Case-insensitive pattern match (e.g., name_ilike=%usd%)
//   - isnull: Is null (e.g., identity_id_isnull=true)
//   - isnotnull: Is not null (e.g., identity_id_isnotnull=true)
//
// Parameters:
// - c: The gin context containing the request
// - opts: Optional parse options to limit filters, values, etc.
//
// Returns:
// - *filter.QueryFilterSet: The parsed filters, or nil if no filters found
// - []filter.ParseError: Any errors encountered during parsing
func ParseFiltersFromContext(c *gin.Context, opts *filter.ParseOptions) (*filter.QueryFilterSet, []filter.ParseError) {
	result := filter.ParseFromQuery(c.Request.URL.Query(), opts)
	return result.Filters, result.Errors
}

// HasFilters checks if the request contains any filter parameters.
// It returns true if filters were provided, false otherwise.
func HasFilters(c *gin.Context) bool {
	result := filter.ParseFromQuery(c.Request.URL.Query(), nil)
	return result.Filters != nil && len(result.Filters.Filters) > 0
}

// FilterRequest represents the JSON body for filter endpoints.
// It allows clients to pass filters directly as JSON instead of query parameters.
type FilterRequest struct {
	Filters      []filter.QueryFilter `json:"filters"`
	Limit        int                  `json:"limit,omitempty"`
	Offset       int                  `json:"offset,omitempty"`
	SortBy       string               `json:"sort_by,omitempty"`
	SortOrder    string               `json:"sort_order,omitempty"` // "asc" or "desc"
	IncludeCount bool                 `json:"include_count,omitempty"`
}

// FilterResponse wraps the response with optional count.
type FilterResponse struct {
	Data       interface{} `json:"data"`
	TotalCount *int64      `json:"total_count,omitempty"`
}

// ParseFiltersFromBody parses filters from a JSON request body.
// Returns the QueryFilterSet, QueryOptions, limit, offset, and any error.
func ParseFiltersFromBody(c *gin.Context) (*filter.QueryFilterSet, *filter.QueryOptions, int, int, error) {
	var req FilterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		return nil, nil, 0, 0, err
	}

	// Apply defaults
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	offset := req.Offset
	if offset < 0 {
		offset = 0
	}

	filterSet := &filter.QueryFilterSet{
		Filters: req.Filters,
	}

	opts := &filter.QueryOptions{
		SortBy:       req.SortBy,
		SortOrder:    filter.SortOrder(req.SortOrder),
		IncludeCount: req.IncludeCount,
	}

	return filterSet, opts, limit, offset, nil
}

// ParseQueryOptions extracts sorting options from query parameters.
// Used for GET endpoints with query param filters.
func ParseQueryOptions(c *gin.Context) *filter.QueryOptions {
	return &filter.QueryOptions{
		SortBy:       c.DefaultQuery("sort_by", ""),
		SortOrder:    filter.SortOrder(c.DefaultQuery("sort_order", "desc")),
		IncludeCount: c.DefaultQuery("include_count", "") == "true",
	}
}
