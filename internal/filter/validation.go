package filter

import (
	"fmt"
	"regexp"
	"strings"
)

var jsonKeyRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

func Validate(filters *QueryFilterSet, table string) error {
	if filters == nil {
		return nil
	}

	validFields := GetValidFieldsForTable(table)
	if len(validFields) == 0 {
		return fmt.Errorf("unsupported table for advanced filtering: %s", table)
	}

	for _, f := range filters.Filters {
		if strings.HasPrefix(f.Field, "meta_data.") && validFields["meta_data"] {
			jsonKey := strings.TrimPrefix(f.Field, "meta_data.")
			if !jsonKeyRegex.MatchString(jsonKey) {
				return fmt.Errorf("invalid JSON key '%s' in field '%s': must match pattern ^[a-zA-Z][a-zA-Z0-9_]*$", jsonKey, f.Field)
			}
			continue
		}

		if !validFields[f.Field] {
			return fmt.Errorf("invalid field '%s' for table '%s'", f.Field, table)
		}
	}

	return nil
}

func GetValidFieldsForTable(table string) map[string]bool {
	switch table {
	case "transactions":
		return map[string]bool{
			"transaction_id":       true,
			"parent_transaction":   true,
			"amount":               true,
			"currency":             true,
			"source":               true,
			"destination":          true,
			"balance_id":           true,
			"reference":            true,
			"status":               true,
			"created_at":           true,
			"effective_date":       true,
			"source_identity":      true,
			"destination_identity": true,
			"indicator":            true,
			"description":          true,
			"precision":            true,
			"meta_data":            true,
		}
	case "balances":
		return map[string]bool{
			"balance_id":              true,
			"ledger_id":               true,
			"identity_id":             true,
			"indicator":               true,
			"currency":                true,
			"balance":                 true,
			"credit_balance":          true,
			"debit_balance":           true,
			"inflight_balance":        true,
			"inflight_credit_balance": true,
			"inflight_debit_balance":  true,
			"created_at":              true,
			"meta_data":               true,
		}
	case "ledgers":
		return map[string]bool{
			"ledger_id":  true,
			"name":       true,
			"created_at": true,
			"meta_data":  true,
		}
	case "identity":
		return map[string]bool{
			"identity_id":       true,
			"first_name":        true,
			"last_name":         true,
			"other_names":       true,
			"gender":            true,
			"dob":               true,
			"email_address":     true,
			"phone_number":      true,
			"nationality":       true,
			"street":            true,
			"country":           true,
			"state":             true,
			"organization_name": true,
			"category":          true,
			"identity_type":     true,
			"post_code":         true,
			"city":              true,
			"created_at":        true,
			"meta_data":         true,
		}
	case "reconciliations":
		return map[string]bool{
			"reconciliation_id":      true,
			"upload_id":              true,
			"status":                 true,
			"matched_transactions":   true,
			"unmatched_transactions": true,
			"started_at":             true,
			"completed_at":           true,
		}
	case "matching_rules":
		return map[string]bool{
			"rule_id":     true,
			"name":        true,
			"description": true,
			"created_at":  true,
			"updated_at":  true,
		}
	case "external_transactions":
		return map[string]bool{
			"id":          true,
			"amount":      true,
			"reference":   true,
			"currency":    true,
			"description": true,
			"date":        true,
			"source":      true,
			"upload_id":   true,
		}
	case "accounts":
		return map[string]bool{
			"account_id":  true,
			"name":        true,
			"number":      true,
			"bank_name":   true,
			"currency":    true,
			"ledger_id":   true,
			"identity_id": true,
			"balance_id":  true,
			"created_at":  true,
			"meta_data":   true,
		}
	default:
		return map[string]bool{}
	}
}

// GetSortableFieldsForTable returns fields that can be sorted.
// All filterable fields are sortable. For optimal performance on large datasets,
// consider adding indexes for frequently sorted fields. See docs/performance-tuning.mdx.
func GetSortableFieldsForTable(table string) map[string]bool {
	return GetValidFieldsForTable(table)
}

// ValidateSortField validates that the sort field is allowed for the table.
func ValidateSortField(sortBy, table string) error {
	if sortBy == "" {
		return nil
	}

	sortableFields := GetSortableFieldsForTable(table)
	if len(sortableFields) == 0 {
		return fmt.Errorf("sorting not supported for table: %s", table)
	}

	if !sortableFields[sortBy] {
		return fmt.Errorf("cannot sort by '%s' for table '%s': field is not filterable", sortBy, table)
	}

	return nil
}

// GetDefaultSortField returns the default sort field for a table.
func GetDefaultSortField(table string) string {
	return "created_at"
}
