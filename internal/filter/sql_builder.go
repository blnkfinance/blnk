package filter

import (
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

func Build(filters *QueryFilterSet, table string, alias string, startArgPos int) (*BuildResult, error) {
	if filters == nil || len(filters.Filters) == 0 {
		return &BuildResult{
			CTEs:       nil,
			Conditions: []string{},
			Args:       []interface{}{},
			NextArgPos: startArgPos,
		}, nil
	}

	if err := Validate(filters, table); err != nil {
		return nil, err
	}

	result := &BuildResult{
		CTEs:       nil,
		Conditions: make([]string, 0, len(filters.Filters)),
		Args:       make([]interface{}, 0),
		NextArgPos: startArgPos,
	}

	argPos := startArgPos

	for _, f := range filters.Filters {
		var cond string
		var args []interface{}
		var nextArgPos int
		var ctes []string

		if strings.Contains(f.Field, ".") && strings.HasPrefix(f.Field, "meta_data") {
			cond, args, nextArgPos = buildJSONPathCondition(f, alias, argPos)
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
				result.Args = append(result.Args, args...)
				argPos = nextArgPos
			}
			continue
		}

		if f.Field == "balance_id" && table == "transactions" {
			cond, args, nextArgPos, ctes = buildBalanceIdCondition(f, alias, argPos)
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
				result.Args = append(result.Args, args...)
				argPos = nextArgPos
				if len(ctes) > 0 {
					result.CTEs = append(result.CTEs, ctes...)
				}
			}
			continue
		}

		if f.Field == "indicator" && table == "transactions" {
			cond, args, nextArgPos, ctes = buildIndicatorCondition(f, alias, argPos)
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
				result.Args = append(result.Args, args...)
				argPos = nextArgPos
				if len(ctes) > 0 {
					result.CTEs = append(result.CTEs, ctes...)
				}
			}
			continue
		}

		cond, args, nextArgPos = buildStandardCondition(f, table, alias, argPos)
		if cond != "" {
			result.Conditions = append(result.Conditions, cond)
			result.Args = append(result.Args, args...)
			argPos = nextArgPos
		}
	}

	result.NextArgPos = argPos
	return result, nil
}

func buildStandardCondition(f QueryFilter, table string, tableAlias string, argPosition int) (condition string, args []interface{}, newArgPosition int) {
	// Resolve field to safe column name (breaks taint chain for static analyzers)
	safeField := safeColumnForTableAndField(table, f.Field)
	if safeField == "" {
		return "", nil, argPosition
	}

	fieldName := safeField
	if tableAlias != "" {
		fieldName = fmt.Sprintf("%s.%s", tableAlias, safeField)
	}

	switch f.Operator {
	case OpEqual:
		if tsVal, ok := f.Value.(TimestampValue); ok {
			floor, ceiling := computeTimestampRange(tsVal)
			condition = fmt.Sprintf("%s >= $%d AND %s < $%d", fieldName, argPosition, fieldName, argPosition+1)
			args = []interface{}{floor, ceiling}
			newArgPosition = argPosition + 2
			return
		}
		if timeVal, ok := f.Value.(time.Time); ok {
			tsVal := TimestampValue{Time: timeVal, Precision: getDatePrecisionFromTime(timeVal)}
			floor, ceiling := computeTimestampRange(tsVal)
			condition = fmt.Sprintf("%s >= $%d AND %s < $%d", fieldName, argPosition, fieldName, argPosition+1)
			args = []interface{}{floor, ceiling}
			newArgPosition = argPosition + 2
			return
		}

		condition = fmt.Sprintf("%s = $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpNotEqual:
		condition = fmt.Sprintf("%s != $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThan:
		condition = fmt.Sprintf("%s > $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpGreaterThanOrEqual:
		condition = fmt.Sprintf("%s >= $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThan:
		condition = fmt.Sprintf("%s < $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLessThanOrEqual:
		condition = fmt.Sprintf("%s <= $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpLike:
		condition = fmt.Sprintf("%s LIKE $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpILike:
		condition = fmt.Sprintf("%s ILIKE $%d", fieldName, argPosition)
		args = []interface{}{extractValueForSQL(f.Value)}
		newArgPosition = argPosition + 1

	case OpIn:
		if len(f.Values) > 0 {
			if isStringArray(f.Values) {
				condition = fmt.Sprintf("%s = ANY($%d)", fieldName, argPosition)
				args = []interface{}{pq.Array(convertToStringArray(f.Values))}
			} else {
				placeholders := make([]string, len(f.Values))
				args = make([]interface{}, len(f.Values))
				for i, val := range f.Values {
					placeholders[i] = fmt.Sprintf("$%d", argPosition+i)
					args[i] = extractValueForSQL(val)
				}
				condition = fmt.Sprintf("%s IN (%s)", fieldName, strings.Join(placeholders, ", "))
			}
			newArgPosition = argPosition + len(f.Values)
		}

	case OpBetween:
		if len(f.Values) == 2 {
			condition = fmt.Sprintf("%s BETWEEN $%d AND $%d", fieldName, argPosition, argPosition+1)
			args = []interface{}{extractValueForSQL(f.Values[0]), extractValueForSQL(f.Values[1])}
			newArgPosition = argPosition + 2
		}

	case OpIsNull:
		condition = fmt.Sprintf("%s IS NULL", fieldName)
		args = []interface{}{}
		newArgPosition = argPosition

	case OpIsNotNull:
		condition = fmt.Sprintf("%s IS NOT NULL", fieldName)
		args = []interface{}{}
		newArgPosition = argPosition

	default:
		return "", nil, argPosition
	}

	return condition, args, newArgPosition
}

// BuildWithOptions builds filter conditions and includes sorting options.
// It validates both filters and sort options, returning an error if either is invalid.
func BuildWithOptions(filters *QueryFilterSet, table string, alias string, startArgPos int, opts *QueryOptions) (*BuildResult, error) {
	// First build the filter conditions
	result, err := Build(filters, table, alias, startArgPos)
	if err != nil {
		return nil, err
	}

	order := SortDesc
	sortBy := ""
	if opts != nil {
		order = opts.DefaultSortOrder()
		sortBy = opts.SortBy
		if sortBy != "" {
			if err := ValidateSortField(sortBy, table); err != nil {
				return nil, err
			}
		}
	}
	result.OrderBy = BuildOrderBy(sortBy, order, table, alias)

	return result, nil
}

// ResolveSortField maps a requested sort field to a safe column name.
// It returns only string literals from a switch; user input selects which constant to return.
// This breaks the taint chain for static analyzers.
func ResolveSortField(table, sortBy string) string {
	normalized := strings.ToLower(strings.TrimSpace(sortBy))
	if normalized == "" {
		return GetDefaultSortField(table)
	}
	allowed := GetValidFieldsForTable(table)
	if allowed == nil || !allowed[normalized] {
		return GetDefaultSortField(table)
	}
	return safeColumnForSort(table, normalized)
}

// safeColumnForTableAndField maps a field name to a safe column name using only string literals.
// Returns empty string for unknown fields to break the taint chain for static analyzers.
func safeColumnForTableAndField(table, logicalName string) string {
	switch table {
	case "transactions":
		switch logicalName {
		case "transaction_id": return "transaction_id"
		case "parent_transaction": return "parent_transaction"
		case "amount": return "amount"
		case "currency": return "currency"
		case "source": return "source"
		case "destination": return "destination"
		case "balance_id": return "balance_id"
		case "reference": return "reference"
		case "status": return "status"
		case "created_at": return "created_at"
		case "effective_date": return "effective_date"
		case "source_identity": return "source_identity"
		case "destination_identity": return "destination_identity"
		case "indicator": return "indicator"
		case "description": return "description"
		case "precision": return "precision"
		case "meta_data": return "meta_data"
		}
	case "balances":
		switch logicalName {
		case "balance_id": return "balance_id"
		case "ledger_id": return "ledger_id"
		case "identity_id": return "identity_id"
		case "indicator": return "indicator"
		case "currency": return "currency"
		case "balance": return "balance"
		case "credit_balance": return "credit_balance"
		case "debit_balance": return "debit_balance"
		case "inflight_balance": return "inflight_balance"
		case "inflight_credit_balance": return "inflight_credit_balance"
		case "inflight_debit_balance": return "inflight_debit_balance"
		case "created_at": return "created_at"
		case "meta_data": return "meta_data"
		}
	case "ledgers":
		switch logicalName {
		case "ledger_id": return "ledger_id"
		case "name": return "name"
		case "created_at": return "created_at"
		case "meta_data": return "meta_data"
		}
	case "identity":
		switch logicalName {
		case "identity_id": return "identity_id"
		case "first_name": return "first_name"
		case "last_name": return "last_name"
		case "other_names": return "other_names"
		case "gender": return "gender"
		case "dob": return "dob"
		case "email_address": return "email_address"
		case "phone_number": return "phone_number"
		case "nationality": return "nationality"
		case "street": return "street"
		case "country": return "country"
		case "state": return "state"
		case "organization_name": return "organization_name"
		case "category": return "category"
		case "identity_type": return "identity_type"
		case "post_code": return "post_code"
		case "city": return "city"
		case "created_at": return "created_at"
		case "meta_data": return "meta_data"
		}
	case "accounts":
		switch logicalName {
		case "account_id": return "account_id"
		case "name": return "name"
		case "number": return "number"
		case "bank_name": return "bank_name"
		case "currency": return "currency"
		case "ledger_id": return "ledger_id"
		case "identity_id": return "identity_id"
		case "balance_id": return "balance_id"
		case "created_at": return "created_at"
		case "meta_data": return "meta_data"
		}
	case "reconciliations":
		switch logicalName {
		case "reconciliation_id": return "reconciliation_id"
		case "upload_id": return "upload_id"
		case "status": return "status"
		case "matched_transactions": return "matched_transactions"
		case "unmatched_transactions": return "unmatched_transactions"
		case "started_at": return "started_at"
		case "completed_at": return "completed_at"
		}
	case "matching_rules":
		switch logicalName {
		case "rule_id": return "rule_id"
		case "name": return "name"
		case "description": return "description"
		case "created_at": return "created_at"
		case "updated_at": return "updated_at"
		}
	case "external_transactions":
		switch logicalName {
		case "id": return "id"
		case "amount": return "amount"
		case "reference": return "reference"
		case "currency": return "currency"
		case "description": return "description"
		case "date": return "date"
		case "source": return "source"
		case "upload_id": return "upload_id"
		}
	}
	return ""
}

// safeColumnForSort returns a safe column for sorting, with fallback to default.
func safeColumnForSort(table, logicalName string) string {
	if col := safeColumnForTableAndField(table, logicalName); col != "" {
		return col
	}
	return GetDefaultSortField(table)
}

// BuildOrderBy constructs an ORDER BY clause using only safe, constant column names.
func BuildOrderBy(sortBy string, sortOrder SortOrder, table string, alias string) string {
	safeField := ResolveSortField(table, sortBy)
	fieldName := safeField
	if alias != "" {
		fieldName = fmt.Sprintf("%s.%s", alias, safeField)
	}

	direction := "DESC"
	if sortOrder == SortAsc {
		direction = "ASC"
	}

	return fmt.Sprintf("%s %s", fieldName, direction)
}
