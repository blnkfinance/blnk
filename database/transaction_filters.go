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

package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetAllTransactionsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Transaction, error) {
	transactions, _, err := d.GetAllTransactionsWithFilterAndOptions(ctx, filters, nil, limit, offset)
	return transactions, err
}

// GetAllTransactionsWithFilterAndOptions retrieves transactions with filtering, sorting, and optional count.
// It uses the filter package to build SQL WHERE and ORDER BY conditions.
//
// Parameters:
// - ctx: Context for the database operation.
// - filters: A QueryFilterSet containing the filter conditions.
// - opts: Query options including sorting and count settings.
// - limit: The maximum number of transactions to return.
// - offset: The offset to start fetching transactions from (for pagination).
//
// Returns:
// - []model.Transaction: A slice of transactions matching the filter criteria.
// - *int64: Optional total count of matching records (if opts.IncludeCount is true).
// - error: An error if the query fails or if there's an issue processing the results.
func (d Datasource) GetAllTransactionsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Transaction, *int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetAllTransactionsWithFilterAndOptions")
	defer span.End()

	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	if opts == nil {
		opts = &filter.QueryOptions{}
	}
	if err := filter.ValidateSortByForTable(opts, "transactions"); err != nil {
		return nil, nil, apierror.NewAPIError(apierror.ErrBadRequest, "Invalid sort_by field", nil)
	}

	result, err := filter.BuildWithOptions(filters, "transactions", "", 1, opts)
	if err != nil {
		span.RecordError(err)
		return nil, nil, apierror.NewAPIError(apierror.ErrBadRequest, fmt.Sprintf("Invalid filter: %s", err.Error()), err)
	}

	// Determine select fields based on whether count is requested
	selectFields := "transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, hash, created_at, effective_date, meta_data"
	if opts != nil && opts.IncludeCount {
		selectFields += ", COUNT(*) OVER() AS total_count"
	}

	// Build base query
	baseQuery := fmt.Sprintf(`
		SELECT %s
		FROM blnk.transactions
	`, selectFields)

	var args []interface{}
	args = append(args, result.Args...)
	argPos := result.NextArgPos

	// Add WHERE clause if filters are provided
	if len(result.Conditions) > 0 {
		logicalOperator := filter.LogicalAnd
		if filters != nil {
			logicalOperator = filters.LogicalOperator
		}
		baseQuery += " WHERE " + filter.BuildConditionExpression(result.Conditions, logicalOperator)
	}

	// Prepend CTEs if any
	if len(result.CTEs) > 0 {
		baseQuery = "WITH " + strings.Join(result.CTEs, ", ") + " " + baseQuery
	}

	// Add ORDER BY clause
	baseQuery += " ORDER BY " + result.OrderBy

	// Add pagination
	baseQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argPos, argPos+1)
	args = append(args, limit, offset)

	rows, err := d.Conn.QueryContext(ctx, baseQuery, args...)
	if err != nil {
		span.RecordError(err)
		return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transactions", err)
	}
	defer func() { _ = rows.Close() }()

	var transactions []model.Transaction
	var totalCount *int64

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string

		if opts != nil && opts.IncludeCount {
			var count int64
			err = rows.Scan(
				&transaction.TransactionID,
				&transaction.Source,
				&transaction.Reference,
				&transaction.Amount,
				&preciseAmountStr,
				&transaction.Precision,
				&transaction.Currency,
				&transaction.Destination,
				&transaction.Description,
				&transaction.Status,
				&transaction.Hash,
				&transaction.CreatedAt,
				&transaction.EffectiveDate,
				&metaDataJSON,
				&count,
			)
			if totalCount == nil {
				totalCount = &count
			}
		} else {
			err = rows.Scan(
				&transaction.TransactionID,
				&transaction.Source,
				&transaction.Reference,
				&transaction.Amount,
				&preciseAmountStr,
				&transaction.Precision,
				&transaction.Currency,
				&transaction.Destination,
				&transaction.Description,
				&transaction.Status,
				&transaction.Hash,
				&transaction.CreatedAt,
				&transaction.EffectiveDate,
				&metaDataJSON,
			)
		}
		if err != nil {
			span.RecordError(err)
			return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			span.RecordError(err)
			return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transaction.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			span.RecordError(err)
			return nil, nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}

		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	span.AddEvent("Transactions with filter and options retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))

	return transactions, totalCount, nil
}

// GetStuckQueuedTransactions retrieves QUEUED transactions that are older than the threshold
// and have no child transactions, indicating they were never picked up by Redis processing.
