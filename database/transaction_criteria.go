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
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetTransactionsByCriteria(ctx context.Context, minAmount, maxAmount *float64, currency *string, minDate, maxDate *time.Time, limit int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTransactionsByCriteria")
	defer span.End()

	query := `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, 
			   currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE 1=1
	`
	var args []interface{}
	argCount := 1

	if minAmount != nil && maxAmount != nil {
		query += fmt.Sprintf(" AND amount >= $%d AND amount <= $%d", argCount, argCount+1)
		args = append(args, *minAmount, *maxAmount)
		argCount += 2
	} else if minAmount != nil {
		query += fmt.Sprintf(" AND amount >= $%d", argCount)
		args = append(args, *minAmount)
		argCount++
	} else if maxAmount != nil {
		query += fmt.Sprintf(" AND amount <= $%d", argCount)
		args = append(args, *maxAmount)
		argCount++
	}

	if currency != nil && *currency != "" {
		query += fmt.Sprintf(" AND currency = $%d", argCount)
		args = append(args, *currency)
		argCount++
	}

	if minDate != nil {
		query += fmt.Sprintf(" AND created_at >= $%d", argCount)
		args = append(args, *minDate)
		argCount++
	}

	if maxDate != nil {
		query += fmt.Sprintf(" AND created_at <= $%d", argCount)
		args = append(args, *maxDate)
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY created_at ASC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := d.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transactions by criteria", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Errorf("Error closing rows: %v", err)
		}
	}()

	transactions := []*model.Transaction{}
	for rows.Next() {
		transaction := &model.Transaction{}
		var metaDataJSON []byte
		var preciseAmountStr string
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&preciseAmountStr,
			&transaction.Precision,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.CreatedAt,
			&metaDataJSON,
			&transaction.ScheduledFor,
			&transaction.Hash,
		)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		if err := json.Unmarshal(metaDataJSON, &transaction.MetaData); err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transaction.PreciseAmount, err = parseBigInt(preciseAmountStr)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to parse precise_amount: %w", err)
		}
		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	span.AddEvent("Transactions by criteria retrieved", trace.WithAttributes(
		attribute.Int("transaction.count", len(transactions)),
	))
	return transactions, nil
}
