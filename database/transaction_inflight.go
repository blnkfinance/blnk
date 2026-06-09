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
	"database/sql"
	"errors"
	"math/big"

	"github.com/blnkfinance/blnk/internal/apierror"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) GetTotalCommittedTransactions(ctx context.Context, parentID string) (*big.Int, error) {
	// Start a new tracing span for the operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetTotalCommittedTransactions")
	defer span.End()

	// SQL query to calculate the total precise amount for the given parent transaction
	query := `
		SELECT SUM(precise_amount) AS total_amount
		FROM blnk.transactions
		WHERE parent_transaction = $1 AND status = 'APPLIED'
		GROUP BY parent_transaction;
	`

	// Initialize the variable to store the total amount
	var totalAmountStr string

	// Execute the query and scan the result into totalAmount
	err := d.Conn.QueryRowContext(ctx, query, parentID).Scan(&totalAmountStr)
	if err != nil {
		// If no rows are found, return 0 without error
		if errors.Is(err, sql.ErrNoRows) {
			return new(big.Int), nil
		}
		// Record the error in the tracing span and return the error
		span.RecordError(err)
		return big.NewInt(0), apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get total committed transactions", err)
	}

	total, ok := new(big.Int).SetString(totalAmountStr, 10)
	if !ok {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to parse precise amount", nil)
	}

	// Log the successful retrieval of the total amount
	span.AddEvent("Total committed transactions retrieved", trace.WithAttributes(
		attribute.String("parent_transaction.id", parentID),
		attribute.String("total_amount", total.String()),
	))

	// Return the total amount
	return total, nil
}

// GetTransactionsPaginated retrieves a batch of transactions from the database with pagination support and caches the result.
// If the data is found in cache, it is returned from there; otherwise, it is fetched from the database and then cached.
// Parameters:
// - ctx: Context for managing request and tracing.
// - batchSize: Number of transactions to retrieve in one batch.
// - offset: Number of transactions to skip before retrieving the batch.
// Returns:
// - A slice of transactions, or an error if the retrieval or caching fails.
