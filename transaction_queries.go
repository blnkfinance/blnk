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

package blnk

import (
	"context"

	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) GetTransaction(ctx context.Context, TransactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetTransaction")
	defer span.End()

	// Fetch the transaction from the datasource
	transaction, err := l.datasource.GetTransaction(ctx, TransactionID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Transaction retrieved", trace.WithAttributes(attribute.String("transaction.id", TransactionID)))
	return transaction, nil
}

// GetAllTransactions retrieves all transactions from the datasource.
// It starts a tracing span, fetches all transactions, and records relevant events and errors.
//
// Returns:
// - []model.Transaction: A slice of all retrieved Transaction models.
// - error: An error if the transactions could not be retrieved.
func (l *Blnk) GetAllTransactions(limit, offset int) ([]model.Transaction, error) {
	ctx, span := tracer.Start(context.Background(), "GetAllTransactions")
	defer span.End()

	// Fetch all transactions from the datasource
	transactions, err := l.datasource.GetAllTransactions(ctx, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("All transactions retrieved")
	return transactions, nil
}

// GetAllTransactionsWithFilter retrieves transactions using advanced filters.
// It starts a tracing span, fetches transactions matching the filter criteria, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - filters *filter.QueryFilterSet: Filter conditions to apply.
// - limit int: Maximum number of transactions to return.
// - offset int: Offset for pagination.
//
// Returns:
// - []model.Transaction: A slice of Transaction models matching the filter criteria.
// - error: An error if the transactions could not be retrieved.
func (l *Blnk) GetAllTransactionsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetAllTransactionsWithFilter")
	defer span.End()

	transactions, err := l.datasource.GetAllTransactionsWithFilter(ctx, filters, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Transactions with filter retrieved")
	return transactions, nil
}

// GetAllTransactionsWithFilterAndOptions retrieves transactions with advanced filters, sorting, and optional count.
func (l *Blnk) GetAllTransactionsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Transaction, *int64, error) {
	ctx, span := tracer.Start(ctx, "GetAllTransactionsWithFilterAndOptions")
	defer span.End()

	transactions, count, err := l.datasource.GetAllTransactionsWithFilterAndOptions(ctx, filters, opts, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}

	span.AddEvent("Transactions with filter and options retrieved")
	return transactions, count, nil
}

// GetTransactionByRef retrieves a transaction by its reference from the datasource.
// It starts a tracing span, fetches the transaction by reference, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - reference string: The reference of the transaction to be retrieved.
//
// Returns:
// - model.Transaction: The retrieved Transaction model.
// - error: An error if the transaction could not be retrieved.
func (l *Blnk) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "GetTransactionByRef")
	defer span.End()

	// Fetch the transaction by reference from the datasource
	transaction, err := l.datasource.GetTransactionByRef(ctx, reference)
	if err != nil {
		span.RecordError(err)
		return model.Transaction{}, err
	}

	span.AddEvent("Transaction retrieved by reference", trace.WithAttributes(attribute.String("transaction.reference", reference)))
	return transaction, nil
}

// UpdateTransactionStatus updates the status of a transaction by its ID in the datasource.
// It starts a tracing span, updates the transaction status, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the transaction to be updated.
// - status string: The new status to be set for the transaction.
//
// Returns:
// - error: An error if the transaction status could not be updated.
func (l *Blnk) UpdateTransactionStatus(ctx context.Context, id string, status string) error {
	ctx, span := tracer.Start(ctx, "UpdateTransactionStatus")
	defer span.End()

	// Update the transaction status in the datasource
	err := l.datasource.UpdateTransactionStatus(ctx, id, status)
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Transaction status updated", trace.WithAttributes(attribute.String("transaction.id", id), attribute.String("transaction.status", status)))
	return nil
}

// getOriginalTransactionForRefund retrieves the original transaction to be refunded,
// checking both the database and the queue if necessary.
