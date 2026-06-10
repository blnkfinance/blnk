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
	"encoding/json"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// utcOrNil normalizes an optional timestamp to UTC so the naive value stored
// in timestamp-without-time-zone columns is timezone-independent.
func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func (d Datasource) RecordTransaction(ctx context.Context, txn *model.Transaction) (*model.Transaction, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PersistTransaction")
	defer span.End()

	// Marshal transaction metadata into JSON format
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		span.RecordError(err) // Record the error in the tracing span
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Execute the SQL insert statement to record the transaction
	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.AmountString, txn.PreciseAmount.String(), txn.Precision, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt.UTC(), metaDataJSON, txn.ScheduledFor.UTC(), txn.Hash, utcOrNil(txn.EffectiveDate),
	)
	// Handle errors that may occur during the execution of the query
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	// Log the successful transaction recording as an event in the tracing span
	span.AddEvent("Transaction recorded", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))

	return txn, nil
}

// recordTransactionInTx inserts a transaction record within an existing database transaction.
// This is a helper function used by RecordTransactionWithBalances for atomic operations.
func recordTransactionInTx(ctx context.Context, tx *sql.Tx, txn *model.Transaction) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "recordTransactionInTx")
	defer span.End()

	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.AmountString, txn.PreciseAmount.String(), txn.Precision, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt.UTC(), metaDataJSON, txn.ScheduledFor.UTC(), txn.Hash, utcOrNil(txn.EffectiveDate),
	)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	span.AddEvent("Transaction recorded in transaction", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
	))

	return nil
}

// recordTransactionsInTx inserts multiple transaction records within an existing database
// transaction using PostgreSQL's COPY protocol to reduce SQL parsing and roundtrips.
func recordTransactionsInTx(ctx context.Context, tx *sql.Tx, txns []*model.Transaction) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "recordTransactionsInTx")
	defer span.End()

	if len(txns) == 0 {
		return nil
	}

	stmt, err := tx.PrepareContext(ctx, pq.CopyInSchema(
		"blnk",
		"transactions",
		"transaction_id",
		"parent_transaction",
		"source",
		"reference",
		"amount",
		"precise_amount",
		"precision",
		"currency",
		"destination",
		"description",
		"status",
		"created_at",
		"meta_data",
		"scheduled_for",
		"hash",
		"effective_date",
	))
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to prepare transaction copy", err)
	}

	defer func() {
		_ = stmt.Close()
	}()

	for _, txn := range txns {
		metaDataJSON, err := json.Marshal(txn.MetaData)
		if err != nil {
			span.RecordError(err)
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
		}

		if _, err := stmt.ExecContext(ctx,
			txn.TransactionID,
			txn.ParentTransaction,
			txn.Source,
			txn.Reference,
			txn.AmountString,
			txn.PreciseAmount.String(),
			txn.Precision,
			txn.Currency,
			txn.Destination,
			txn.Description,
			txn.Status,
			txn.CreatedAt.UTC(),
			string(metaDataJSON),
			txn.ScheduledFor.UTC(),
			txn.Hash,
			utcOrNil(txn.EffectiveDate),
		); err != nil {
			span.RecordError(err)
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to stream transaction copy row", err)
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to flush transaction copy", err)
	}

	if err := stmt.Close(); err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to finalize transaction copy", err)
	}

	span.AddEvent("Transactions recorded in transaction", trace.WithAttributes(
		attribute.Int("transaction.count", len(txns)),
	))

	return nil
}

// RecordTransactionWithBalances atomically records a transaction and updates both source and destination balances
// within a single database transaction. This ensures that either all operations succeed together,
// or none of them are committed, preventing inconsistent ledger states.
//
// Parameters:
// - ctx: Context for managing the request and tracing.
// - txn: The transaction object containing details to be recorded.
// - sourceBalance: The source balance to be updated.
// - destinationBalance: The destination balance to be updated.
//
// Returns:
// - The recorded transaction if successful, or an error if any operation fails.
func (d Datasource) RecordTransactionWithBalances(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransactionWithBalances")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := recordTransactionInTx(ctx, tx, txn); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transaction and balances recorded atomically", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("source.balance_id", sourceBalance.BalanceID),
		attribute.String("destination.balance_id", destinationBalance.BalanceID),
	))

	return txn, nil
}

// RecordTransactionWithBalancesAndOutbox atomically records a transaction, updates balances,
// and optionally inserts a lineage outbox entry within a single database transaction.
// This ensures that the lineage processing intent is captured atomically with the main transaction,
// guaranteeing no lineage work is lost even if subsequent async operations fail.
//
// Parameters:
// - ctx: Context for managing the request and tracing.
// - txn: The transaction object containing details to be recorded.
// - sourceBalance: The source balance to be updated.
// - destinationBalance: The destination balance to be updated.
// - outbox: Optional lineage outbox entry to insert atomically (can be nil if no lineage processing needed).
//
// Returns:
// - The recorded transaction if successful, or an error if any operation fails.
func (d Datasource) RecordTransactionWithBalancesAndOutbox(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance, outbox *model.LineageOutbox) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransactionWithBalancesAndOutbox")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := recordTransactionInTx(ctx, tx, txn); err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Insert lineage outbox entry atomically if provided
	if outbox != nil {
		if err := d.InsertLineageOutboxInTx(ctx, tx, outbox); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to insert lineage outbox: %w", err)
		}
		span.AddEvent("Lineage outbox entry inserted", trace.WithAttributes(
			attribute.String("outbox.transaction_id", outbox.TransactionID),
			attribute.String("outbox.lineage_type", outbox.LineageType),
		))
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transaction, balances, and outbox recorded atomically", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("source.balance_id", sourceBalance.BalanceID),
		attribute.String("destination.balance_id", destinationBalance.BalanceID),
		attribute.Bool("outbox.included", outbox != nil),
	))

	return txn, nil
}

// RecordTransactionsWithBalancesAndOutboxes atomically records multiple transactions, updates
// the source and destination balances once, and inserts any lineage outbox entries in the same
// database transaction.
func (d Datasource) RecordTransactionsWithBalancesAndOutboxes(ctx context.Context, txns []*model.Transaction, sourceBalance, destinationBalance *model.Balance, outboxes []*model.LineageOutbox) ([]*model.Transaction, error) {
	return d.RecordTransactionsWithBalanceSetAndOutboxes(ctx, txns, []*model.Balance{sourceBalance, destinationBalance}, outboxes)
}

// RecordTransactionsWithBalanceSetAndOutboxes atomically records multiple transactions, updates
// all changed balances, and inserts any lineage outbox entries in the same database transaction.
func (d Datasource) RecordTransactionsWithBalanceSetAndOutboxes(ctx context.Context, txns []*model.Transaction, balances []*model.Balance, outboxes []*model.LineageOutbox) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransactionsWithBalancesAndOutboxes")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalanceSet(ctx, tx, balances); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := recordTransactionsInTx(ctx, tx, txns); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := insertLineageOutboxesInTx(ctx, tx, outboxes); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to insert lineage outboxes: %w", err)
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transactions, balances, and outboxes recorded atomically", trace.WithAttributes(
		attribute.Int("transaction.count", len(txns)),
		attribute.Int("balance.count", len(balances)),
		attribute.Int("outbox.count", len(outboxes)),
	))

	return txns, nil
}

// GetTransaction retrieves a transaction by its ID from the database.
// It logs the transaction retrieval using OpenTelemetry tracing.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The unique transaction ID.
// Returns:
// - The retrieved transaction if successful, or an error if retrieval fails.
