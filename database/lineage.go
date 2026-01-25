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
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
)

// UpsertLineageMapping creates or updates a lineage mapping between a main balance and its shadow balances.
// If a mapping already exists for the same balance_id and provider, it updates the existing record.
func (d Datasource) UpsertLineageMapping(ctx context.Context, mapping model.LineageMapping) error {
	_, err := d.Conn.ExecContext(ctx, `
		INSERT INTO blnk.lineage_mappings (balance_id, provider, shadow_balance_id, aggregate_balance_id, identity_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (balance_id, provider) DO UPDATE SET
			shadow_balance_id = EXCLUDED.shadow_balance_id,
			aggregate_balance_id = EXCLUDED.aggregate_balance_id
	`, mapping.BalanceID, mapping.Provider, mapping.ShadowBalanceID, mapping.AggregateBalanceID, mapping.IdentityID, time.Now())
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok {
			switch pqErr.Code.Name() {
			case "foreign_key_violation":
				return apierror.NewAPIError(apierror.ErrBadRequest, "Invalid balance or identity ID", err)
			default:
				return apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
			}
		}
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to upsert lineage mapping", err)
	}
	return nil
}

// GetLineageMappings retrieves all lineage mappings for a given balance ID.
func (d Datasource) GetLineageMappings(ctx context.Context, balanceID string) ([]model.LineageMapping, error) {
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, balance_id, provider, shadow_balance_id, aggregate_balance_id, identity_id, created_at
		FROM blnk.lineage_mappings
		WHERE balance_id = $1
		ORDER BY created_at ASC
	`, balanceID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve lineage mappings", err)
	}
	defer func() { _ = rows.Close() }()

	var mappings []model.LineageMapping
	for rows.Next() {
		var mapping model.LineageMapping
		err := rows.Scan(
			&mapping.ID,
			&mapping.BalanceID,
			&mapping.Provider,
			&mapping.ShadowBalanceID,
			&mapping.AggregateBalanceID,
			&mapping.IdentityID,
			&mapping.CreatedAt,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan lineage mapping", err)
		}
		mappings = append(mappings, mapping)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over lineage mappings", err)
	}

	return mappings, nil
}

// GetLineageMappingByProvider retrieves a specific lineage mapping for a balance and provider.
func (d Datasource) GetLineageMappingByProvider(ctx context.Context, balanceID, provider string) (*model.LineageMapping, error) {
	row := d.Conn.QueryRowContext(ctx, `
		SELECT id, balance_id, provider, shadow_balance_id, aggregate_balance_id, identity_id, created_at
		FROM blnk.lineage_mappings
		WHERE balance_id = $1 AND provider = $2
	`, balanceID, provider)

	var mapping model.LineageMapping
	err := row.Scan(
		&mapping.ID,
		&mapping.BalanceID,
		&mapping.Provider,
		&mapping.ShadowBalanceID,
		&mapping.AggregateBalanceID,
		&mapping.IdentityID,
		&mapping.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve lineage mapping", err)
	}

	return &mapping, nil
}

// DeleteLineageMapping deletes a lineage mapping by its ID.
func (d Datasource) DeleteLineageMapping(ctx context.Context, id int64) error {
	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.lineage_mappings WHERE id = $1
	`, id)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete lineage mapping", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, "Lineage mapping not found", nil)
	}

	return nil
}

// InsertLineageOutboxInTx inserts a lineage outbox entry within an existing database transaction.
// This ensures the outbox entry is committed atomically with the main transaction.
func (d Datasource) InsertLineageOutboxInTx(ctx context.Context, tx *sql.Tx, outbox *model.LineageOutbox) error {
	query := `
		INSERT INTO blnk.lineage_outbox
		(transaction_id, source_balance_id, destination_balance_id, provider, lineage_type, payload, status, max_attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`
	err := tx.QueryRowContext(ctx, query,
		outbox.TransactionID,
		outbox.SourceBalanceID,
		outbox.DestinationBalanceID,
		outbox.Provider,
		outbox.LineageType,
		outbox.Payload,
		model.OutboxStatusPending,
		outbox.MaxAttempts,
		time.Now(),
	).Scan(&outbox.ID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert lineage outbox entry", err)
	}
	return nil
}

// ClaimPendingOutboxEntries claims a batch of pending outbox entries for processing.
// It uses SELECT FOR UPDATE SKIP LOCKED to allow concurrent processors.
func (d Datasource) ClaimPendingOutboxEntries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.LineageOutbox, error) {
	lockedUntil := time.Now().Add(lockDuration)

	query := `
		UPDATE blnk.lineage_outbox
		SET status = $1, locked_until = $2
		WHERE id IN (
			SELECT id FROM blnk.lineage_outbox
			WHERE status = 'pending'
			  AND (locked_until IS NULL OR locked_until < NOW())
			  AND attempts < max_attempts
			ORDER BY created_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, transaction_id, source_balance_id, destination_balance_id, provider, lineage_type, payload, status, attempts, max_attempts, last_error, created_at, processed_at, locked_until
	`

	rows, err := d.Conn.QueryContext(ctx, query, model.OutboxStatusProcessing, lockedUntil, batchSize)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to claim pending outbox entries", err)
	}
	defer func() { _ = rows.Close() }()

	var entries []model.LineageOutbox
	for rows.Next() {
		var entry model.LineageOutbox
		var sourceBalanceID, destinationBalanceID, provider, lastError sql.NullString
		var processedAt, lockedUntilVal sql.NullTime

		err := rows.Scan(
			&entry.ID,
			&entry.TransactionID,
			&sourceBalanceID,
			&destinationBalanceID,
			&provider,
			&entry.LineageType,
			&entry.Payload,
			&entry.Status,
			&entry.Attempts,
			&entry.MaxAttempts,
			&lastError,
			&entry.CreatedAt,
			&processedAt,
			&lockedUntilVal,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan outbox entry", err)
		}

		entry.SourceBalanceID = sourceBalanceID.String
		entry.DestinationBalanceID = destinationBalanceID.String
		entry.Provider = provider.String
		entry.LastError = lastError.String
		if processedAt.Valid {
			entry.ProcessedAt = &processedAt.Time
		}
		if lockedUntilVal.Valid {
			entry.LockedUntil = &lockedUntilVal.Time
		}

		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over outbox entries", err)
	}

	return entries, nil
}

// MarkOutboxCompleted marks an outbox entry as completed.
func (d Datasource) MarkOutboxCompleted(ctx context.Context, id int64) error {
	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.lineage_outbox
		SET status = $1, processed_at = NOW(), locked_until = NULL
		WHERE id = $2
	`, model.OutboxStatusCompleted, id)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark outbox entry as completed", err)
	}
	return nil
}

// MarkOutboxFailed marks an outbox entry as failed and increments the attempt counter.
// If attempts exceed max_attempts, the status becomes 'failed', otherwise it returns to 'pending' for retry.
func (d Datasource) MarkOutboxFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.lineage_outbox
		SET status = CASE WHEN attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			locked_until = NULL
		WHERE id = $4
	`, model.OutboxStatusFailed, model.OutboxStatusPending, errMsg, id)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark outbox entry as failed", err)
	}
	return nil
}

// GetOutboxByTransactionID retrieves an outbox entry by its transaction ID.
func (d Datasource) GetOutboxByTransactionID(ctx context.Context, transactionID string) (*model.LineageOutbox, error) {
	row := d.Conn.QueryRowContext(ctx, `
		SELECT id, transaction_id, source_balance_id, destination_balance_id, provider, lineage_type, payload, status, attempts, max_attempts, last_error, created_at, processed_at, locked_until
		FROM blnk.lineage_outbox
		WHERE transaction_id = $1
	`, transactionID)

	var entry model.LineageOutbox
	var sourceBalanceID, destinationBalanceID, provider, lastError sql.NullString
	var processedAt, lockedUntil sql.NullTime

	err := row.Scan(
		&entry.ID,
		&entry.TransactionID,
		&sourceBalanceID,
		&destinationBalanceID,
		&provider,
		&entry.LineageType,
		&entry.Payload,
		&entry.Status,
		&entry.Attempts,
		&entry.MaxAttempts,
		&lastError,
		&entry.CreatedAt,
		&processedAt,
		&lockedUntil,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve outbox entry", err)
	}

	entry.SourceBalanceID = sourceBalanceID.String
	entry.DestinationBalanceID = destinationBalanceID.String
	entry.Provider = provider.String
	entry.LastError = lastError.String
	if processedAt.Valid {
		entry.ProcessedAt = &processedAt.Time
	}
	if lockedUntil.Valid {
		entry.LockedUntil = &lockedUntil.Time
	}

	return &entry, nil
}
