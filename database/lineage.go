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
