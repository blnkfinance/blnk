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
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/lib/pq"
)

// CreateLedger inserts a new ledger record into the database, ensuring metadata is properly marshaled into JSON format.
// It assigns a unique ledger ID with a suffix and captures the current timestamp as the creation time.
//
// Parameters:
// - ledger: The ledger data to be inserted into the database.
//
// Returns:
// - model.Ledger: The created ledger object including the generated LedgerID and creation timestamp.
// - error: An error if the ledger creation fails, including specific database error handling for conflicts.
func (d Datasource) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	// Marshal the metadata into JSON format
	metaDataJSON, err := json.Marshal(ledger.MetaData)
	if err != nil {
		return model.Ledger{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Assign a unique ledger ID and record the creation time
	ledger.LedgerID = model.GenerateUUIDWithSuffix("ldg")
	ledger.CreatedAt = time.Now()

	// Insert the ledger into the database
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.ledgers (meta_data, name, ledger_id)
		VALUES ($1, $2, $3)
	`, metaDataJSON, ledger.Name, ledger.LedgerID)

	// Handle database errors, specifically unique constraint violations
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok {
			switch pqErr.Code.Name() {
			case "unique_violation":
				return model.Ledger{}, apierror.NewAPIError(apierror.ErrConflict, "Ledger with this name or ID already exists", err)
			default:
				return model.Ledger{}, apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
			}
		}
		return model.Ledger{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create ledger", err)
	}

	return ledger, nil
}

// GetAllLedgers retrieves a paginated list of ledger records from the database, unmarshaling their metadata from JSON format.
// This method supports pagination and can be used to efficiently retrieve all ledgers over multiple requests.
//
// Parameters:
// - limit: The maximum number of ledgers to return (e.g., 20).
// - offset: The offset to start fetching ledgers from (for pagination).
//
// Returns:
// - []model.Ledger: A slice of ledgers retrieved from the database.
// - error: An error if the query fails or if there's an issue processing the results.
func (d Datasource) GetAllLedgers(limit, offset int) ([]model.Ledger, error) {
	if limit <= 0 || limit > 100 {
		limit = 20 // Default limit to 20 if the provided limit is invalid or too large
	}

	// Execute a paginated query to select ledgers from the database
	query := `
		SELECT ledger_id, name, created_at, meta_data
		FROM blnk.ledgers
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := d.Conn.Query(query, limit, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, err.Error(), err)
	}
	defer rows.Close()

	ledgers := []model.Ledger{}

	// Iterate through the query results, scanning each row into a ledger object
	for rows.Next() {
		ledger := model.Ledger{}
		var metaDataJSON []byte
		err = rows.Scan(&ledger.LedgerID, &ledger.Name, &ledger.CreatedAt, &metaDataJSON)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan ledger data", err)
		}

		// Unmarshal the metadata JSON into the ledger's MetaData field
		err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		ledgers = append(ledgers, ledger)
	}

	// Check for any errors that occurred during the iteration of the rows
	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over ledgers", err)
	}

	return ledgers, nil
}

// GetLedgerByID retrieves a ledger record from the database by its ID.
// It handles cases where the ledger is not found and unmarshals the metadata from JSON format.
//
// Parameters:
// - id: The unique ID of the ledger to retrieve.
//
// Returns:
// - *model.Ledger: The ledger object, if found.
// - error: An error if the ledger is not found or if the query fails.
func (d Datasource) GetLedgerByID(id string) (*model.Ledger, error) {
	ledger := model.Ledger{}

	// Query the database to find the ledger by its ID
	row := d.Conn.QueryRow(`
		SELECT ledger_id, name, created_at, meta_data
		FROM blnk.ledgers
		WHERE ledger_id = $1
	`, id)

	var metaDataJSON []byte
	err := row.Scan(&ledger.LedgerID, &ledger.Name, &ledger.CreatedAt, &metaDataJSON)
	if err != nil {
		// Handle case where the ledger is not found
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, "Ledger not found", err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve ledger", err)
	}

	// Unmarshal the metadata JSON into the ledger's MetaData field
	err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	return &ledger, nil
}
