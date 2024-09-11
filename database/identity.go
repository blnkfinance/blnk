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

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
)

// CreateIdentity inserts a new identity record into the database.
// It generates a unique IdentityID, sets the creation timestamp, and stores the identity metadata.
// Parameters:
// - identity: The identity object to be inserted.
// Returns:
// - The created identity object, or an error if the creation fails.
func (d Datasource) CreateIdentity(identity model.Identity) (model.Identity, error) {
	// Marshal metadata into JSON format
	metaDataJSON, err := json.Marshal(identity.MetaData)
	if err != nil {
		return identity, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Generate a unique identity ID and set the creation timestamp
	identity.IdentityID = model.GenerateUUIDWithSuffix("idt")
	identity.CreatedAt = time.Now()

	// Insert the identity record into the database
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.identity (identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
	`, identity.IdentityID, identity.IdentityType, identity.FirstName, identity.LastName, identity.OtherNames, identity.Gender, identity.DOB, identity.EmailAddress, identity.PhoneNumber, identity.Nationality, identity.OrganizationName, identity.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, identity.CreatedAt, metaDataJSON)

	// Handle any errors that occur during insertion
	if err != nil {
		return identity, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create identity", err)
	}

	// Return the created identity
	return identity, nil
}

// GetIdentityByID retrieves an identity from the database based on the given identity ID.
// It starts a transaction, executes a query to fetch the identity details, and commits the transaction upon success.
// Parameters:
// - id: The ID of the identity to be retrieved.
// Returns:
// - A pointer to the Identity object if found, or an error if the identity is not found or the query fails.
func (d Datasource) GetIdentityByID(id string) (*model.Identity, error) {
	// Set a timeout for the context and ensure cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Begin a transaction
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	// Query the database for the identity by ID
	row := tx.QueryRow(`
		SELECT identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data
		FROM blnk.identity
		WHERE identity_id = $1
	`, id)

	identity := &model.Identity{}
	var metaDataJSON []byte

	// Scan the row into the identity object
	err = row.Scan(
		&identity.IdentityID, &identity.IdentityType,
		&identity.FirstName, &identity.LastName, &identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress, &identity.PhoneNumber, &identity.Nationality,
		&identity.OrganizationName, &identity.Category,
		&identity.Street, &identity.Country, &identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt, &metaDataJSON,
	)

	// Handle potential errors during the scan
	if err != nil {
		_ = tx.Rollback()
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Identity with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve identity", err)
	}

	// Unmarshal the metadata JSON into the identity's MetaData field
	err = json.Unmarshal(metaDataJSON, &identity.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	// Return the retrieved identity
	return identity, nil
}

// GetAllIdentities retrieves all identities from the database.
// It executes a query to fetch all identity records, parses the result into Identity structs, and handles metadata unmarshalling.
// Returns:
// - A slice of Identity objects if successful, or an error if any operation fails.
func (d Datasource) GetAllIdentities() ([]model.Identity, error) {
	// Execute query to retrieve all identities, ordered by creation date
	rows, err := d.Conn.Query(`
		SELECT identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data
		FROM blnk.identity
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve identities", err)
	}
	defer rows.Close()

	var identities []model.Identity

	// Iterate through the result set
	for rows.Next() {
		identity := model.Identity{}
		var metaDataJSON []byte

		// Scan the row into the identity object
		err = rows.Scan(
			&identity.IdentityID, &identity.IdentityType,
			&identity.FirstName, &identity.LastName, &identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress, &identity.PhoneNumber, &identity.Nationality,
			&identity.OrganizationName, &identity.Category,
			&identity.Street, &identity.Country, &identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt, &metaDataJSON,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan identity data", err)
		}

		// Unmarshal metadata JSON into the MetaData field
		err = json.Unmarshal(metaDataJSON, &identity.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		// Append the identity to the slice
		identities = append(identities, identity)
	}

	// Check for any errors encountered during row iteration
	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over identities", err)
	}

	// Return the slice of identities
	return identities, nil
}

// UpdateIdentity updates a specific identity record in the database.
// It marshals the identity metadata, constructs an SQL update query, and checks the result.
// Parameters:
// - identity: A pointer to the Identity object containing the updated details.
// Returns:
// - An error if the update fails, or nil if successful.
func (d Datasource) UpdateIdentity(identity *model.Identity) error {
	// Marshal the MetaData field into JSON
	metaDataJSON, err := json.Marshal(identity.MetaData)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Execute the SQL update query with the provided identity details
	result, err := d.Conn.Exec(`
		UPDATE blnk.identity
		SET identity_type = $2, first_name = $3, last_name = $4, other_names = $5, gender = $6, dob = $7, email_address = $8, phone_number = $9, nationality = $10, organization_name = $11, category = $12, street = $13, country = $14, state = $15, post_code = $16, city = $17, created_at = $18, meta_data = $19
		WHERE identity_id = $1
	`, identity.IdentityID, identity.IdentityType, identity.FirstName, identity.LastName, identity.OtherNames, identity.Gender, identity.DOB, identity.EmailAddress, identity.PhoneNumber, identity.Nationality, identity.OrganizationName, identity.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, identity.CreatedAt, metaDataJSON)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update identity", err)
	}

	// Check the number of rows affected to ensure the update was applied
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// Return an error if no rows were updated (i.e., the identity was not found)
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Identity with ID '%s' not found", identity.IdentityID), nil)
	}

	return nil
}

// DeleteIdentity deletes a specific identity record from the database.
// It executes the SQL delete query based on the provided identity ID.
// Parameters:
// - id: The ID of the identity to be deleted.
// Returns:
// - An error if the deletion fails, or nil if successful.
func (d Datasource) DeleteIdentity(id string) error {
	// Execute the SQL delete query
	result, err := d.Conn.Exec(`
		DELETE FROM blnk.identity
		WHERE identity_id = $1
	`, id)

	// Handle any errors that occur during execution
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete identity", err)
	}

	// Check how many rows were affected by the delete query
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were deleted, return a "not found" error
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Identity with ID '%s' not found", id), nil)
	}

	return nil
}
