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
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
)

var (
	ErrAPIKeyNotFound = errors.New("api key not found")
	ErrInvalidAPIKey  = errors.New("invalid api key")
)

// CreateAPIKey creates a new API key with the specified parameters and stores it in the database.
// It generates a unique API key ID and secure key string, then inserts the record with the provided metadata.
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - name: A human-readable name for the API key.
// - ownerID: The ID of the user or entity that owns this API key.
// - scopes: A slice of permission scopes that this API key grants access to.
// - expiresAt: The timestamp when this API key will expire.
//
// Returns:
// - *model.APIKey: The created API key object with generated ID and key string.
// - error: An error if the API key creation fails during generation or database insertion.
func (s *Datasource) CreateAPIKey(ctx context.Context, name, ownerID string, scopes []string, expiresAt time.Time) (*model.APIKey, error) {
	apiKey, err := model.NewAPIKey(name, ownerID, scopes, expiresAt)
	if err != nil {
		return nil, err
	}

	query := `
		INSERT INTO blnk.api_keys (api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err = s.Conn.ExecContext(ctx, query,
		apiKey.APIKeyID,
		apiKey.Key,
		apiKey.Name,
		apiKey.OwnerID,
		pq.StringArray(apiKey.Scopes),
		apiKey.ExpiresAt,
		apiKey.CreatedAt,
		apiKey.LastUsedAt,
		apiKey.IsRevoked,
	)
	if err != nil {
		return nil, err
	}

	return apiKey, nil
}

// GetAPIKey retrieves an API key from the database using its key string.
// This function is typically used for authentication and authorization purposes.
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - key: The API key string to search for in the database.
//
// Returns:
// - *model.APIKey: The API key object if found, including all metadata and status information.
// - error: Returns ErrAPIKeyNotFound if the key doesn't exist, or other database errors if the query fails.
func (s *Datasource) GetAPIKey(ctx context.Context, key string) (*model.APIKey, error) {
	query := `
		SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at
		FROM blnk.api_keys
		WHERE key = $1
	`

	apiKey := &model.APIKey{}
	var scopes pq.StringArray
	err := s.Conn.QueryRowContext(ctx, query, key).Scan(
		&apiKey.APIKeyID,
		&apiKey.Key,
		&apiKey.Name,
		&apiKey.OwnerID,
		&scopes,
		&apiKey.ExpiresAt,
		&apiKey.CreatedAt,
		&apiKey.LastUsedAt,
		&apiKey.IsRevoked,
		&apiKey.RevokedAt,
	)
	apiKey.Scopes = []string(scopes)

	if err == sql.ErrNoRows {
		fmt.Println("API key not found", key)
		return nil, ErrAPIKeyNotFound
	}
	if err != nil {
		fmt.Println("Error getting API key:", err)
		return nil, err
	}

	return apiKey, nil
}

// RevokeAPIKey marks an API key as revoked in the database, preventing its future use.
// The function updates the is_revoked flag to true and sets the revoked_at timestamp.
// Only the owner of the API key can revoke it.
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - id: The unique identifier of the API key to revoke.
// - ownerID: The ID of the owner, used to ensure only the owner can revoke their keys.
//
// Returns:
//   - error: Returns ErrAPIKeyNotFound if the key doesn't exist or doesn't belong to the owner,
//     or other database errors if the update operation fails.
func (s *Datasource) RevokeAPIKey(ctx context.Context, id, ownerID string) error {
	query := `
		UPDATE blnk.api_keys
		SET is_revoked = true, revoked_at = $1
		WHERE api_key_id = $2 AND owner_id = $3
	`

	result, err := s.Conn.ExecContext(ctx, query, time.Now(), id, ownerID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return ErrAPIKeyNotFound
	}

	return nil
}

// UpdateLastUsed updates the last_used_at timestamp for an API key to the current time.
// This function is typically called during authentication to track API key usage patterns.
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - id: The unique identifier of the API key to update.
//
// Returns:
// - error: An error if the database update operation fails.
func (s *Datasource) UpdateLastUsed(ctx context.Context, id string) error {
	query := `
		UPDATE blnk.api_keys
		SET last_used_at = $1
		WHERE api_key_id = $2
	`

	_, err := s.Conn.ExecContext(ctx, query, time.Now(), id)
	return err
}

// ListAPIKeys retrieves all API keys belonging to a specific owner from the database.
// The results are ordered by creation date in descending order (newest first).
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - ownerID: The ID of the owner whose API keys should be retrieved.
//
// Returns:
// - []*model.APIKey: A slice of API key objects belonging to the specified owner.
// - error: An error if the database query fails or if there are issues scanning the results.
func (s *Datasource) ListAPIKeys(ctx context.Context, ownerID string) ([]*model.APIKey, error) {
	query := `
		SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at
		FROM blnk.api_keys
		WHERE owner_id = $1
		ORDER BY created_at DESC
	`

	rows, err := s.Conn.QueryContext(ctx, query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var apiKeys []*model.APIKey
	for rows.Next() {
		apiKey := &model.APIKey{}
		var scopes pq.StringArray
		err := rows.Scan(
			&apiKey.APIKeyID,
			&apiKey.Key,
			&apiKey.Name,
			&apiKey.OwnerID,
			&scopes,
			&apiKey.ExpiresAt,
			&apiKey.CreatedAt,
			&apiKey.LastUsedAt,
			&apiKey.IsRevoked,
			&apiKey.RevokedAt,
		)
		apiKey.Scopes = []string(scopes)
		if err != nil {
			return nil, err
		}
		apiKeys = append(apiKeys, apiKey)
	}

	return apiKeys, nil
}
