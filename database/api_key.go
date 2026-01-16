package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
)

var (
	ErrAPIKeyNotFound = errors.New("api key not found")
	ErrInvalidAPIKey  = errors.New("invalid api key")
)

// hashAPIKey creates a SHA-256 hash of the API key for secure storage
func hashAPIKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// getAPIKeyCacheKey generates a cache key for an API key lookup
// Uses the hashed key to ensure consistent cache keys
func getAPIKeyCacheKey(hashedKey string) string {
	return fmt.Sprintf("api_key:hash:%s", hashedKey)
}

// CreateAPIKey creates a new API key with the specified parameters and stores it in the database.
// The key is hashed before storage for security. The plain text key is returned ONLY during creation
// and should be displayed to the user immediately as it cannot be retrieved later.
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - name: A human-readable name for the API key.
// - ownerID: The ID of the user or entity that owns this API key.
// - scopes: A slice of permission scopes that this API key grants access to.
// - expiresAt: The timestamp when this API key will expire.
//
// Returns:
// - *model.APIKey: The created API key object with the PLAIN TEXT key (store this immediately, it won't be retrievable later).
// - error: An error if the API key creation fails during generation or database insertion.
func (s *Datasource) CreateAPIKey(ctx context.Context, name, ownerID string, scopes []string, expiresAt time.Time) (*model.APIKey, error) {
	// Generate the API key with plain text
	apiKey, err := model.NewAPIKey(name, ownerID, scopes, expiresAt)
	if err != nil {
		return nil, err
	}

	// Store the plain text key to return to the user
	plainTextKey := apiKey.Key

	// Hash the key before storing in database
	hashedKey := hashAPIKey(plainTextKey)
	apiKey.Key = hashedKey

	query := `
		INSERT INTO blnk.api_keys (api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err = s.Conn.ExecContext(ctx, query,
		apiKey.APIKeyID,
		apiKey.Key, // This is now the hashed key
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

	// Return the API key with the PLAIN TEXT key for the user
	// This is the ONLY time they'll see this key
	apiKey.Key = plainTextKey
	return apiKey, nil
}

// GetAPIKey retrieves an API key from the database using its key string.
// The provided key is hashed before comparison with stored hashed keys.
// Results are cached for 5 minutes to improve performance.
// This function is typically used for authentication and authorization purposes.
//
// Parameters:
// - ctx: Context for managing the request lifecycle and cancellation.
// - key: The PLAIN TEXT API key string to authenticate.
//
// Returns:
// - *model.APIKey: The API key object if found (with hashed key, not plain text).
// - error: Returns ErrAPIKeyNotFound if the key doesn't exist, or other database errors if the query fails.
func (s *Datasource) GetAPIKey(ctx context.Context, key string) (*model.APIKey, error) {
	// Hash the incoming key for comparison
	hashedKey := hashAPIKey(key)
	cacheKey := getAPIKeyCacheKey(hashedKey)

	// Try to get from cache first
	var apiKey model.APIKey
	if s.Cache != nil {
		err := s.Cache.Get(ctx, cacheKey, &apiKey)
		if err == nil && apiKey.Key != "" {
			return &apiKey, nil
		}
	}

	// Cache miss - query the database
	query := `
		SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at
		FROM blnk.api_keys
		WHERE key = $1
	`

	var scopes pq.StringArray
	err := s.Conn.QueryRowContext(ctx, query, hashedKey).Scan(
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
		return nil, ErrAPIKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	if s.Cache != nil {
		err = s.Cache.Set(ctx, cacheKey, &apiKey, 5*time.Minute)
		if err != nil {
			log.Printf("Failed to cache API key: %v", err)
		}
	}

	return &apiKey, nil
}

// RevokeAPIKey marks an API key as revoked in the database, preventing its future use.
// The function updates the is_revoked flag to true and sets the revoked_at timestamp.
// It also invalidates the cache entry for the revoked key to ensure immediate effect.
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
	// First, get the API key to retrieve its hashed key for cache invalidation
	getQuery := `
		SELECT key
		FROM blnk.api_keys
		WHERE api_key_id = $1 AND owner_id = $2
	`

	var hashedKey string
	err := s.Conn.QueryRowContext(ctx, getQuery, id, ownerID).Scan(&hashedKey)
	if err == sql.ErrNoRows {
		return ErrAPIKeyNotFound
	}
	if err != nil {
		return err
	}

	// Update the API key to mark it as revoked
	updateQuery := `
		UPDATE blnk.api_keys
		SET is_revoked = true, revoked_at = $1
		WHERE api_key_id = $2 AND owner_id = $3
	`

	result, err := s.Conn.ExecContext(ctx, updateQuery, time.Now(), id, ownerID)
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

	// Invalidate the cache entry for this API key
	cacheKey := getAPIKeyCacheKey(hashedKey)
	if s.Cache != nil {
		err = s.Cache.Delete(ctx, cacheKey)
		if err != nil {
			// Log the error, but don't fail the revocation
			log.Printf("Failed to invalidate API key cache: %v", err)
		}
	}

	return nil
}

// UpdateLastUsed updates the last_used_at timestamp for an API key to the current time.
// This function is typically called during authentication to track API key usage patterns.
// Note: This does NOT invalidate the cache to avoid performance overhead on every request.
// The cached version will still work for authentication, and the timestamp will be updated
// in the database for auditing purposes.
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
// Note: The Key field will contain hashed values. For user display, show only a preview
// (e.g., last 4 characters of the key ID or a masked version).
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
	defer func() { _ = rows.Close() }()

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
