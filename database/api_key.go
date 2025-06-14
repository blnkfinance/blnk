package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jerry-enebeli/blnk/model"
	"github.com/lib/pq"
)

var (
	ErrAPIKeyNotFound = errors.New("api key not found")
	ErrInvalidAPIKey  = errors.New("invalid api key")
)

// CreateAPIKey creates a new API key
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

// GetAPIKey retrieves an API key by its key string
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

// RevokeAPIKey revokes an API key
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

// UpdateLastUsed updates the last_used_at timestamp for an API key
func (s *Datasource) UpdateLastUsed(ctx context.Context, id string) error {
	query := `
		UPDATE blnk.api_keys
		SET last_used_at = $1
		WHERE id = $2
	`

	_, err := s.Conn.ExecContext(ctx, query, time.Now(), id)
	return err
}

// ListAPIKeys lists all API keys for an owner
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
