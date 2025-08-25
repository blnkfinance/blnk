package blnk

import (
	"context"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// CreateAPIKey creates a new API key for the specified owner
//
// Parameters:
// - ctx: The context for the operation
// - name: Name of the API key
// - ownerID: ID of the key owner
// - scopes: List of permission scopes
// - expiresAt: Expiration time for the key
//
// Returns:
// - *model.APIKey: The created API key
// - error: An error if the operation fails
func (l *Blnk) CreateAPIKey(ctx context.Context, name, ownerID string, scopes []string, expiresAt time.Time) (*model.APIKey, error) {
	return l.datasource.CreateAPIKey(ctx, name, ownerID, scopes, expiresAt)
}

// ListAPIKeys retrieves all API keys for a specific owner
//
// Parameters:
// - ctx: The context for the operation
// - ownerID: ID of the key owner
//
// Returns:
// - []*model.APIKey: List of API keys
// - error: An error if the operation fails
func (l *Blnk) ListAPIKeys(ctx context.Context, ownerID string) ([]*model.APIKey, error) {
	return l.datasource.ListAPIKeys(ctx, ownerID)
}

// RevokeAPIKey revokes an API key if it belongs to the specified owner
//
// Parameters:
// - ctx: The context for the operation
// - id: ID of the API key to revoke
// - ownerID: ID of the key owner
//
// Returns:
// - error: An error if the operation fails
func (l *Blnk) RevokeAPIKey(ctx context.Context, id, ownerID string) error {
	return l.datasource.RevokeAPIKey(ctx, id, ownerID)
}

// GetAPIKeyByKey retrieves an API key by its key string
//
// Parameters:
// - ctx: The context for the operation
// - key: The API key string
//
// Returns:
// - *model.APIKey: The API key if found
// - error: An error if the operation fails
func (l *Blnk) GetAPIKeyByKey(ctx context.Context, key string) (*model.APIKey, error) {
	return l.datasource.GetAPIKey(ctx, key)
}

// UpdateLastUsed updates the last used timestamp of an API key
//
// Parameters:
// - ctx: The context for the operation
// - id: ID of the API key to update
//
// Returns:
// - error: An error if the operation fails
func (l *Blnk) UpdateLastUsed(ctx context.Context, id string) error {
	return l.datasource.UpdateLastUsed(ctx, id)
}
