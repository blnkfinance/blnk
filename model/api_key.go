package model

import (
	"crypto/rand"
	"encoding/base64"
	"time"
)

type APIKey struct {
	APIKeyID   string     `json:"api_key_id" db:"api_key_id"`
	Key        string     `json:"key" db:"key"`
	Name       string     `json:"name" db:"name"`
	OwnerID    string     `json:"owner_id" db:"owner_id"`
	Scopes     []string   `json:"scopes" db:"scopes"`
	ExpiresAt  time.Time  `json:"expires_at" db:"expires_at"`
	CreatedAt  time.Time  `json:"created_at" db:"created_at"`
	LastUsedAt time.Time  `json:"last_used_at" db:"last_used_at"`
	IsRevoked  bool       `json:"is_revoked" db:"is_revoked"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty" db:"revoked_at"`
}

// GenerateKey creates a new secure API key
func GenerateKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// NewAPIKey creates a new API key instance
func NewAPIKey(name, ownerID string, scopes []string, expiresAt time.Time) (*APIKey, error) {
	key, err := GenerateKey()
	if err != nil {
		return nil, err
	}

	return &APIKey{
		APIKeyID:  GenerateUUIDWithSuffix("api_key"),
		Key:       key,
		Name:      name,
		OwnerID:   ownerID,
		Scopes:    scopes,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now(),
	}, nil
}

// IsValid checks if the API key is valid
func (k *APIKey) IsValid() bool {
	now := time.Now()
	return !k.IsRevoked && now.Before(k.ExpiresAt)
}

// HasScope checks if the API key has the required scope
func (k *APIKey) HasScope(scope string) bool {
	for _, s := range k.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}
