package model

import "time"

type CreateAPIKeyRequest struct {
	Name      string    `json:"name" binding:"required"`
	Scopes    []string  `json:"scopes" binding:"required"`
	Owner     string    `json:"owner" binding:"required"`
	ExpiresAt time.Time `json:"expires_at" binding:"required"`
}
