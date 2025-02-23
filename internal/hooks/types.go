package hooks

import (
	"context"
	"encoding/json"
	"time"
)

type HookType string

const (
	PreTransaction  HookType = "PRE_TRANSACTION"
	PostTransaction HookType = "POST_TRANSACTION"
)

// Hook represents a webhook configuration
type Hook struct {
	ID          string    `json:"id"`           // Unique identifier for the hook
	Name        string    `json:"name"`         // Friendly name for the hook
	URL         string    `json:"url"`          // Webhook endpoint URL
	Type        HookType  `json:"type"`         // Type of hook (pre or post transaction)
	Active      bool      `json:"active"`       // Whether the hook is currently active
	Timeout     int       `json:"timeout"`      // Timeout in seconds for the webhook call
	RetryCount  int       `json:"retry_count"`  // Number of retries on failure
	CreatedAt   time.Time `json:"created_at"`   // Creation timestamp
	LastRun     time.Time `json:"last_run"`     // Last execution timestamp
	LastSuccess bool      `json:"last_success"` // Status of last execution
}

// HookPayload represents the data sent to webhook endpoints
type HookPayload struct {
	TransactionID string          `json:"transaction_id"`
	HookType      HookType        `json:"hook_type"`
	Timestamp     time.Time       `json:"timestamp"`
	Data          json.RawMessage `json:"data,omitempty"` // Changed to omitempty to handle nil data
}

// HookResponse represents the expected response from webhook endpoints
type HookResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// HookManager defines the interface for managing hooks
type HookManager interface {
	RegisterHook(ctx context.Context, hook *Hook) error
	UpdateHook(ctx context.Context, hookID string, hook *Hook) error
	DeleteHook(ctx context.Context, hookID string) error
	GetHook(ctx context.Context, hookID string) (*Hook, error)
	ListHooks(ctx context.Context, hookType HookType) ([]*Hook, error)
	ExecutePreHooks(ctx context.Context, transactionID string, data interface{}) error
	ExecutePostHooks(ctx context.Context, transactionID string, data interface{}) error
}
