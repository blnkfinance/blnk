package model

import (
	"encoding/json"
	"time"
)

// Outbox status constants
const (
	OutboxStatusPending    = "pending"
	OutboxStatusProcessing = "processing"
	OutboxStatusCompleted  = "completed"
	OutboxStatusFailed     = "failed"
)

// LineageOutbox represents a pending lineage processing task.
// It is inserted atomically with the main transaction to ensure no lineage work is lost.
type LineageOutbox struct {
	ID                   int64           `json:"id"`
	TransactionID        string          `json:"transaction_id"`
	SourceBalanceID      string          `json:"source_balance_id,omitempty"`
	DestinationBalanceID string          `json:"destination_balance_id,omitempty"`
	Provider             string          `json:"provider,omitempty"`
	LineageType          string          `json:"lineage_type"` // "credit", "debit", "both"
	Payload              json.RawMessage `json:"payload"`
	Status               string          `json:"status"`
	Attempts             int             `json:"attempts"`
	MaxAttempts          int             `json:"max_attempts"`
	LastError            string          `json:"last_error,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	ProcessedAt          *time.Time      `json:"processed_at,omitempty"`
	LockedUntil          *time.Time      `json:"locked_until,omitempty"`
}
