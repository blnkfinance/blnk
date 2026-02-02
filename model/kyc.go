package model

import (
	"time"
)

// KYCWorkflowStatus represents the overall status of a KYC workflow
type KYCWorkflowStatus string

const (
	KYCStatusPending    KYCWorkflowStatus = "PENDING"
	KYCStatusInProgress KYCWorkflowStatus = "IN_PROGRESS"
	KYCStatusApproved   KYCWorkflowStatus = "APPROVED"
	KYCStatusRejected   KYCWorkflowStatus = "REJECTED"
	KYCStatusReview     KYCWorkflowStatus = "REVIEW_NEEDED"
)

// KYCWorkflow represents a long-running KYC process for an identity
type KYCWorkflow struct {
	WorkflowID     string                 `json:"workflow_id"`
	IdentityID     string                 `json:"identity_id"`
	Status         KYCWorkflowStatus      `json:"status"`
	CurrentStep    string                 `json:"current_step"` // CheckID of the current active step
	ProviderID     string                 `json:"provider_id"`  // The provider handling this workflow
	DecisionReason string                 `json:"decision_reason"`
	Steps          []KYCStep              `json:"steps" gorm:"-"` // Not persisted directly in workflow table
	MetaData       map[string]interface{} `json:"meta_data"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

// KYCStepStatus represents the status of a specific verification step
type KYCStepStatus string

const (
	StepStatusPending   KYCStepStatus = "PENDING"
	StepStatusSubmitted KYCStepStatus = "SUBMITTED" // Sent to provider
	StepStatusPassed    KYCStepStatus = "PASSED"
	StepStatusFailed    KYCStepStatus = "FAILED"
	StepStatusRetry     KYCStepStatus = "RETRY_NEEDED"
)

// KYCStep represents a single check within a workflow (e.g., "ID Document Check")
type KYCStep struct {
	StepID      string                 `json:"step_id"`
	WorkflowID  string                 `json:"workflow_id"`
	StepName    string                 `json:"step_name"` // e.g., "document_verification", "liveness_check"
	Status      KYCStepStatus          `json:"status"`
	ProviderRef string                 `json:"provider_ref"` // Reference ID from the external provider
	Result      map[string]interface{} `json:"result"`
	RetryCount  int                    `json:"retry_count"`
	MaxRetries  int                    `json:"max_retries"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
}
