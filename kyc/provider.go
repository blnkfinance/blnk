package kyc

import (
	"context"
	"time"

	"github.com/blnkfinance/blnk/model"
)

type VerificationStatus string

const (
	StatusVerified VerificationStatus = "VERIFIED"
	StatusFailed   VerificationStatus = "FAILED"
	StatusPending  VerificationStatus = "PENDING"
	StatusReview   VerificationStatus = "REVIEW_NEEDED"
)

type VerificationResult struct {
	Status      VerificationStatus     `json:"status"`
	Reason      string                 `json:"reason"`
	ProviderRef string                 `json:"provider_ref"`
	RawData     map[string]interface{} `json:"raw_data"`
	Timestamp   time.Time              `json:"timestamp"`
}

type ProviderAdapter interface {
	Name() string
	VerifyIdentity(ctx context.Context, identity *model.Identity) (*VerificationResult, error)
	VerifyDocument(ctx context.Context, identityID string, docType string, docData []byte) (*VerificationResult, error)
	CheckStatus(ctx context.Context, providerRef string) (*VerificationResult, error)
}
