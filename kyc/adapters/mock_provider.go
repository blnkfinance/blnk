package adapters

import (
	"context"
	"time"

	"github.com/blnkfinance/blnk/kyc"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
)

type MockProvider struct {
	ShouldFail bool
	Delay      time.Duration
}

func NewMockProvider() *MockProvider {
	return &MockProvider{
		ShouldFail: false,
		Delay:      0,
	}
}

func (m *MockProvider) Name() string {
	return "mock_provider"
}

func (m *MockProvider) VerifyIdentity(ctx context.Context, identity *model.Identity) (*kyc.VerificationResult, error) {
	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}

	ref := "mock_" + uuid.New().String()

	if m.ShouldFail {
		return &kyc.VerificationResult{
			Status:      kyc.StatusFailed,
			Reason:      "Mock verification failure triggered",
			ProviderRef: ref,
			Timestamp:   time.Now(),
		}, nil
	}

	return &kyc.VerificationResult{
		Status:      kyc.StatusVerified,
		Reason:      "Identity verified by mock provider",
		ProviderRef: ref,
		Timestamp:   time.Now(),
	}, nil
}

func (m *MockProvider) VerifyDocument(ctx context.Context, identityID string, docType string, docData []byte) (*kyc.VerificationResult, error) {
	return &kyc.VerificationResult{
		Status:      kyc.StatusPending,
		Reason:      "Document received, processing started",
		ProviderRef: "doc_" + uuid.New().String(),
		Timestamp:   time.Now(),
	}, nil
}

func (m *MockProvider) CheckStatus(ctx context.Context, providerRef string) (*kyc.VerificationResult, error) {
	return &kyc.VerificationResult{
		Status:      kyc.StatusVerified,
		Reason:      "Async check completed successfully",
		ProviderRef: providerRef,
		Timestamp:   time.Now(),
	}, nil
}
