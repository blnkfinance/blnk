package kyc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/model"
)

type configurableProviderAdapter struct {
	config     ProviderConfig
	httpClient *http.Client
}

func newConfigurableProviderAdapter(config ProviderConfig) ProviderAdapter {
	return &configurableProviderAdapter{
		config: config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (p *configurableProviderAdapter) Name() string {
	return p.config.Name
}

func (p *configurableProviderAdapter) VerifyIdentity(ctx context.Context, identity *model.Identity) (*VerificationResult, error) {
	endpoint := p.replaceIdentityPlaceholders(p.config.Endpoints.CreateCheck, identity)
	url := p.config.BaseURL + endpoint

	body := p.buildIdentityRequestBody(identity)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	p.addAuth(req)
	contentType := p.config.RequestConfig.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	return p.parseResponse(resp)
}

func (p *configurableProviderAdapter) VerifyDocument(ctx context.Context, identityID string, docType string, docData []byte) (*VerificationResult, error) {
	if p.config.Endpoints.UploadDocument == "" {
		return nil, fmt.Errorf("document upload not configured for provider %s", p.config.Name)
	}

	endpoint := strings.ReplaceAll(p.config.Endpoints.UploadDocument, "{identity_id}", identityID)
	url := p.config.BaseURL + endpoint

	body := map[string]interface{}{
		"type": docType,
		"file": base64.StdEncoding.EncodeToString(docData),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	p.addAuth(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	return p.parseResponse(resp)
}

func (p *configurableProviderAdapter) CheckStatus(ctx context.Context, providerRef string) (*VerificationResult, error) {
	endpoint := strings.ReplaceAll(p.config.Endpoints.GetStatus, "{provider_ref}", providerRef)
	endpoint = strings.ReplaceAll(endpoint, "{check_id}", providerRef)
	url := p.config.BaseURL + endpoint

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	p.addAuth(req)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	return p.parseResponse(resp)
}

func (p *configurableProviderAdapter) addAuth(req *http.Request) {
	switch strings.ToLower(p.config.AuthType) {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+p.config.APIKey)
	case "basic":
		auth := base64.StdEncoding.EncodeToString([]byte(p.config.APIKey + ":" + p.config.APISecret))
		req.Header.Set("Authorization", "Basic "+auth)
	case "header":
		header := p.config.AuthHeader
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, p.config.APIKey)
	default:
		req.Header.Set("Authorization", "Bearer "+p.config.APIKey)
	}
}

func (p *configurableProviderAdapter) buildIdentityRequestBody(identity *model.Identity) map[string]interface{} {
	body := make(map[string]interface{})

	defaultMapping := map[string]string{
		"first_name":    "first_name",
		"last_name":     "last_name",
		"email_address": "email",
		"phone_number":  "phone",
		"nationality":   "country",
		"dob":           "date_of_birth",
	}

	mapping := p.config.RequestConfig.IdentityFieldMapping
	if mapping == nil {
		mapping = defaultMapping
	}

	if v, ok := mapping["first_name"]; ok {
		body[v] = identity.FirstName
	}
	if v, ok := mapping["last_name"]; ok {
		body[v] = identity.LastName
	}
	if v, ok := mapping["email_address"]; ok {
		body[v] = identity.EmailAddress
	}
	if v, ok := mapping["phone_number"]; ok {
		body[v] = identity.PhoneNumber
	}
	if v, ok := mapping["nationality"]; ok {
		body[v] = identity.Nationality
	}
	if v, ok := mapping["dob"]; ok && !identity.DOB.IsZero() {
		body[v] = identity.DOB.Format("2006-01-02")
	}

	return body
}

func (p *configurableProviderAdapter) replaceIdentityPlaceholders(endpoint string, identity *model.Identity) string {
	result := endpoint
	result = strings.ReplaceAll(result, "{identity_id}", identity.IdentityID)
	result = strings.ReplaceAll(result, "{applicant_id}", identity.IdentityID)
	return result
}

func (p *configurableProviderAdapter) parseResponse(resp *http.Response) (*VerificationResult, error) {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("provider returned error status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var data map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to parse response JSON: %w", err)
	}

	statusValue := p.getNestedValue(data, p.config.ResponseMapping.StatusField)
	statusStr, _ := statusValue.(string)

	refValue := p.getNestedValue(data, p.config.ResponseMapping.ReferenceField)
	refStr, _ := refValue.(string)

	status := p.mapStatus(statusStr)

	return &VerificationResult{
		Status:      status,
		ProviderRef: refStr,
		RawData:     data,
		Timestamp:   time.Now(),
	}, nil
}

func (p *configurableProviderAdapter) getNestedValue(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else {
			return nil
		}
	}

	return current
}

func (p *configurableProviderAdapter) mapStatus(status string) VerificationStatus {
	statusLower := strings.ToLower(status)

	for _, v := range p.config.ResponseMapping.VerifiedValues {
		if strings.ToLower(v) == statusLower {
			return StatusVerified
		}
	}

	for _, v := range p.config.ResponseMapping.FailedValues {
		if strings.ToLower(v) == statusLower {
			return StatusFailed
		}
	}

	for _, v := range p.config.ResponseMapping.ReviewValues {
		if strings.ToLower(v) == statusLower {
			return StatusReview
		}
	}

	return StatusPending
}
