# KYC Module

Provider-agnostic KYC/Compliance orchestration for Blnk.

## Overview

This module provides:

- Workflow-based verification with multiple steps
- Async polling for providers that don't return results immediately  
- YAML configuration for quick provider integration
- Code-based adapters for complex provider logic
- Automatic status tracking and workflow state management

## Usage

### Initialize the Engine

```go
import (
    "github.com/blnkfinance/blnk/kyc"
)

engine := kyc.NewWorkflowEngine(datasource)
```

### Add Providers

Option 1: Load from YAML config

```go
engine.LoadProvidersFromConfig("kyc_providers.yaml")
```

Option 2: Register code-based adapter

```go
engine.RegisterProvider(adapters.NewMockProvider())
```

### Start a Workflow

```go
workflow, err := engine.StartWorkflow(ctx, identityID, kyc.WorkflowConfig{
    ProviderID: "onfido",
    Workflow:   "default",
})
```

### Start the Poller

For async providers, start the background poller:

```go
poller := kyc.NewPoller(engine, datasource, 30*time.Second)
poller.Start()
defer poller.Stop()
```

## YAML Configuration

Create `kyc_providers.yaml`:

```yaml
providers:
  - name: "onfido"
    enabled: true
    api_key: "${ONFIDO_API_KEY}"
    auth_type: "bearer"
    base_url: "https://api.onfido.com/v3.6"
    endpoints:
      create_check: "/applicants"
      get_status: "/checks/{provider_ref}"
    response_mapping:
      status_field: "result"
      reference_field: "id"
      verified_values: ["clear", "complete"]
      failed_values: ["consider", "rejected"]
      pending_values: ["in_progress"]
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| name | Yes | Unique provider identifier |
| enabled | Yes | Whether to load this provider |
| api_key | Yes | API key (supports ${ENV_VAR}) |
| api_secret | No | API secret for basic auth |
| auth_type | Yes | bearer, basic, or header |
| auth_header | No | Custom header name for header auth |
| base_url | Yes | Provider API base URL |
| endpoints.create_check | Yes | Endpoint to initiate verification |
| endpoints.get_status | Yes | Endpoint to check status |
| endpoints.upload_document | No | Endpoint for document uploads |
| response_mapping.status_field | Yes | JSON path to status in response |
| response_mapping.reference_field | Yes | JSON path to reference ID |
| response_mapping.verified_values | Yes | Status values meaning verified |
| response_mapping.failed_values | Yes | Status values meaning failed |
| response_mapping.pending_values | Yes | Status values meaning pending |

## Code-Based Adapter

For providers requiring custom logic, implement the ProviderAdapter interface:

```go
type ProviderAdapter interface {
    Name() string
    VerifyIdentity(ctx context.Context, identity *model.Identity) (*VerificationResult, error)
    VerifyDocument(ctx context.Context, identityID string, docType string, docData []byte) (*VerificationResult, error)
    CheckStatus(ctx context.Context, providerRef string) (*VerificationResult, error)
}
```

Example:

```go
type MyProvider struct {
    client *myclient.Client
}

func (p *MyProvider) Name() string {
    return "my_provider"
}

func (p *MyProvider) VerifyIdentity(ctx context.Context, identity *model.Identity) (*kyc.VerificationResult, error) {
    resp, err := p.client.CreateCheck(identity)
    if err != nil {
        return nil, err
    }
    
    return &kyc.VerificationResult{
        Status:      kyc.StatusPending,
        ProviderRef: resp.ID,
    }, nil
}

func (p *MyProvider) CheckStatus(ctx context.Context, providerRef string) (*kyc.VerificationResult, error) {
    resp, err := p.client.GetCheck(providerRef)
    if err != nil {
        return nil, err
    }
    
    status := kyc.StatusPending
    if resp.Status == "complete" {
        status = kyc.StatusVerified
    }
    
    return &kyc.VerificationResult{
        Status:      status,
        ProviderRef: providerRef,
    }, nil
}
```

## Workflow States

Workflow status progression:

```
PENDING -> IN_PROGRESS -> APPROVED
                       -> REJECTED
                       -> REVIEW_NEEDED
```

Step status progression:

```
PENDING -> SUBMITTED -> PASSED
                     -> FAILED
                     -> RETRY_NEEDED
```

## Database Tables

The module uses two tables:

- `blnk.kyc_workflows` - Stores workflow records
- `blnk.kyc_steps` - Stores individual verification steps

See `sql/1741009000.sql` for schema.
