package kyc

import (
	"context"
	"errors"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

type WorkflowConfig struct {
	ProviderID string
	Workflow   string
}

type WorkflowEngine struct {
	datasource KYCDataSource
	providers  map[string]ProviderAdapter
}

func NewWorkflowEngine(ds KYCDataSource) *WorkflowEngine {
	return &WorkflowEngine{
		datasource: ds,
		providers:  make(map[string]ProviderAdapter),
	}
}

func (e *WorkflowEngine) RegisterProvider(provider ProviderAdapter) {
	e.providers[provider.Name()] = provider
}

func (e *WorkflowEngine) GetWorkflow(id string) (*model.KYCWorkflow, error) {
	return e.datasource.GetKYCWorkflow(id)
}

func (e *WorkflowEngine) StartWorkflow(ctx context.Context, identityID string, cfg WorkflowConfig) (*model.KYCWorkflow, error) {
	identity, err := e.datasource.GetIdentityByID(identityID)
	if err != nil {
		return nil, err
	}
	if identity == nil {
		return nil, errors.New("identity not found")
	}

	provider, exists := e.providers[cfg.ProviderID]
	if !exists {
		return nil, errors.New("provider not configured")
	}

	workflow := model.KYCWorkflow{
		WorkflowID: model.GenerateUUIDWithSuffix("kyc"),
		IdentityID: identityID,
		Status:     model.KYCStatusPending,
		ProviderID: provider.Name(),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	savedWorkflow, err := e.datasource.CreateKYCWorkflow(workflow)
	if err != nil {
		return nil, err
	}

	step := model.KYCStep{
		StepID:     model.GenerateUUIDWithSuffix("step"),
		WorkflowID: savedWorkflow.WorkflowID,
		StepName:   "identity_verification",
		Status:     model.StepStatusPending,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	savedStep, err := e.datasource.CreateKYCStep(step)
	if err != nil {
		return nil, err
	}

	savedWorkflow.CurrentStep = savedStep.StepID
	e.datasource.UpdateKYCWorkflow(&savedWorkflow)
	savedWorkflow.Steps = append(savedWorkflow.Steps, savedStep)

	go e.ProcessStep(context.Background(), savedStep.StepID)

	return &savedWorkflow, nil
}

func (e *WorkflowEngine) ProcessStep(ctx context.Context, stepID string) error {
	logrus.Infof("Processing KYC Step: %s", stepID)
	step, err := e.datasource.GetKYCStep(stepID)
	if err != nil {
		return err
	}

	workflow, err := e.datasource.GetKYCWorkflow(step.WorkflowID)
	if err != nil {
		return err
	}

	provider, exists := e.providers[workflow.ProviderID]
	if !exists {
		return errors.New("provider missing for active workflow")
	}

	identity, err := e.datasource.GetIdentityByID(workflow.IdentityID)
	if err != nil {
		return err
	}

	var result *VerificationResult

	if step.StepName == "identity_verification" {
		result, err = provider.VerifyIdentity(ctx, identity)
	} else {
		return errors.New("unknown step type")
	}

	if err != nil {
		return err
	}

	step.ProviderRef = result.ProviderRef
	step.Result = result.RawData

	if result.Status == StatusVerified {
		step.Status = model.StepStatusPassed
	} else if result.Status == StatusFailed {
		step.Status = model.StepStatusFailed
	} else {
		step.Status = model.StepStatusSubmitted
	}

	e.datasource.UpdateKYCStep(step)
	e.evaluateWorkflow(ctx, workflow)

	return nil
}

func (e *WorkflowEngine) evaluateWorkflow(ctx context.Context, workflow *model.KYCWorkflow) error {
	steps, err := e.datasource.GetKYCStepsByWorkflow(workflow.WorkflowID)
	if err != nil {
		return err
	}

	allPassed := true
	anyFailed := false

	for _, s := range steps {
		if s.Status == model.StepStatusFailed {
			anyFailed = true
		}
		if s.Status != model.StepStatusPassed {
			allPassed = false
		}
	}

	if anyFailed {
		workflow.Status = model.KYCStatusRejected
		workflow.DecisionReason = "One or more steps failed"
	} else if allPassed {
		workflow.Status = model.KYCStatusApproved
	} else {
		workflow.Status = model.KYCStatusInProgress
	}

	return e.datasource.UpdateKYCWorkflow(workflow)
}
