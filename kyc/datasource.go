package kyc

import "github.com/blnkfinance/blnk/model"

type KYCDataSource interface {
	GetIdentityByID(id string) (*model.Identity, error)
	CreateKYCWorkflow(workflow model.KYCWorkflow) (model.KYCWorkflow, error)
	GetKYCWorkflow(id string) (*model.KYCWorkflow, error)
	UpdateKYCWorkflow(workflow *model.KYCWorkflow) error
	CreateKYCStep(step model.KYCStep) (model.KYCStep, error)
	GetKYCStep(id string) (*model.KYCStep, error)
	UpdateKYCStep(step *model.KYCStep) error
	GetKYCStepsByWorkflow(workflowID string) ([]model.KYCStep, error)
	GetKYCStepsByStatus(status model.KYCStepStatus) ([]model.KYCStep, error)
}
