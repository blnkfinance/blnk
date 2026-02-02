package database

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// CreateKYCWorkflow creates a new KYC workflow record in the database
func (d *Datasource) CreateKYCWorkflow(workflow model.KYCWorkflow) (model.KYCWorkflow, error) {
	metaDataJSON, err := json.Marshal(workflow.MetaData)
	if err != nil {
		return model.KYCWorkflow{}, err
	}

	query := `
		INSERT INTO blnk.kyc_workflows (workflow_id, identity_id, status, current_step, provider_id, decision_reason, meta_data, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING workflow_id
	`
	err = d.Conn.QueryRow(
		query,
		workflow.WorkflowID,
		workflow.IdentityID,
		workflow.Status,
		workflow.CurrentStep,
		workflow.ProviderID,
		workflow.DecisionReason,
		metaDataJSON,
		workflow.CreatedAt,
		workflow.UpdatedAt,
	).Scan(&workflow.WorkflowID)

	if err != nil {
		return model.KYCWorkflow{}, err
	}
	return workflow, nil
}

// GetKYCWorkflow retrieves a KYC workflow by its ID
func (d *Datasource) GetKYCWorkflow(id string) (*model.KYCWorkflow, error) {
	var workflow model.KYCWorkflow
	var metaDataJSON []byte

	query := `
		SELECT workflow_id, identity_id, status, current_step, provider_id, decision_reason, meta_data, created_at, updated_at
		FROM blnk.kyc_workflows
		WHERE workflow_id = $1
	`
	err := d.Conn.QueryRow(query, id).Scan(
		&workflow.WorkflowID,
		&workflow.IdentityID,
		&workflow.Status,
		&workflow.CurrentStep,
		&workflow.ProviderID,
		&workflow.DecisionReason,
		&metaDataJSON,
		&workflow.CreatedAt,
		&workflow.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil // Return nil if not found
	}
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(metaDataJSON, &workflow.MetaData); err != nil {
		return nil, err
	}

	return &workflow, nil
}

// UpdateKYCWorkflow updates an existing KYC workflow
func (d *Datasource) UpdateKYCWorkflow(workflow *model.KYCWorkflow) error {
	metaDataJSON, err := json.Marshal(workflow.MetaData)
	if err != nil {
		return err
	}

	workflow.UpdatedAt = time.Now()

	query := `
		UPDATE blnk.kyc_workflows
		SET status = $2, current_step = $3, decision_reason = $4, meta_data = $5, updated_at = $6
		WHERE workflow_id = $1
	`
	_, err = d.Conn.Exec(
		query,
		workflow.WorkflowID,
		workflow.Status,
		workflow.CurrentStep,
		workflow.DecisionReason,
		metaDataJSON,
		workflow.UpdatedAt,
	)
	return err
}

// CreateKYCStep creates a new KYC step record
func (d *Datasource) CreateKYCStep(step model.KYCStep) (model.KYCStep, error) {
	resultJSON, err := json.Marshal(step.Result)
	if err != nil {
		return model.KYCStep{}, err
	}

	query := `
		INSERT INTO blnk.kyc_steps (step_id, workflow_id, step_name, status, provider_ref, result, retry_count, max_retries, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING step_id
	`
	err = d.Conn.QueryRow(
		query,
		step.StepID,
		step.WorkflowID,
		step.StepName,
		step.Status,
		step.ProviderRef,
		resultJSON,
		step.RetryCount,
		step.MaxRetries,
		step.CreatedAt,
		step.UpdatedAt,
	).Scan(&step.StepID)

	if err != nil {
		return model.KYCStep{}, err
	}
	return step, nil
}

// GetKYCStep retrieves a KYC step by ID
func (d *Datasource) GetKYCStep(id string) (*model.KYCStep, error) {
	var step model.KYCStep
	var resultJSON []byte

	query := `
		SELECT step_id, workflow_id, step_name, status, provider_ref, result, retry_count, max_retries, created_at, updated_at
		FROM blnk.kyc_steps
		WHERE step_id = $1
	`
	err := d.Conn.QueryRow(query, id).Scan(
		&step.StepID,
		&step.WorkflowID,
		&step.StepName,
		&step.Status,
		&step.ProviderRef,
		&resultJSON,
		&step.RetryCount,
		&step.MaxRetries,
		&step.CreatedAt,
		&step.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(resultJSON, &step.Result); err != nil {
		return nil, err
	}

	return &step, nil
}

// UpdateKYCStep updates an existing KYC step
func (d *Datasource) UpdateKYCStep(step *model.KYCStep) error {
	resultJSON, err := json.Marshal(step.Result)
	if err != nil {
		return err
	}

	step.UpdatedAt = time.Now()

	query := `
		UPDATE blnk.kyc_steps
		SET status = $2, provider_ref = $3, result = $4, retry_count = $5, updated_at = $6
		WHERE step_id = $1
	`
	_, err = d.Conn.Exec(
		query,
		step.StepID,
		step.Status,
		step.ProviderRef,
		resultJSON,
		step.RetryCount,
		step.UpdatedAt,
	)
	return err
}

// GetKYCStepsByWorkflow retrieves all steps for a workflow
func (d *Datasource) GetKYCStepsByWorkflow(workflowID string) ([]model.KYCStep, error) {
	query := `
		SELECT step_id, workflow_id, step_name, status, provider_ref, result, retry_count, max_retries, created_at, updated_at
		FROM blnk.kyc_steps
		WHERE workflow_id = $1
		ORDER BY created_at ASC
	`
	rows, err := d.Conn.Query(query, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []model.KYCStep
	for rows.Next() {
		var step model.KYCStep
		var resultJSON []byte
		if err := rows.Scan(
			&step.StepID,
			&step.WorkflowID,
			&step.StepName,
			&step.Status,
			&step.ProviderRef,
			&resultJSON,
			&step.RetryCount,
			&step.MaxRetries,
			&step.CreatedAt,
			&step.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(resultJSON, &step.Result); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, nil
}

// GetKYCStepsByStatus retrieves all steps with a specific status (e.g., SUBMITTED for polling)
func (d *Datasource) GetKYCStepsByStatus(status model.KYCStepStatus) ([]model.KYCStep, error) {
	query := `
		SELECT step_id, workflow_id, step_name, status, provider_ref, result, retry_count, max_retries, created_at, updated_at
		FROM blnk.kyc_steps
		WHERE status = $1
		ORDER BY created_at ASC
	`
	rows, err := d.Conn.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []model.KYCStep
	for rows.Next() {
		var step model.KYCStep
		var resultJSON []byte
		if err := rows.Scan(
			&step.StepID,
			&step.WorkflowID,
			&step.StepName,
			&step.Status,
			&step.ProviderRef,
			&resultJSON,
			&step.RetryCount,
			&step.MaxRetries,
			&step.CreatedAt,
			&step.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(resultJSON, &step.Result); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, nil
}
