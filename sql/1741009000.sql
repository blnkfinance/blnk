-- Create KYC Workflows table
CREATE TABLE IF NOT EXISTS blnk.kyc_workflows (
    workflow_id VARCHAR(255) PRIMARY KEY,
    identity_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    current_step VARCHAR(255),
    provider_id VARCHAR(255),
    decision_reason TEXT,
    meta_data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (identity_id) REFERENCES blnk.identities(identity_id)
);

-- Create KYC Steps table
CREATE TABLE IF NOT EXISTS blnk.kyc_steps (
    step_id VARCHAR(255) PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    provider_ref VARCHAR(255),
    result JSONB DEFAULT '{}',
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (workflow_id) REFERENCES blnk.kyc_workflows(workflow_id)
);

-- Create Indexes for performance
CREATE INDEX IF NOT EXISTS idx_kyc_workflows_identity ON blnk.kyc_workflows(identity_id);
CREATE INDEX IF NOT EXISTS idx_kyc_workflows_status ON blnk.kyc_workflows(status);
CREATE INDEX IF NOT EXISTS idx_kyc_steps_workflow ON blnk.kyc_steps(workflow_id);
