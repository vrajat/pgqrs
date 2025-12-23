CREATE TYPE pgqrs_workflow_status AS ENUM ('PENDING', 'RUNNING', 'SUCCESS', 'ERROR');

CREATE TABLE pgqrs_workflows (
    workflow_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executor_id VARCHAR(255)
);

-- Indexes for pgqrs_workflows
CREATE INDEX idx_pgqrs_workflows_status ON pgqrs_workflows(status);
CREATE INDEX idx_pgqrs_workflows_created_at ON pgqrs_workflows(created_at);
CREATE INDEX idx_pgqrs_workflows_name ON pgqrs_workflows(name);

-- Comments for pgqrs_workflows
COMMENT ON TABLE pgqrs_workflows IS 'Stores execution state of durable workflows.';
COMMENT ON COLUMN pgqrs_workflows.workflow_id IS 'Unique identifier for the workflow execution.';
COMMENT ON COLUMN pgqrs_workflows.name IS 'Name of the workflow type.';
COMMENT ON COLUMN pgqrs_workflows.status IS 'Current status of the workflow execution.';
COMMENT ON COLUMN pgqrs_workflows.input IS 'Initial input payload for the workflow.';
COMMENT ON COLUMN pgqrs_workflows.output IS 'Final output of the workflow if successful.';
COMMENT ON COLUMN pgqrs_workflows.error IS 'Error details if the workflow failed.';
COMMENT ON COLUMN pgqrs_workflows.executor_id IS 'ID of the worker currently executing this workflow (if locked).';


CREATE TABLE pgqrs_workflow_steps (
    workflow_id BIGINT NOT NULL REFERENCES pgqrs_workflows(workflow_id) ON DELETE CASCADE,
    step_id VARCHAR(255) NOT NULL,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, step_id)
);

-- Indexes for pgqrs_workflow_steps
CREATE INDEX idx_pgqrs_workflow_steps_workflow_id ON pgqrs_workflow_steps(workflow_id);
CREATE INDEX idx_pgqrs_workflow_steps_status ON pgqrs_workflow_steps(status);

-- Comments for pgqrs_workflow_steps
COMMENT ON TABLE pgqrs_workflow_steps IS 'Stores state of individual steps within a workflow.';
COMMENT ON COLUMN pgqrs_workflow_steps.workflow_id IS 'Reference to the parent workflow.';
COMMENT ON COLUMN pgqrs_workflow_steps.step_id IS 'Unique identifier for the step within the workflow.';
COMMENT ON COLUMN pgqrs_workflow_steps.status IS 'Current status of the step.';
COMMENT ON COLUMN pgqrs_workflow_steps.input IS 'Input provided to the step.';
COMMENT ON COLUMN pgqrs_workflow_steps.output IS 'Output produced by the step if successful.';
COMMENT ON COLUMN pgqrs_workflow_steps.error IS 'Error details if the step failed.';
