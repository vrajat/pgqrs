CREATE TYPE pgqrs_workflow_status AS ENUM ('PENDING', 'RUNNING', 'SUCCESS', 'ERROR');

CREATE TABLE pgqrs_workflows (
    workflow_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executor_id VARCHAR(255)
);

CREATE TABLE pgqrs_workflow_steps (
    workflow_id UUID NOT NULL REFERENCES pgqrs_workflows(workflow_id) ON DELETE CASCADE,
    step_id VARCHAR(255) NOT NULL,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, step_id)
);
