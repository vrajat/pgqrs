-- Create enum type
DO $$
BEGIN
    CREATE TYPE pgqrs_workflow_status AS ENUM ('QUEUED', 'RUNNING', 'SUCCESS', 'ERROR', 'PAUSED');
EXCEPTION
    WHEN duplicate_object THEN
        BEGIN
            ALTER TYPE pgqrs_workflow_status ADD VALUE IF NOT EXISTS 'QUEUED';
        EXCEPTION
            WHEN duplicate_object THEN null;
        END;
END $$;

-- Workflow definitions (templates)
CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    queue_id BIGINT NOT NULL REFERENCES pgqrs_queues(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow runs (executions)
CREATE TABLE IF NOT EXISTS pgqrs_workflow_runs (
    id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL REFERENCES pgqrs_workflows(id) ON DELETE CASCADE,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    paused_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    worker_id BIGINT REFERENCES pgqrs_workers(id)
);

-- Step state (for crash recovery)
CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES pgqrs_workflow_runs(id) ON DELETE CASCADE,
    step_name VARCHAR(255) NOT NULL,
    status pgqrs_workflow_status NOT NULL,
    input JSONB,
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Retry scheduling
    retry_count INTEGER NOT NULL DEFAULT 0,
    retry_at TIMESTAMPTZ,
    last_retry_at TIMESTAMPTZ,

    UNIQUE (run_id, step_name)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pgqrs_workflows_name ON pgqrs_workflows(name);
CREATE INDEX IF NOT EXISTS idx_pgqrs_workflow_runs_status ON pgqrs_workflow_runs(status);
CREATE INDEX IF NOT EXISTS idx_pgqrs_workflow_steps_status ON pgqrs_workflow_steps(status);
CREATE INDEX IF NOT EXISTS idx_pgqrs_workflow_steps_retry_at ON pgqrs_workflow_steps(retry_at) WHERE retry_at IS NOT NULL;

-- Comments
COMMENT ON TABLE pgqrs_workflows IS 'Workflow definitions (templates)';
COMMENT ON TABLE pgqrs_workflow_runs IS 'Workflow execution instances';
COMMENT ON TABLE pgqrs_workflow_steps IS 'Workflow step state for crash recovery';
