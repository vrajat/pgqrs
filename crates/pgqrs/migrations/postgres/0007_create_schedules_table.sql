-- Migration: Create pgqrs_schedules table
CREATE TABLE pgqrs_schedules (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    cron_expression VARCHAR(255) NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    input JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    next_fire_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for efficient next_fire scans
CREATE INDEX idx_pgqrs_schedules_next_fire ON pgqrs_schedules (status, next_fire_at);

-- Comments
COMMENT ON TABLE pgqrs_schedules IS 'Workflow execution schedules';
