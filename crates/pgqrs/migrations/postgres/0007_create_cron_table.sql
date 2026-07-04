-- Migration: Create pgqrs_cron table
CREATE TYPE pgqrs_trigger_state AS ENUM ('idle', 'firing');

CREATE TABLE pgqrs_cron (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    queue_id BIGINT NOT NULL UNIQUE REFERENCES pgqrs_queues(id) ON DELETE CASCADE,
    cron_expression VARCHAR(255) NOT NULL,
    input JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    trigger_state pgqrs_trigger_state NOT NULL DEFAULT 'idle',
    next_fire_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for efficient next_fire scans
CREATE INDEX idx_pgqrs_cron_next_fire ON pgqrs_cron (status, next_fire_at);

-- Comments
COMMENT ON TABLE pgqrs_cron IS 'Workflow execution cron schedules';
