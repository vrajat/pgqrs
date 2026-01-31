-- Add retry tracking columns to workflow steps
-- These columns enable automatic step-level retry with configurable backoff strategies

-- Add retry_count column to track number of retry attempts
ALTER TABLE pgqrs_workflow_steps ADD COLUMN retry_count INTEGER DEFAULT 0 NOT NULL;

-- Add last_retry_at column to track when the last retry occurred
ALTER TABLE pgqrs_workflow_steps ADD COLUMN last_retry_at TEXT;

-- Add retry_at column to schedule delayed retries (prevents worker blocking)
ALTER TABLE pgqrs_workflow_steps ADD COLUMN retry_at TEXT;

-- Index for efficient retry queries
CREATE INDEX IF NOT EXISTS idx_workflow_steps_retry 
ON pgqrs_workflow_steps(workflow_id, retry_count);

-- Index for efficient retry_at polling
-- Note: Partial index (WHERE retry_at IS NOT NULL) would be more efficient,
-- but turso_core 0.4.2 has a bug parsing partial indices, so using full index
CREATE INDEX IF NOT EXISTS idx_workflow_steps_retry_at
ON pgqrs_workflow_steps(retry_at);
