-- Add retry tracking columns to workflow steps
-- These columns enable automatic step-level retry with configurable backoff strategies

-- Add retry_count column to track number of retry attempts
ALTER TABLE pgqrs_workflow_steps ADD COLUMN retry_count INTEGER DEFAULT 0 NOT NULL;

-- Add last_retry_at column to track when the last retry occurred
ALTER TABLE pgqrs_workflow_steps ADD COLUMN last_retry_at TEXT;

-- Index for efficient retry queries
CREATE INDEX IF NOT EXISTS idx_workflow_steps_retry 
ON pgqrs_workflow_steps(workflow_id, retry_count);
