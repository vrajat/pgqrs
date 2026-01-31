-- Add retry tracking columns to workflow steps
-- These columns enable automatic step-level retry with configurable backoff strategies

-- Add retry_count column to track number of retry attempts
ALTER TABLE pgqrs_workflow_steps
ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;

-- Add last_retry_at column to track when the last retry occurred
ALTER TABLE pgqrs_workflow_steps
ADD COLUMN last_retry_at TIMESTAMPTZ;

-- Index for efficient retry queries
CREATE INDEX idx_pgqrs_workflow_steps_retry 
ON pgqrs_workflow_steps(workflow_id, retry_count);

-- Documentation comments
COMMENT ON COLUMN pgqrs_workflow_steps.retry_count IS 'Number of times this step has been retried (0 = no retries yet)';
COMMENT ON COLUMN pgqrs_workflow_steps.last_retry_at IS 'Timestamp of the last retry attempt (NULL if never retried)';
