ALTER TYPE pgqrs_workflow_status ADD VALUE IF NOT EXISTS 'CANCELLED';

ALTER TABLE pgqrs_workflow_runs
    ADD COLUMN IF NOT EXISTS cancel_reason JSONB,
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;

