ALTER TABLE pgqrs_workflow_runs ADD COLUMN cancel_reason TEXT;
ALTER TABLE pgqrs_workflow_runs ADD COLUMN cancelled_at TEXT;
