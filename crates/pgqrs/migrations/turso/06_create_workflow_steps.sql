CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
    step_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES pgqrs_workflows(workflow_id),
    step_key TEXT NOT NULL,
    status TEXT DEFAULT 'PENDING' NOT NULL,
    output TEXT,
    error TEXT,
    started_at TEXT,
    completed_at TEXT,
    retry_count INTEGER DEFAULT 0 NOT NULL,
    last_retry_at TEXT,
    retry_at TEXT,
    UNIQUE(workflow_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON pgqrs_workflow_steps(workflow_id);

-- Index for efficient retry queries
CREATE INDEX IF NOT EXISTS idx_workflow_steps_retry 
ON pgqrs_workflow_steps(workflow_id, retry_count);

-- Index for efficient retry_at polling
CREATE INDEX IF NOT EXISTS idx_workflow_steps_retry_at
ON pgqrs_workflow_steps(retry_at);
