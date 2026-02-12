-- Workflow definitions (templates)
CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    workflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);

-- Workflow runs (executions)
CREATE TABLE IF NOT EXISTS pgqrs_workflow_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_name TEXT NOT NULL,
    status TEXT NOT NULL,
    input TEXT,
    output TEXT,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')) NOT NULL,
    started_at TEXT,
    paused_at TEXT,
    completed_at TEXT,
    worker_id INTEGER REFERENCES pgqrs_workers(id)
);

-- Step state (for crash recovery)
CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
    run_id INTEGER NOT NULL REFERENCES pgqrs_workflow_runs(run_id),
    step_id TEXT NOT NULL,
    status TEXT NOT NULL,
    input TEXT,
    output TEXT,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')) NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    retry_at TEXT,
    last_retry_at TEXT,
    PRIMARY KEY (run_id, step_id)
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status ON pgqrs_workflow_runs(status);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_workflow_name ON pgqrs_workflow_runs(workflow_name);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_status ON pgqrs_workflow_steps(status);
