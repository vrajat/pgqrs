-- Workflow definitions (templates)
CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);

-- Workflow runs (executions)
CREATE TABLE IF NOT EXISTS pgqrs_workflow_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES pgqrs_workflows(id),
    message_id INTEGER NOT NULL UNIQUE REFERENCES pgqrs_messages(id),
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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES pgqrs_workflow_runs(id),
    step_name TEXT NOT NULL,
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
    UNIQUE (run_id, step_name)
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_workflow_runs_message_id ON pgqrs_workflow_runs(message_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status ON pgqrs_workflow_runs(status);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_status ON pgqrs_workflow_steps(status);
