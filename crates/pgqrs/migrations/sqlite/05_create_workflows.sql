CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    workflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    input TEXT NOT NULL,  -- JSON
    status TEXT DEFAULT 'PENDING' NOT NULL
        CHECK(status IN ('PENDING', 'RUNNING', 'SUCCESS', 'ERROR')),
    output TEXT,  -- JSON, nullable
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')) NOT NULL,
    executor_id TEXT
);

CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
    step_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES pgqrs_workflows(workflow_id),
    step_key TEXT NOT NULL, -- Logical step ID (e.g. 'process_payment')
    status TEXT DEFAULT 'PENDING' NOT NULL
        CHECK(status IN ('PENDING', 'RUNNING', 'SUCCESS', 'ERROR')),
    output TEXT,  -- JSON, nullable
    error TEXT,
    started_at TEXT,
    completed_at TEXT,
    UNIQUE(workflow_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON pgqrs_workflow_steps(workflow_id);
