CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    input TEXT NOT NULL,  -- JSON
    status TEXT DEFAULT 'pending' NOT NULL
        CHECK(status IN ('pending', 'running', 'completed', 'failed')),
    output TEXT,  -- JSON, nullable
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')) NOT NULL
);

CREATE TABLE IF NOT EXISTS pgqrs_workflow_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES pgqrs_workflows(id),
    step_id TEXT NOT NULL,
    status TEXT DEFAULT 'pending' NOT NULL
        CHECK(status IN ('pending', 'running', 'completed', 'failed')),
    output TEXT,  -- JSON, nullable
    error TEXT,
    started_at TEXT,
    completed_at TEXT,
    UNIQUE(workflow_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON pgqrs_workflow_steps(workflow_id);
