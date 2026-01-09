CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    workflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    input TEXT NOT NULL,
    status TEXT DEFAULT 'PENDING' NOT NULL,
    output TEXT,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')) NOT NULL,
    executor_id TEXT
);
