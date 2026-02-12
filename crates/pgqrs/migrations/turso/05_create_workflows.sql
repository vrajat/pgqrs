-- Workflow definitions (templates)
CREATE TABLE IF NOT EXISTS pgqrs_workflows (
    workflow_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);
