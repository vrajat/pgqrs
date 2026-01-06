CREATE TABLE IF NOT EXISTS pgqrs_queues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT UNIQUE NOT NULL,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);
