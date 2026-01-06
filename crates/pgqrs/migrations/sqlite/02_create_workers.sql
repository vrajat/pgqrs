CREATE TABLE IF NOT EXISTS pgqrs_workers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    hostname TEXT NOT NULL,
    port INTEGER NOT NULL,
    status TEXT DEFAULT 'ready' NOT NULL
        CHECK(status IN ('ready', 'suspended', 'stopped')),
    last_heartbeat TEXT DEFAULT (datetime('now')) NOT NULL,
    created_at TEXT DEFAULT (datetime('now')) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workers_queue_id ON pgqrs_workers(queue_id);
CREATE INDEX IF NOT EXISTS idx_workers_status ON pgqrs_workers(status);
