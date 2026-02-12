CREATE TABLE IF NOT EXISTS pgqrs_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,
    vt TEXT DEFAULT (datetime('now')),
    enqueued_at TEXT DEFAULT (datetime('now')),
    read_ct INTEGER DEFAULT 0,
    dequeued_at TEXT,
    producer_worker_id INTEGER REFERENCES pgqrs_workers(id),
    consumer_worker_id INTEGER REFERENCES pgqrs_workers(id)
);
