CREATE TABLE IF NOT EXISTS pgqrs_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_msg_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    vt TEXT NOT NULL,
    read_ct INTEGER NOT NULL,
    archived_at TEXT DEFAULT (datetime('now')) NOT NULL,
    dequeued_at TEXT,
    producer_worker_id INTEGER,
    consumer_worker_id INTEGER
);
