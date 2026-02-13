CREATE TABLE IF NOT EXISTS pgqrs_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,  -- JSON stored as TEXT
    vt TEXT DEFAULT (datetime('now')),  -- visibility timeout
    enqueued_at TEXT DEFAULT (datetime('now')),
    read_ct INTEGER DEFAULT 0,
    dequeued_at TEXT,
    producer_worker_id INTEGER REFERENCES pgqrs_workers(id),
    consumer_worker_id INTEGER REFERENCES pgqrs_workers(id)
);

CREATE INDEX IF NOT EXISTS idx_messages_queue_vt ON pgqrs_messages(queue_id, vt);
CREATE INDEX IF NOT EXISTS idx_messages_consumer ON pgqrs_messages(consumer_worker_id);
CREATE INDEX IF NOT EXISTS idx_messages_queue_enqueued_at ON pgqrs_messages(queue_id, enqueued_at);
