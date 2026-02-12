CREATE TABLE IF NOT EXISTS pgqrs_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_msg_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,  -- JSON stored as TEXT
    enqueued_at TEXT NOT NULL,
    vt TEXT NOT NULL,
    read_ct INTEGER NOT NULL,
    archived_at TEXT DEFAULT (datetime('now')) NOT NULL,
    dequeued_at TEXT,
    producer_worker_id INTEGER,
    consumer_worker_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_archive_queue_id ON pgqrs_archive(queue_id);
CREATE INDEX IF NOT EXISTS idx_archive_original_msg ON pgqrs_archive(original_msg_id);
CREATE INDEX IF NOT EXISTS idx_archive_archived_at ON pgqrs_archive(archived_at);
CREATE INDEX IF NOT EXISTS idx_archive_producer ON pgqrs_archive(producer_worker_id);
CREATE INDEX IF NOT EXISTS idx_archive_consumer ON pgqrs_archive(consumer_worker_id);
