CREATE TABLE IF NOT EXISTS pgqrs_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_message_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    archived_at TEXT DEFAULT (datetime('now')) NOT NULL,
    read_ct INTEGER NOT NULL,
    archive_reason TEXT NOT NULL
        CHECK(archive_reason IN ('completed', 'failed', 'expired', 'manual')),
    producer_worker_id INTEGER,
    consumer_worker_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_archive_queue_id ON pgqrs_archive(queue_id);
CREATE INDEX IF NOT EXISTS idx_archive_reason ON pgqrs_archive(archive_reason);
