CREATE TABLE IF NOT EXISTS pgqrs_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES pgqrs_queues(id),
    payload TEXT NOT NULL,  -- JSON stored as TEXT
    vt TEXT DEFAULT (datetime('now')),  -- visibility timeout
    enqueued_at TEXT DEFAULT (datetime('now')),
    read_ct INTEGER DEFAULT 0,
    dequeued_at TEXT,
    producer_worker_id INTEGER REFERENCES pgqrs_workers(id),
    consumer_worker_id INTEGER REFERENCES pgqrs_workers(id),
    archived_at TEXT
);

-- Partial index for the "hot set" (ready or leased messages)
CREATE INDEX IF NOT EXISTS idx_messages_hot_path ON pgqrs_messages (queue_id, vt, enqueued_at, id) WHERE archived_at IS NULL;

-- Index for active consumer worker lookups
CREATE INDEX IF NOT EXISTS idx_messages_consumer_active ON pgqrs_messages (consumer_worker_id) WHERE archived_at IS NULL AND consumer_worker_id IS NOT NULL;

-- Index for archived messages lookups
CREATE INDEX IF NOT EXISTS idx_messages_archived_at ON pgqrs_messages (queue_id, archived_at) WHERE archived_at IS NOT NULL;
