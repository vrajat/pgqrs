-- Migration: Add archived_at column to pgqrs_messages and create indexes
ALTER TABLE pgqrs_messages ADD COLUMN archived_at TEXT;

CREATE INDEX IF NOT EXISTS idx_messages_hot_path ON pgqrs_messages (queue_id, vt, enqueued_at, id) WHERE archived_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_consumer_active ON pgqrs_messages (consumer_worker_id) WHERE archived_at IS NULL AND consumer_worker_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_archived_at ON pgqrs_messages (queue_id, archived_at) WHERE archived_at IS NOT NULL;
