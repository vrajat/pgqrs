-- Migration: Add archived_at column to pgqrs_messages and create partial indexes
ALTER TABLE pgqrs_messages ADD COLUMN archived_at TIMESTAMPTZ;

CREATE INDEX idx_pgqrs_messages_hot_path ON pgqrs_messages (queue_id, vt, enqueued_at, id) WHERE archived_at IS NULL;
CREATE INDEX idx_pgqrs_messages_consumer_worker_id_active ON pgqrs_messages (consumer_worker_id) WHERE archived_at IS NULL AND consumer_worker_id IS NOT NULL;
CREATE INDEX idx_pgqrs_messages_archived_at ON pgqrs_messages (queue_id, archived_at) WHERE archived_at IS NOT NULL;
