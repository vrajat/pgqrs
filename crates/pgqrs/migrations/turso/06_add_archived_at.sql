-- Migration: Add archived_at column to pgqrs_messages
ALTER TABLE pgqrs_messages ADD COLUMN archived_at TEXT;

CREATE INDEX IF NOT EXISTS idx_messages_archived_at ON pgqrs_messages (queue_id, archived_at);
