-- Create schema and meta table for queues
CREATE SCHEMA IF NOT EXISTS pgqrs;

CREATE TABLE IF NOT EXISTS pgqrs.meta (
    queue_name VARCHAR PRIMARY KEY UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    unlogged BOOLEAN DEFAULT FALSE
);
