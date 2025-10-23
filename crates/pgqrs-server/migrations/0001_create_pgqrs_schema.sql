-- Create schema and meta table for queues
CREATE SCHEMA IF NOT EXISTS pgqrs;

CREATE TABLE IF NOT EXISTS pgqrs.meta (
    id INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue_name VARCHAR UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    unlogged BOOLEAN DEFAULT FALSE
);