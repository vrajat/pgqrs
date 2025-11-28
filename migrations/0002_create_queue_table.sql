-- Migration: Create pgqrs_queues table and indexes
CREATE TABLE pgqrs_queues (
    id BIGSERIAL PRIMARY KEY,
    queue_name VARCHAR UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL
);
