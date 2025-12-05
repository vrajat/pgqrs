-- Migration: Create pgqrs_workers table and indexes
CREATE TABLE IF NOT EXISTS pgqrs_workers (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    hostname TEXT NOT NULL,
    port INTEGER NOT NULL,
    queue_id BIGINT,  -- Nullable for Admin workers not tied to a specific queue
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    heartbeat_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    shutdown_at TIMESTAMP WITH TIME ZONE,
    status worker_status NOT NULL DEFAULT 'ready'::worker_status,

    UNIQUE(hostname, port),
    CONSTRAINT fk_workers_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id)
);
CREATE INDEX idx_pgqrs_workers_queue_status ON pgqrs_workers(queue_id, status);
CREATE INDEX idx_pgqrs_workers_heartbeat ON pgqrs_workers(heartbeat_at);
