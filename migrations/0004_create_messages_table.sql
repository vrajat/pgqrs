-- Migration: Create pgqrs_messages table and indexes
CREATE TABLE pgqrs_messages (
    id BIGSERIAL PRIMARY KEY,
    queue_id BIGINT NOT NULL,
    worker_id BIGINT,
    payload JSONB NOT NULL,
    vt TIMESTAMPTZ DEFAULT NOW(),
    enqueued_at TIMESTAMPTZ DEFAULT NOW(),
    read_ct INT DEFAULT 0,
    dequeued_at TIMESTAMPTZ,
    CONSTRAINT fk_messages_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id),
    CONSTRAINT fk_messages_worker_id FOREIGN KEY (worker_id) REFERENCES pgqrs_workers(id)
);
CREATE INDEX idx_pgqrs_messages_queue_vt ON pgqrs_messages (queue_id, vt);
CREATE INDEX idx_pgqrs_messages_worker_id ON pgqrs_messages (worker_id);
