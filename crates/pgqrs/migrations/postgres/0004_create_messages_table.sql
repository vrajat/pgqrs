-- Migration: Create pgqrs_messages table and indexes
CREATE TABLE pgqrs_messages (
    id BIGSERIAL PRIMARY KEY,
    queue_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    vt TIMESTAMPTZ DEFAULT NOW(),
    enqueued_at TIMESTAMPTZ DEFAULT NOW(),
    read_ct INT DEFAULT 0,
    dequeued_at TIMESTAMPTZ,
    producer_worker_id BIGINT,
    consumer_worker_id BIGINT,
    archived_at TIMESTAMPTZ,
    CONSTRAINT fk_messages_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id),
    CONSTRAINT fk_messages_producer_worker_id FOREIGN KEY (producer_worker_id) REFERENCES pgqrs_workers(id),
    CONSTRAINT fk_messages_consumer_worker_id FOREIGN KEY (consumer_worker_id) REFERENCES pgqrs_workers(id)
);

-- Partial index for the "hot set" (ready or leased messages)
CREATE INDEX idx_pgqrs_messages_hot_path ON pgqrs_messages (queue_id, vt, enqueued_at, id) WHERE archived_at IS NULL;

-- Index for active consumer worker lookups
CREATE INDEX idx_pgqrs_messages_consumer_worker_id_active ON pgqrs_messages (consumer_worker_id) WHERE archived_at IS NULL AND consumer_worker_id IS NOT NULL;

-- Index for archived messages lookups
CREATE INDEX idx_pgqrs_messages_archived_at ON pgqrs_messages (queue_id, archived_at) WHERE archived_at IS NOT NULL;

-- General index for producer worker lookups
CREATE INDEX idx_pgqrs_messages_producer_worker_id ON pgqrs_messages (producer_worker_id);
