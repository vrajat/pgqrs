-- Migration: Create pgqrs_archive table and indexes
CREATE TABLE pgqrs_archive (
    id BIGSERIAL PRIMARY KEY,
    original_msg_id BIGINT NOT NULL,
    queue_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL,
    vt TIMESTAMPTZ NOT NULL,
    read_ct INT NOT NULL,
    archived_at TIMESTAMPTZ DEFAULT NOW(),
    dequeued_at TIMESTAMPTZ,
    producer_worker_id BIGINT,
    consumer_worker_id BIGINT,
    CONSTRAINT fk_archive_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id),
    CONSTRAINT fk_archive_producer_worker_id FOREIGN KEY (producer_worker_id) REFERENCES pgqrs_workers(id),
    CONSTRAINT fk_archive_consumer_worker_id FOREIGN KEY (consumer_worker_id) REFERENCES pgqrs_workers(id)
);
CREATE INDEX idx_pgqrs_archive_queue_id ON pgqrs_archive (queue_id);
CREATE INDEX idx_pgqrs_archive_archived_at ON pgqrs_archive (archived_at);
CREATE INDEX idx_pgqrs_archive_original_msg_id ON pgqrs_archive (original_msg_id);
CREATE INDEX idx_pgqrs_archive_producer_worker_id ON pgqrs_archive (producer_worker_id);
CREATE INDEX idx_pgqrs_archive_consumer_worker_id ON pgqrs_archive (consumer_worker_id);
