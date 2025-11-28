-- Migration: Create pgqrs_archive table and indexes
CREATE TABLE pgqrs_archive (
    id BIGSERIAL PRIMARY KEY,
    original_msg_id BIGINT NOT NULL,
    queue_id BIGINT NOT NULL,
    worker_id BIGINT,
    payload JSONB NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL,
    vt TIMESTAMPTZ NOT NULL,
    read_ct INT NOT NULL,
    archived_at TIMESTAMPTZ DEFAULT NOW(),
    dequeued_at TIMESTAMPTZ,
    CONSTRAINT fk_archive_queue_id FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id),
    CONSTRAINT fk_archive_worker_id FOREIGN KEY (worker_id) REFERENCES pgqrs_workers(id)
);
CREATE INDEX idx_pgqrs_archive_queue_id ON pgqrs_archive (queue_id);
CREATE INDEX idx_pgqrs_archive_archived_at ON pgqrs_archive (archived_at);
CREATE INDEX idx_pgqrs_archive_original_msg_id ON pgqrs_archive (original_msg_id);
