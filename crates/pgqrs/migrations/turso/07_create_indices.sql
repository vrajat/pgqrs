CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON pgqrs_workers(heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_workers_queue_status ON pgqrs_workers(queue_id, status);
