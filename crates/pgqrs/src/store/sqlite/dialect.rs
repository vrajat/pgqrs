use crate::store::dialect::{
    DbStateSql, MessageSql, QueueSql, RunSql, SqlDialect, StepSql, WorkerSql, WorkflowSql,
};

pub(crate) struct SqliteDialect;

const SQLITE_STEP_SQL: StepSql = StepSql {
    get: r#"
SELECT id, run_id, step_name, status, input, output, error, retry_at, retry_count, created_at, updated_at
FROM pgqrs_workflow_steps
WHERE id = $1
"#,
    list: r#"
SELECT id, run_id, step_name, status, input, output, error, retry_at, retry_count, created_at, updated_at
FROM pgqrs_workflow_steps
ORDER BY created_at DESC
"#,
    count: r#"
SELECT COUNT(*) FROM pgqrs_workflow_steps
"#,
    delete: r#"
DELETE FROM pgqrs_workflow_steps WHERE id = $1
"#,
    acquire: r#"
INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
VALUES ($1, $2, 'RUNNING', datetime('now'), 0)
ON CONFLICT (run_id, step_name) DO UPDATE
SET status = CASE
    WHEN status = 'SUCCESS' THEN 'SUCCESS'
    WHEN status = 'ERROR' THEN 'ERROR'
    ELSE 'RUNNING'
END,
started_at = CASE
    WHEN status IN ('SUCCESS', 'ERROR') THEN started_at
    ELSE datetime('now')
END
RETURNING id, run_id, step_name, status, input, output, error, retry_at, retry_count, created_at, updated_at
"#,
    clear_retry: r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING', retry_at = NULL, error = NULL
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_at, retry_count, created_at, updated_at
"#,
    complete: r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS', output = $2, completed_at = datetime('now')
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_at, retry_count, created_at, updated_at
"#,
    fail: r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR', error = $2, completed_at = datetime('now'),
    retry_at = $3, retry_count = $4
WHERE id = $1
RETURNING id, run_id, step_name, status, input, output, error, retry_at, retry_count, created_at, updated_at
"#,
};

impl SqlDialect for SqliteDialect {
    const STEP: StepSql = SQLITE_STEP_SQL;

    const QUEUE: QueueSql = QueueSql {
        insert: r#"
INSERT INTO pgqrs_queues (queue_name)
VALUES ($1)
RETURNING id, queue_name, created_at
"#,
        get: r#"
SELECT id, queue_name, created_at
FROM pgqrs_queues
WHERE id = $1
"#,
        get_by_name: r#"
SELECT id, queue_name, created_at
FROM pgqrs_queues
WHERE queue_name = $1
"#,
        list: r#"
SELECT id, queue_name, created_at
FROM pgqrs_queues
ORDER BY created_at DESC
"#,
        delete: r#"
DELETE FROM pgqrs_queues
WHERE id = $1
"#,
        delete_by_name: r#"
DELETE FROM pgqrs_queues
WHERE queue_name = $1
"#,
        exists: r#"
SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = $1)
"#,
    };

    const RUN: RunSql = RunSql {
        insert: r#"
INSERT INTO pgqrs_workflow_runs (workflow_id, message_id, status, input, created_at, updated_at)
VALUES ($1, $2, 'QUEUED', $3, $4, $4)
RETURNING id, workflow_id, message_id, status, input, output, error, created_at, updated_at
"#,
        get: r#"
SELECT id, workflow_id, message_id, status, input, output, error, created_at, updated_at
FROM pgqrs_workflow_runs
WHERE id = $1
"#,
        list: r#"
SELECT id, workflow_id, message_id, status, input, output, error, created_at, updated_at
FROM pgqrs_workflow_runs
ORDER BY created_at DESC
"#,
        count: r#"
SELECT COUNT(*) FROM pgqrs_workflow_runs
"#,
        delete: r#"
DELETE FROM pgqrs_workflow_runs WHERE id = $1
"#,
        start: r#"
UPDATE pgqrs_workflow_runs
SET status = 'RUNNING',
    started_at = CASE WHEN status = 'QUEUED' THEN datetime('now') ELSE started_at END,
    updated_at = datetime('now')
WHERE id = $1 AND status IN ('QUEUED', 'PAUSED')
RETURNING id, workflow_id, message_id, status, input, output, error, created_at, updated_at
"#,
        get_status: r#"
SELECT status FROM pgqrs_workflow_runs WHERE id = $1
"#,
        complete: r#"
UPDATE pgqrs_workflow_runs
SET status = 'SUCCESS', output = $2, updated_at = $3
WHERE id = $1
"#,
        pause: r#"
UPDATE pgqrs_workflow_runs
SET status = 'PAUSED', error = $2, paused_at = $3, updated_at = $3
WHERE id = $1
"#,
        cancel: r#"
UPDATE pgqrs_workflow_runs
SET status = 'CANCELLING', updated_at = $2
WHERE id = $1 AND status IN ('QUEUED', 'RUNNING', 'PAUSED')
"#,
        complete_cancel: r#"
UPDATE pgqrs_workflow_runs
SET status = 'CANCELLED', updated_at = $2, completed_at = $2
WHERE id = $1 AND status = 'CANCELLING'
"#,
        fail: r#"
UPDATE pgqrs_workflow_runs
SET status = 'ERROR', error = $2, updated_at = $3
WHERE id = $1
"#,
        get_by_message_id: r#"
SELECT id, workflow_id, message_id, status, input, output, error, created_at, updated_at
FROM pgqrs_workflow_runs
WHERE message_id = $1
"#,
    };

    const MESSAGE: MessageSql = MessageSql {
        list_by_consumer_worker: r#"
SELECT id, queue_id, producer_worker_id, consumer_worker_id, payload, vt, enqueued_at, read_ct, dequeued_at, archived_at
FROM pgqrs_messages
WHERE consumer_worker_id = ?
ORDER BY id
"#,
        count_by_consumer_worker: r#"
SELECT COUNT(*)
FROM pgqrs_messages
WHERE consumer_worker_id = ? AND archived_at IS NULL
"#,
        count_worker_references: r#"
SELECT COUNT(*) as total_references FROM (
    SELECT 1 FROM pgqrs_messages WHERE producer_worker_id = ? OR consumer_worker_id = ?
) refs
"#,
        move_to_dlq: r#"
UPDATE pgqrs_messages
SET archived_at = datetime('now')
WHERE read_ct >= ? AND archived_at IS NULL
RETURNING id
"#,
        release_by_consumer_worker: r#"
UPDATE pgqrs_messages
SET vt = datetime('now'), consumer_worker_id = NULL
WHERE consumer_worker_id = ? AND archived_at IS NULL
"#,
    };

    const WORKER: WorkerSql = WorkerSql {
        mark_stopped: r#"
UPDATE pgqrs_workers
SET status = 'stopped',
    shutdown_at = datetime('now')
WHERE id = ?
"#,
        suspend: r#"
UPDATE pgqrs_workers
SET status = 'suspended'
WHERE id = $1 AND status IN ('ready', 'polling', 'interrupted')
"#,
        poll: r#"
UPDATE pgqrs_workers
SET status = 'polling'
WHERE id = $1 AND status IN ('ready', 'polling')
"#,
        interrupt: r#"
UPDATE pgqrs_workers
SET status = 'interrupted'
WHERE id = $1 AND status = 'polling'
"#,
        shutdown: r#"
UPDATE pgqrs_workers
SET status = 'stopped', shutdown_at = $2
WHERE id = $1 AND status = 'suspended'
"#,
        complete_poll: r#"
UPDATE pgqrs_workers
SET status = 'ready'
WHERE id = $1 AND status = 'polling'
"#,
        heartbeat: r#"
UPDATE pgqrs_workers SET heartbeat_at = $1 WHERE id = $2
"#,
        resume: r#"
UPDATE pgqrs_workers SET status = 'ready' WHERE id = $1 AND status = 'suspended'
"#,
    };

    const WORKFLOW: WorkflowSql = WorkflowSql {
        get_by_name: r#"
SELECT id, name, queue_id, created_at
FROM pgqrs_workflows
WHERE name = $1
"#,
        insert: r#"
INSERT INTO pgqrs_workflows (name, queue_id, created_at)
VALUES ($1, $2, $3)
RETURNING id, name, queue_id, created_at
"#,
        get: r#"
SELECT id, name, queue_id, created_at
FROM pgqrs_workflows
WHERE id = $1
"#,
        list: r#"
SELECT id, name, queue_id, created_at
FROM pgqrs_workflows
ORDER BY created_at DESC
"#,
        count: r#"
SELECT COUNT(*) FROM pgqrs_workflows
"#,
        delete: r#"
DELETE FROM pgqrs_workflows WHERE id = $1
"#,
    };

    const DB_STATE: DbStateSql = DbStateSql {
        check_table_exists: r#"
SELECT EXISTS (
    SELECT 1 FROM sqlite_master
    WHERE type = 'table' AND name = ?
)
"#,
        check_orphaned_messages: r#"
SELECT COUNT(*)
FROM pgqrs_messages m
LEFT OUTER JOIN pgqrs_queues q ON m.queue_id = q.id
WHERE q.id IS NULL
"#,
        check_orphaned_message_workers: r#"
SELECT COUNT(*)
FROM pgqrs_messages m
LEFT OUTER JOIN pgqrs_workers pw ON m.producer_worker_id = pw.id
LEFT OUTER JOIN pgqrs_workers cw ON m.consumer_worker_id = cw.id
WHERE (m.producer_worker_id IS NOT NULL AND pw.id IS NULL)
   OR (m.consumer_worker_id IS NOT NULL AND cw.id IS NULL)
"#,
        purge_queue_messages: r#"
DELETE FROM pgqrs_messages WHERE queue_id = ?
"#,
        purge_queue_workers: r#"
DELETE FROM pgqrs_workers WHERE queue_id = ?
"#,
        queue_metrics: r#"
SELECT
    q.queue_name as name,
    COUNT(m.id) as total_messages,
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0) as pending_messages,
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0) as locked_messages,
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.archived_at IS NOT NULL THEN 1 ELSE 0 END), 0) as archived_messages,
    MIN(CASE WHEN m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN m.enqueued_at END) as oldest_pending_message,
    MAX(m.enqueued_at) as newest_message
FROM pgqrs_queues q
LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
WHERE q.id = ?
GROUP BY q.id, q.queue_name
"#,
        all_queues_metrics: r#"
SELECT
    q.queue_name as name,
    COUNT(m.id) as total_messages,
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0) as pending_messages,
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0) as locked_messages,
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.archived_at IS NOT NULL THEN 1 ELSE 0 END), 0) as archived_messages,
    MIN(CASE WHEN m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN m.enqueued_at END) as oldest_pending_message,
    MAX(m.enqueued_at) as newest_message
FROM pgqrs_queues q
LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
GROUP BY q.id, q.queue_name
"#,
        system_stats: r#"
SELECT
    (SELECT COUNT(*) FROM pgqrs_queues) as total_queues,
    (SELECT COUNT(*) FROM pgqrs_workers) as total_workers,
    (SELECT COUNT(*) FROM pgqrs_workers WHERE status = 'ready') as active_workers,
    (SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NULL) as total_messages,
    (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NULL AND archived_at IS NULL) as pending_messages,
    (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL AND archived_at IS NULL) as locked_messages,
    (SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NOT NULL) as archived_messages,
    '0.5.0' as schema_version
"#,
        worker_health_global: r#"
SELECT
    'Global' as queue_name,
    COUNT(*) as total_workers,
    SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) as ready_workers,
    SUM(CASE WHEN status = 'polling' THEN 1 ELSE 0 END) as polling_workers,
    SUM(CASE WHEN status = 'interrupted' THEN 1 ELSE 0 END) as interrupted_workers,
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) as suspended_workers,
    SUM(CASE WHEN status = 'stopped' THEN 1 ELSE 0 END) as stopped_workers,
    SUM(CASE WHEN status IN ('ready', 'polling') AND heartbeat_at < ? THEN 1 ELSE 0 END) as stale_workers
FROM pgqrs_workers
"#,
        worker_health_by_queue: r#"
SELECT
    COALESCE(q.queue_name, 'Admin') as queue_name,
    COUNT(w.id) as total_workers,
    SUM(CASE WHEN w.status = 'ready' THEN 1 ELSE 0 END) as ready_workers,
    SUM(CASE WHEN w.status = 'polling' THEN 1 ELSE 0 END) as polling_workers,
    SUM(CASE WHEN w.status = 'interrupted' THEN 1 ELSE 0 END) as interrupted_workers,
    SUM(CASE WHEN w.status = 'suspended' THEN 1 ELSE 0 END) as suspended_workers,
    SUM(CASE WHEN w.status = 'stopped' THEN 1 ELSE 0 END) as stopped_workers,
    SUM(CASE WHEN w.status IN ('ready', 'polling') AND w.heartbeat_at < ? THEN 1 ELSE 0 END) as stale_workers
FROM pgqrs_workers w
LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
GROUP BY q.queue_name
"#,
        purge_old_workers: r#"
DELETE FROM pgqrs_workers
WHERE status = 'stopped'
  AND heartbeat_at < ?
  AND id NOT IN (
      SELECT DISTINCT worker_id
      FROM (
          SELECT producer_worker_id as worker_id FROM pgqrs_messages WHERE producer_worker_id IS NOT NULL
          UNION
          SELECT consumer_worker_id as worker_id FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL
      ) refs
  )
"#,
    };
}
