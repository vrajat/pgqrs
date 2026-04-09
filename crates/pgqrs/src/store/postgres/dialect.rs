use crate::store::dialect::{DbStateSql, MessageSql, SqlDialect, StepSql, WorkerSql};
#[cfg(any(feature = "sqlite", feature = "turso"))]
use crate::store::dialect::{QueueSql, RunSql, WorkflowSql};

pub(crate) struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    const STEP: StepSql = StepSql {
        get: r#"
SELECT
    id,
    run_id,
    step_name,
    status,
    input,
    output,
    error,
    created_at,
    updated_at,
    retry_at,
    retry_count
FROM pgqrs_workflow_steps
WHERE id = $1
"#,
        list: r#"
SELECT
    id,
    run_id,
    step_name,
    status,
    input,
    output,
    error,
    created_at,
    updated_at,
    retry_at,
    retry_count
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
VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, NOW(), 0)
ON CONFLICT (run_id, step_name) DO UPDATE
SET status = CASE
    WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN 'SUCCESS'::pgqrs_workflow_status
    WHEN pgqrs_workflow_steps.status = 'ERROR' THEN 'ERROR'::pgqrs_workflow_status
    ELSE 'RUNNING'::pgqrs_workflow_status
END,
started_at = CASE
    WHEN pgqrs_workflow_steps.status IN ('SUCCESS', 'ERROR') THEN pgqrs_workflow_steps.started_at
    ELSE NOW()
END
RETURNING
    id,
    run_id,
    step_name,
    status,
    input,
    output,
    error,
    created_at,
    updated_at,
    retry_at,
    retry_count
"#,
        clear_retry: r#"
UPDATE pgqrs_workflow_steps
SET status = 'RUNNING'::pgqrs_workflow_status, retry_at = NULL, error = NULL
WHERE id = $1
RETURNING
    id,
    run_id,
    step_name,
    status,
    input,
    output,
    error,
    created_at,
    updated_at,
    retry_at,
    retry_count
"#,
        complete: r#"
UPDATE pgqrs_workflow_steps
SET status = 'SUCCESS'::pgqrs_workflow_status, output = $2, completed_at = NOW()
WHERE id = $1
RETURNING
    id,
    run_id,
    step_name,
    status,
    input,
    output,
    error,
    created_at,
    updated_at,
    retry_at,
    retry_count
"#,
        fail: r#"
UPDATE pgqrs_workflow_steps
SET status = 'ERROR'::pgqrs_workflow_status, error = $2, completed_at = NOW(),
    retry_at = $3, retry_count = $4
WHERE id = $1
RETURNING
    id,
    run_id,
    step_name,
    status,
    input,
    output,
    error,
    created_at,
    updated_at,
    retry_at,
    retry_count
"#,
    };

    #[cfg(any(feature = "sqlite", feature = "turso"))]
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

    #[cfg(any(feature = "sqlite", feature = "turso"))]
    const RUN: RunSql = RunSql {
        insert: r#"
INSERT INTO pgqrs_workflow_runs (workflow_id, message_id, status, input, created_at, updated_at)
VALUES ($1, $2, 'QUEUED', $3, NOW(), NOW())
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
    started_at = CASE WHEN status = 'QUEUED' THEN NOW() ELSE started_at END,
    updated_at = NOW()
WHERE id = $1 AND status IN ('QUEUED', 'PAUSED')
RETURNING id, workflow_id, message_id, status, input, output, error, created_at, updated_at
"#,
        get_status: r#"
SELECT status FROM pgqrs_workflow_runs WHERE id = $1
"#,
        complete: r#"
UPDATE pgqrs_workflow_runs
SET status = 'SUCCESS', output = $2, updated_at = NOW(), completed_at = NOW()
WHERE id = $1
"#,
        pause: r#"
UPDATE pgqrs_workflow_runs
SET status = 'PAUSED', error = $2, paused_at = NOW(), updated_at = NOW()
WHERE id = $1
"#,
        fail: r#"
UPDATE pgqrs_workflow_runs
SET status = 'ERROR', error = $2, updated_at = NOW(), completed_at = NOW()
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
SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
FROM pgqrs_messages
WHERE consumer_worker_id = $1
ORDER BY id
"#,
        count_by_consumer_worker: r#"
SELECT COUNT(*)
FROM pgqrs_messages
WHERE consumer_worker_id = $1 AND archived_at IS NULL
"#,
        count_worker_references: r#"
SELECT COUNT(*) as total_references FROM (
    SELECT 1 FROM pgqrs_messages WHERE producer_worker_id = $1 OR consumer_worker_id = $2
) refs
"#,
        move_to_dlq: r#"
UPDATE pgqrs_messages
SET archived_at = NOW()
WHERE read_ct >= $1 AND archived_at IS NULL
RETURNING id as original_msg_id
"#,
        release_by_consumer_worker: r#"
UPDATE pgqrs_messages
SET vt = NOW(), consumer_worker_id = NULL
WHERE consumer_worker_id = $1 AND archived_at IS NULL
"#,
    };

    const WORKER: WorkerSql = WorkerSql {
        mark_stopped: r#"
UPDATE pgqrs_workers
SET status = 'stopped',
    shutdown_at = NOW()
WHERE id = $1
"#,
        complete_poll: r#"
UPDATE pgqrs_workers
SET status = 'ready'
WHERE id = $1 AND status = 'polling'
RETURNING id
"#,
    };

    #[cfg(any(feature = "sqlite", feature = "turso"))]
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
    SELECT 1 FROM information_schema.tables
    WHERE table_name = $1
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
DELETE FROM pgqrs_messages WHERE queue_id = $1
"#,
        purge_queue_workers: r#"
DELETE FROM pgqrs_workers WHERE queue_id = $1
"#,
        queue_metrics: r#"
SELECT
    q.queue_name as name,
    COUNT(m.id) as total_messages,
    COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as pending_messages,
    COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL) as locked_messages,
    COUNT(m.id) FILTER (WHERE m.archived_at IS NOT NULL) as archived_messages,
    MIN(m.enqueued_at) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as oldest_pending_message,
    MAX(m.enqueued_at) as newest_message
FROM pgqrs_queues q
LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
WHERE q.id = $1
GROUP BY q.id, q.queue_name
"#,
        all_queues_metrics: r#"
SELECT
    q.queue_name as name,
    COUNT(m.id) as total_messages,
    COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as pending_messages,
    COUNT(m.id) FILTER (WHERE m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL) as locked_messages,
    COUNT(m.id) FILTER (WHERE m.archived_at IS NOT NULL) as archived_messages,
    MIN(m.enqueued_at) FILTER (WHERE m.consumer_worker_id IS NULL AND m.archived_at IS NULL) as oldest_pending_message,
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
    COUNT(*) FILTER (WHERE status = 'ready') as ready_workers,
    COUNT(*) FILTER (WHERE status = 'polling') as polling_workers,
    COUNT(*) FILTER (WHERE status = 'interrupted') as interrupted_workers,
    COUNT(*) FILTER (WHERE status = 'suspended') as suspended_workers,
    COUNT(*) FILTER (WHERE status = 'stopped') as stopped_workers,
    COUNT(*) FILTER (WHERE status IN ('ready', 'polling') AND heartbeat_at < $1) as stale_workers
FROM pgqrs_workers
"#,
        worker_health_by_queue: r#"
SELECT
    COALESCE(q.queue_name, 'Admin') as queue_name,
    COUNT(w.id) as total_workers,
    COUNT(w.id) FILTER (WHERE w.status = 'ready') as ready_workers,
    COUNT(w.id) FILTER (WHERE w.status = 'polling') as polling_workers,
    COUNT(w.id) FILTER (WHERE w.status = 'interrupted') as interrupted_workers,
    COUNT(w.id) FILTER (WHERE w.status = 'suspended') as suspended_workers,
    COUNT(w.id) FILTER (WHERE w.status = 'stopped') as stopped_workers,
    COUNT(w.id) FILTER (WHERE w.status IN ('ready', 'polling') AND w.heartbeat_at < $1) as stale_workers
FROM pgqrs_workers w
LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
GROUP BY q.queue_name
"#,
        purge_old_workers: r#"
DELETE FROM pgqrs_workers
WHERE status = 'stopped'
  AND heartbeat_at < $1
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
