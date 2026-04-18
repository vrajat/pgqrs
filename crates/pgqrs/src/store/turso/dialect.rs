use crate::store::dialect::{
    DbStateSql, MessageSql, QueueSql, RunSql, SqlDialect, StepSql, WorkerSql, WorkflowSql,
};

pub(crate) struct TursoDialect;

impl SqlDialect for TursoDialect {
    const STEP: StepSql = StepSql {
        get: r#"
    SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
    FROM pgqrs_workflow_steps
    WHERE id = ?
"#,
        list: r#"
    SELECT id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
    FROM pgqrs_workflow_steps
    ORDER BY created_at DESC
"#,
        count: r#"
    SELECT COUNT(*) FROM pgqrs_workflow_steps
"#,
        delete: r#"
    DELETE FROM pgqrs_workflow_steps WHERE id = ?
"#,
        acquire: r#"
    INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at, retry_count)
    VALUES (?, ?, 'RUNNING', datetime('now'), 0)
    ON CONFLICT (run_id, step_name) DO UPDATE
    SET status = CASE
        WHEN pgqrs_workflow_steps.status = 'SUCCESS' THEN 'SUCCESS'
        WHEN pgqrs_workflow_steps.status = 'ERROR' THEN 'ERROR'
        ELSE 'RUNNING'
    END,
    started_at = CASE
        WHEN pgqrs_workflow_steps.status IN ('SUCCESS', 'ERROR') THEN pgqrs_workflow_steps.started_at
        ELSE datetime('now')
    END
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
        clear_retry: r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'RUNNING', retry_at = NULL, error = NULL
    WHERE id = ?
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
        complete: r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'SUCCESS', output = ?2, completed_at = datetime('now')
    WHERE id = ?1
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
        fail: r#"
    UPDATE pgqrs_workflow_steps
    SET status = 'ERROR', error = ?2, completed_at = datetime('now'),
        retry_at = ?3, retry_count = ?4
    WHERE id = ?1
    RETURNING id, run_id, step_name, status, input, output, error, created_at, updated_at, retry_at, retry_count
"#,
    };

    const QUEUE: QueueSql = QueueSql {
        insert: r#"
INSERT INTO pgqrs_queues (queue_name)
VALUES (?)
RETURNING id, queue_name, created_at
"#,
        get: r#"
SELECT id, queue_name, created_at
FROM pgqrs_queues
WHERE id = ?
"#,
        get_by_name: r#"
SELECT id, queue_name, created_at
FROM pgqrs_queues
WHERE queue_name = ?
"#,
        list: r#"
SELECT id, queue_name, created_at
FROM pgqrs_queues
ORDER BY created_at DESC
"#,
        delete: r#"
DELETE FROM pgqrs_queues
WHERE id = ?
"#,
        delete_by_name: r#"
DELETE FROM pgqrs_queues
WHERE queue_name = ?
"#,
        exists: r#"
SELECT EXISTS(SELECT 1 FROM pgqrs_queues WHERE queue_name = ?)
"#,
    };

    const RUN: RunSql = RunSql {
        insert: r#"
INSERT INTO pgqrs_workflow_runs (workflow_id, message_id, status, input)
VALUES (?, ?, 'QUEUED', ?)
RETURNING id, workflow_id, message_id, status, input, output, error, cancel_reason, cancelled_at, created_at, updated_at
"#,
        get: r#"
SELECT id, workflow_id, message_id, status, input, output, error, cancel_reason, cancelled_at, created_at, updated_at
FROM pgqrs_workflow_runs
WHERE id = ?
"#,
        list: r#"
SELECT id, workflow_id, message_id, status, input, output, error, cancel_reason, cancelled_at, created_at, updated_at
FROM pgqrs_workflow_runs
ORDER BY created_at DESC
"#,
        count: r#"
SELECT COUNT(*) FROM pgqrs_workflow_runs
"#,
        delete: r#"
DELETE FROM pgqrs_workflow_runs WHERE id = ?
"#,
        start: r#"
UPDATE pgqrs_workflow_runs
SET status = 'RUNNING',
    updated_at = datetime('now'),
    started_at = CASE WHEN status = 'QUEUED' THEN datetime('now') ELSE started_at END
WHERE id = ? AND status IN ('QUEUED', 'PAUSED')
RETURNING id, workflow_id, message_id, status, input, output, error, cancel_reason, cancelled_at, created_at, updated_at
"#,
        get_status: r#"
SELECT status FROM pgqrs_workflow_runs WHERE id = ?
"#,
        complete: r#"
UPDATE pgqrs_workflow_runs
SET status = 'SUCCESS', output = ?2, updated_at = datetime('now'), completed_at = datetime('now')
WHERE id = ?1
"#,
        pause: r#"
UPDATE pgqrs_workflow_runs
SET status = 'PAUSED', error = ?2, paused_at = datetime('now'), updated_at = datetime('now')
WHERE id = ?1
"#,
        cancel: r#"
UPDATE pgqrs_workflow_runs
SET status = 'CANCELLED', cancel_reason = ?2, cancelled_at = datetime('now'), completed_at = datetime('now'), updated_at = datetime('now')
WHERE id = ?1 AND status IN ('QUEUED', 'RUNNING', 'PAUSED')
"#,
        fail: r#"
UPDATE pgqrs_workflow_runs
SET status = 'ERROR', error = ?2, updated_at = datetime('now'), completed_at = datetime('now')
WHERE id = ?1
"#,
        get_by_message_id: r#"
SELECT id, workflow_id, message_id, status, input, output, error, cancel_reason, cancelled_at, created_at, updated_at
FROM pgqrs_workflow_runs
WHERE message_id = ?
"#,
    };

    const MESSAGE: MessageSql = MessageSql {
        list_by_consumer_worker: r#"
SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id, archived_at
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
SELECT COUNT(*)
FROM pgqrs_messages
WHERE producer_worker_id = ? OR consumer_worker_id = ?
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
WHERE id = ? AND status IN ('ready', 'polling', 'interrupted')
"#,
        poll: r#"
UPDATE pgqrs_workers
SET status = 'polling'
WHERE id = ? AND status IN ('ready', 'polling')
"#,
        interrupt: r#"
UPDATE pgqrs_workers
SET status = 'interrupted'
WHERE id = ? AND status = 'polling'
"#,
        shutdown: r#"
UPDATE pgqrs_workers
SET status = 'stopped', shutdown_at = ?2
WHERE id = ?1 AND status = 'suspended'
"#,
        complete_poll: r#"
UPDATE pgqrs_workers
SET status = 'ready'
WHERE id = ? AND status = 'polling'
"#,
        heartbeat: r#"
UPDATE pgqrs_workers SET heartbeat_at = ? WHERE id = ?
"#,
        resume: r#"
UPDATE pgqrs_workers SET status = 'ready' WHERE id = ? AND status = 'suspended'
"#,
    };

    const WORKFLOW: WorkflowSql = WorkflowSql {
        get_by_name: r#"
SELECT id, name, queue_id, created_at
FROM pgqrs_workflows
WHERE name = ?
"#,
        insert: r#"
INSERT INTO pgqrs_workflows (name, queue_id, created_at)
VALUES (?, ?, ?)
RETURNING id, name, queue_id, created_at
"#,
        get: r#"
SELECT id, name, queue_id, created_at
FROM pgqrs_workflows
WHERE id = ?
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
DELETE FROM pgqrs_workflows WHERE id = ?
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
    q.queue_name,
    COUNT(m.id),
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.archived_at IS NOT NULL THEN 1 ELSE 0 END), 0),
    MIN(CASE WHEN m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN m.enqueued_at END),
    MAX(m.enqueued_at)
FROM pgqrs_queues q
LEFT JOIN pgqrs_messages m ON q.id = m.queue_id
WHERE q.id = ?
GROUP BY q.id, q.queue_name
"#,
        all_queues_metrics: r#"
SELECT
    q.queue_name,
    COUNT(m.id),
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.consumer_worker_id IS NOT NULL AND m.archived_at IS NULL THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN m.id IS NOT NULL AND m.archived_at IS NOT NULL THEN 1 ELSE 0 END), 0),
    MIN(CASE WHEN m.consumer_worker_id IS NULL AND m.archived_at IS NULL THEN m.enqueued_at END),
    MAX(m.enqueued_at)
FROM pgqrs_queues q
LEFT JOIN pgqrs_messages m ON q.id = m.queue_id AND m.archived_at IS NULL
GROUP BY q.id, q.queue_name
"#,
        system_stats: r#"
SELECT
    (SELECT COUNT(*) FROM pgqrs_queues),
    (SELECT COUNT(*) FROM pgqrs_workers),
    (SELECT COUNT(*) FROM pgqrs_workers WHERE status = 'ready'),
    (SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NULL),
    (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NULL AND archived_at IS NULL),
    (SELECT COUNT(*) FROM pgqrs_messages WHERE consumer_worker_id IS NOT NULL AND archived_at IS NULL),
    (SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NOT NULL),
    '0.5.0'
"#,
        worker_health_global: r#"
SELECT
    'Global',
    COUNT(*),
    SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'polling' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'interrupted' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'stopped' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status IN ('ready', 'polling') AND heartbeat_at < ? THEN 1 ELSE 0 END)
FROM pgqrs_workers
"#,
        worker_health_by_queue: r#"
SELECT
    COALESCE(q.queue_name, 'Admin'),
    COUNT(w.id),
    SUM(CASE WHEN w.status = 'ready' THEN 1 ELSE 0 END),
    SUM(CASE WHEN w.status = 'polling' THEN 1 ELSE 0 END),
    SUM(CASE WHEN w.status = 'interrupted' THEN 1 ELSE 0 END),
    SUM(CASE WHEN w.status = 'suspended' THEN 1 ELSE 0 END),
    SUM(CASE WHEN w.status = 'stopped' THEN 1 ELSE 0 END),
    SUM(CASE WHEN w.status IN ('ready', 'polling') AND w.heartbeat_at < ? THEN 1 ELSE 0 END)
FROM pgqrs_workers w
LEFT JOIN pgqrs_queues q ON w.queue_id = q.id
GROUP BY q.queue_name
"#,
        purge_old_workers: r#"
SELECT w.id FROM pgqrs_workers w
LEFT JOIN pgqrs_messages m ON m.producer_worker_id = w.id OR m.consumer_worker_id = w.id
WHERE w.status = 'stopped'
  AND w.heartbeat_at < ?
  AND m.id IS NULL
GROUP BY w.id
"#,
    };
}
