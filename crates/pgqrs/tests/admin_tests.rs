use chrono::{Duration, Utc};
use serde_json::json;

use pgqrs::types::WorkerStatus;
use pgqrs::Store;

mod common;

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ScheduleRow {
    id: i64,
    name: String,
    cron_expression: String,
    workflow_name: String,
    input: Option<serde_json::Value>,
}

#[derive(sqlx::FromRow)]
struct TimedOutRun {
    id: i64,
}

#[derive(sqlx::FromRow)]
struct SchedCheck {
    status: String,
    next_fire_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct RunCheck {
    status: String,
    error: Option<serde_json::Value>,
}

#[derive(sqlx::FromRow)]
struct StepCheck {
    status: String,
    error: Option<serde_json::Value>,
}

// Re-implement or import the helper functions for testing
fn parse_cron_or_interval(
    expr: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let expr = expr.trim();
    let parts: Vec<&str> = expr.split_whitespace().collect();

    // 1. Try parsing as an interval
    if parts.len() == 2 {
        if let Ok(amount) = parts[0].parse::<i64>() {
            let unit = parts[1].to_lowercase();
            let seconds = if unit.starts_with("second") {
                Some(amount)
            } else if unit.starts_with("minute") {
                Some(amount * 60)
            } else if unit.starts_with("hour") {
                Some(amount * 3600)
            } else if unit.starts_with("day") {
                Some(amount * 86400)
            } else if unit.starts_with("week") {
                Some(amount * 86400 * 7)
            } else {
                None
            };
            if let Some(secs) = seconds {
                return Ok(now + chrono::Duration::seconds(secs));
            }
        }
    }

    // 2. Try parsing as cron expression
    let cron_str = if parts.len() == 5 {
        format!("0 {}", expr)
    } else {
        expr.to_string()
    };

    use std::str::FromStr;
    let schedule = cron::Schedule::from_str(&cron_str)
        .map_err(|e| anyhow::anyhow!("Invalid cron or interval expression '{}': {}", expr, e))?;

    let next = schedule
        .upcoming(chrono::Utc)
        .next()
        .ok_or_else(|| anyhow::anyhow!("No upcoming execution time for schedule '{}'", expr))?;

    Ok(next)
}

async fn scan_schedules_once(store: &Store) -> anyhow::Result<bool> {
    let mut tx = store.pool().begin().await?;

    let row_opt = sqlx::query_as::<_, ScheduleRow>(
        r#"
        SELECT id, name, cron_expression, workflow_name, input
        FROM pgqrs_schedules
        WHERE status = 'active'
          AND next_fire_at <= NOW()
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .fetch_optional(&mut *tx)
    .await?;

    let row = match row_opt {
        Some(r) => r,
        None => {
            tx.commit().await?;
            return Ok(false);
        }
    };

    let queue_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO pgqrs_queues (queue_name)
        VALUES ($1)
        ON CONFLICT (queue_name) DO UPDATE SET queue_name = EXCLUDED.queue_name
        RETURNING id
        "#,
    )
    .bind(&row.workflow_name)
    .fetch_one(&mut *tx)
    .await?;

    let payload = serde_json::json!({
        "input": row.input.unwrap_or(serde_json::Value::Null)
    });

    sqlx::query(
        r#"
        INSERT INTO pgqrs_messages (queue_id, payload, vt, enqueued_at)
        VALUES ($1, $2, NOW(), NOW())
        RETURNING id
        "#,
    )
    .bind(queue_id)
    .bind(&payload)
    .execute(&mut *tx)
    .await?;

    let next_fire = match parse_cron_or_interval(&row.cron_expression, chrono::Utc::now()) {
        Ok(t) => t,
        Err(_) => {
            sqlx::query(
                r#"
                UPDATE pgqrs_schedules
                SET status = 'paused',
                    updated_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(row.id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(true);
        }
    };

    sqlx::query(
        r#"
        UPDATE pgqrs_schedules
        SET next_fire_at = $2,
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(row.id)
    .bind(next_fire)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

async fn run_maintenance_sweep(
    store: &Store,
    heartbeat_timeout_secs: i64,
    workflow_timeout_secs: i64,
) -> anyhow::Result<()> {
    // 1. Worker Health
    sqlx::query(
        r#"
        UPDATE pgqrs_workers
        SET status = 'stopped'::worker_status,
            shutdown_at = NOW()
        WHERE status IN ('ready'::worker_status, 'polling'::worker_status, 'suspended'::worker_status, 'interrupted'::worker_status)
          AND heartbeat_at < NOW() - make_interval(secs => $1::double precision)
        "#
    )
    .bind(heartbeat_timeout_secs as f64)
    .execute(store.pool())
    .await?;

    // 2. Lease Reclamation
    sqlx::query(
        r#"
        UPDATE pgqrs_messages
        SET vt = NOW(),
            consumer_worker_id = NULL
        WHERE consumer_worker_id IS NOT NULL
          AND vt <= NOW()
          AND archived_at IS NULL
          AND consumer_worker_id IN (
              SELECT id FROM pgqrs_workers
              WHERE status = 'stopped'
                 OR heartbeat_at < NOW() - make_interval(secs => $1::double precision)
          )
        "#,
    )
    .bind(heartbeat_timeout_secs as f64)
    .execute(store.pool())
    .await?;

    // 3. Workflow Timeout
    let timed_out_runs = sqlx::query_as::<_, TimedOutRun>(
        r#"
        UPDATE pgqrs_workflow_runs
        SET status = 'ERROR'::pgqrs_workflow_status,
            error = '{"message": "Workflow run execution timed out"}'::jsonb,
            completed_at = NOW(),
            updated_at = NOW()
        WHERE status = 'RUNNING'::pgqrs_workflow_status
          AND started_at < NOW() - make_interval(secs => $1::double precision)
        RETURNING id
        "#,
    )
    .bind(workflow_timeout_secs as f64)
    .fetch_all(store.pool())
    .await?;

    if !timed_out_runs.is_empty() {
        let timed_out_ids: Vec<i64> = timed_out_runs.iter().map(|r| r.id).collect();
        sqlx::query(
            r#"
            UPDATE pgqrs_workflow_steps
            SET status = 'ERROR'::pgqrs_workflow_status,
                error = '{"message": "Workflow run execution timed out"}'::jsonb,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE run_id = ANY($1)
              AND status IN ('RUNNING'::pgqrs_workflow_status, 'QUEUED'::pgqrs_workflow_status)
            "#,
        )
        .bind(&timed_out_ids)
        .execute(store.pool())
        .await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_schedule_scanning_interval() {
    let store = common::create_store("pgqrs_admin_scan_interval_test").await;

    // Create a schedule that is due (interval style)
    let next_fire = Utc::now() - Duration::seconds(10);
    sqlx::query(
        r#"
        INSERT INTO pgqrs_schedules (name, cron_expression, workflow_name, input, status, next_fire_at)
        VALUES ('test_sched_1', '10 seconds', 'target_workflow_1', '{"user_id": 42}'::jsonb, 'active', $1)
        "#
    )
    .bind(next_fire)
    .execute(store.pool())
    .await
    .unwrap();

    // Scan
    let triggered = scan_schedules_once(&store).await.unwrap();
    assert!(triggered);

    // Verify schedule updated
    let sched = sqlx::query_as::<_, SchedCheck>(
        "SELECT status, next_fire_at FROM pgqrs_schedules WHERE name = $1",
    )
    .bind("test_sched_1")
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(sched.status, "active");
    assert!(sched.next_fire_at > Utc::now());

    // Verify message enqueued
    let queue_info = store
        .queues()
        .get_by_name("target_workflow_1")
        .await
        .unwrap();
    let messages = store.messages().filter_by_fk(queue_info.id).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload["input"]["user_id"], 42);
}

#[tokio::test]
async fn test_schedule_scanning_cron() {
    let store = common::create_store("pgqrs_admin_scan_cron_test").await;

    // Create a schedule that is due (cron style)
    let next_fire = Utc::now() - Duration::seconds(10);
    sqlx::query(
        r#"
        INSERT INTO pgqrs_schedules (name, cron_expression, workflow_name, input, status, next_fire_at)
        VALUES ('test_sched_cron', '*/5 * * * *', 'target_workflow_cron', '{"task": "cron"}'::jsonb, 'active', $1)
        "#
    )
    .bind(next_fire)
    .execute(store.pool())
    .await
    .unwrap();

    // Scan
    let triggered = scan_schedules_once(&store).await.unwrap();
    assert!(triggered);

    // Verify schedule updated next fire
    let sched = sqlx::query_as::<_, SchedCheck>(
        "SELECT status, next_fire_at FROM pgqrs_schedules WHERE name = $1",
    )
    .bind("test_sched_cron")
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(sched.status, "active");
    assert!(sched.next_fire_at > Utc::now());
}

#[tokio::test]
async fn test_lease_reclamation() {
    let store = common::create_store("pgqrs_admin_reclaim_test").await;

    // Create queue
    let queue = pgqrs::admin(&store)
        .create_queue("reclaim_queue")
        .await
        .unwrap();

    // Register worker
    let consumer = pgqrs::consumer("reclaim-worker-1", &queue.queue_name)
        .create(&store)
        .await
        .unwrap();

    // Enqueue message
    pgqrs::enqueue()
        .message(&json!({"hello": "world"}))
        .to(&queue.queue_name)
        .execute(&store)
        .await
        .unwrap();

    // Dequeue message (leases it)
    let leased_msgs = pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .unwrap();

    assert_eq!(leased_msgs.len(), 1);

    // Make worker heartbeat stale (older than 30s)
    let stale_heartbeat = Utc::now() - Duration::seconds(40);
    sqlx::query("UPDATE pgqrs_workers SET heartbeat_at = $1 WHERE id = $2")
        .bind(stale_heartbeat)
        .bind(consumer.worker_id())
        .execute(store.pool())
        .await
        .unwrap();

    // Make message vt expired
    let expired_vt = Utc::now() - Duration::seconds(10);
    sqlx::query("UPDATE pgqrs_messages SET vt = $1 WHERE id = $2")
        .bind(expired_vt)
        .bind(leased_msgs[0].id)
        .execute(store.pool())
        .await
        .unwrap();

    // Run sweep (heartbeat_timeout = 30s)
    run_maintenance_sweep(&store, 30, 3600).await.unwrap();

    // Verify worker is marked Stopped
    let worker = pgqrs::tables(&store)
        .workers()
        .get(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(worker.status, WorkerStatus::Stopped);

    // Verify message consumer_worker_id is reset and vt is reset to now
    let msg = store.messages().get(leased_msgs[0].id).await.unwrap();
    assert!(msg.consumer_worker_id.is_none());
    assert!(msg.vt >= Utc::now() - Duration::seconds(2));
}

#[tokio::test]
async fn test_workflow_run_timeout() {
    let store = common::create_store("pgqrs_admin_timeout_test").await;

    // Create workflow backing queue & workflow definition
    let wf_name = "timeout_workflow";
    let queue = pgqrs::admin(&store).create_queue(wf_name).await.unwrap();

    sqlx::query(
        "INSERT INTO pgqrs_workflows (name, queue_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(wf_name)
    .bind(queue.id)
    .execute(store.pool())
    .await
    .unwrap();

    let wf_id: i64 = sqlx::query_scalar("SELECT id FROM pgqrs_workflows WHERE name = $1")
        .bind(wf_name)
        .fetch_one(store.pool())
        .await
        .unwrap();

    // Create message
    let msg_id: i64 = sqlx::query_scalar(
        "INSERT INTO pgqrs_messages (queue_id, payload) VALUES ($1, '{\"input\": {}}'::jsonb) RETURNING id"
    )
    .bind(queue.id)
    .fetch_one(store.pool())
    .await
    .unwrap();

    // Create a workflow run in RUNNING state
    let run_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO pgqrs_workflow_runs (workflow_id, message_id, status, started_at)
        VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, $3)
        RETURNING id
        "#,
    )
    .bind(wf_id)
    .bind(msg_id)
    .bind(Utc::now() - Duration::seconds(4000)) // Older than 3600s
    .fetch_one(store.pool())
    .await
    .unwrap();

    // Create an outstanding step in RUNNING state
    sqlx::query(
        r#"
        INSERT INTO pgqrs_workflow_steps (run_id, step_name, status, started_at)
        VALUES ($1, 'step_1', 'RUNNING'::pgqrs_workflow_status, NOW())
        "#,
    )
    .bind(run_id)
    .execute(store.pool())
    .await
    .unwrap();

    // Run sweep (workflow_timeout = 3600s)
    run_maintenance_sweep(&store, 30, 3600).await.unwrap();

    // Verify run status is ERROR
    let run = sqlx::query_as::<_, RunCheck>(
        "SELECT status::text, error FROM pgqrs_workflow_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(run.status, "ERROR");
    assert_eq!(
        run.error.unwrap()["message"],
        "Workflow run execution timed out"
    );

    // Verify step status is ERROR
    let step = sqlx::query_as::<_, StepCheck>(
        "SELECT status::text, error FROM pgqrs_workflow_steps WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(step.status, "ERROR");
    assert_eq!(
        step.error.unwrap()["message"],
        "Workflow run execution timed out"
    );
}
