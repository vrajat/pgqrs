use chrono::{Duration, Utc};
use serde_json::json;

use pgqrs::types::WorkerStatus;
use pgqrs::Store;

mod common;

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

// Re-implement thin helpers pointing to library admin API for tests
async fn scan_cron_once(store: &Store) -> anyhow::Result<bool> {
    let mut producers = std::collections::HashMap::new();
    let res = pgqrs::admin(store)
        .scan_cron_batch(&mut producers, 10)
        .await?;
    Ok(res)
}

async fn run_maintenance_sweep(
    store: &Store,
    heartbeat_timeout_secs: i64,
    workflow_timeout_secs: i64,
) -> anyhow::Result<()> {
    pgqrs::admin(store)
        .run_maintenance_sweep(heartbeat_timeout_secs, workflow_timeout_secs)
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_cron_scanning_every_minute() {
    let store = common::create_store("pgqrs_admin_scan_interval_test").await;

    // Create backing queue
    let queue = store
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "target_workflow_1".to_string(),
        })
        .await
        .unwrap();

    // Create a cron schedule that is due (every minute cron style)
    let next_fire = Utc::now() - Duration::seconds(10);
    sqlx::query(
        r#"
        INSERT INTO pgqrs_cron (name, queue_id, cron_expression, input, status, next_fire_at)
        VALUES ('test_cron_1', $1, '* * * * *', '{"user_id": 42}'::jsonb, 'active', $2)
        "#,
    )
    .bind(queue.id)
    .bind(next_fire)
    .execute(store.pool())
    .await
    .unwrap();

    // Scan
    let triggered = scan_cron_once(&store).await.unwrap();
    assert!(triggered);

    // Verify cron updated
    let sched = sqlx::query_as::<_, SchedCheck>(
        "SELECT status, next_fire_at FROM pgqrs_cron WHERE name = $1",
    )
    .bind("test_cron_1")
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(sched.status, "active");
    assert!(sched.next_fire_at > Utc::now());

    // Verify message enqueued
    let messages = store.messages().filter_by_fk(queue.id).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload["input"]["user_id"], 42);
}

#[tokio::test]
async fn test_cron_scanning_custom() {
    let store = common::create_store("pgqrs_admin_scan_cron_test").await;

    // Create backing queue
    let queue = store
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "target_workflow_cron".to_string(),
        })
        .await
        .unwrap();

    // Create a cron that is due
    let next_fire = Utc::now() - Duration::seconds(10);
    sqlx::query(
        r#"
        INSERT INTO pgqrs_cron (name, queue_id, cron_expression, input, status, next_fire_at)
        VALUES ('test_cron_custom', $1, '*/5 * * * *', '{"task": "cron"}'::jsonb, 'active', $2)
        "#,
    )
    .bind(queue.id)
    .bind(next_fire)
    .execute(store.pool())
    .await
    .unwrap();

    // Scan
    let triggered = scan_cron_once(&store).await.unwrap();
    assert!(triggered);

    // Verify cron updated next fire
    let sched = sqlx::query_as::<_, SchedCheck>(
        "SELECT status, next_fire_at FROM pgqrs_cron WHERE name = $1",
    )
    .bind("test_cron_custom")
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(sched.status, "active");
    assert!(sched.next_fire_at > Utc::now());
}

#[tokio::test]
async fn test_cron_constraints() {
    let store = common::create_store("pgqrs_cron_constraints_test").await;

    // 1. Trying to insert a cron with a non-existent queue_id must fail (foreign key constraint)
    let res = sqlx::query(
        r#"
        INSERT INTO pgqrs_cron (name, queue_id, cron_expression, status, next_fire_at)
        VALUES ('test_orphan_cron', 999999, '* * * * *', 'active', NOW())
        "#,
    )
    .execute(store.pool())
    .await;
    assert!(res.is_err()); // Violates FK constraint

    // Create a queue
    let queue = store
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "test_constraint_queue".to_string(),
        })
        .await
        .unwrap();

    // 2. Insert valid cron
    let res = sqlx::query(
        r#"
        INSERT INTO pgqrs_cron (name, queue_id, cron_expression, status, next_fire_at)
        VALUES ('valid_cron_1', $1, '* * * * *', 'active', NOW())
        "#,
    )
    .bind(queue.id)
    .execute(store.pool())
    .await;
    assert!(res.is_ok());

    // 3. Trying to insert a second cron for the same queue_id must fail (unique constraint)
    let res = sqlx::query(
        r#"
        INSERT INTO pgqrs_cron (name, queue_id, cron_expression, status, next_fire_at)
        VALUES ('duplicate_cron_for_queue', $1, '* * * * *', 'active', NOW())
        "#,
    )
    .bind(queue.id)
    .execute(store.pool())
    .await;
    assert!(res.is_err()); // Violates UNIQUE constraint
}

#[tokio::test]
async fn test_cron_crash_recovery_firing() {
    let store = common::create_store("pgqrs_cron_crash_recovery_test").await;

    // Create queue
    let queue = store
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "target_workflow_recovery".to_string(),
        })
        .await
        .unwrap();

    // Create a cron that crashed in the 'firing' state (next_fire_at is in the future, but trigger_state is 'firing')
    let next_fire = Utc::now() + Duration::minutes(5);
    sqlx::query(
        r#"
        INSERT INTO pgqrs_cron (name, queue_id, cron_expression, input, status, trigger_state, next_fire_at)
        VALUES ('crashed_cron', $1, '*/5 * * * *', '{"recovered": true}'::jsonb, 'active', 'firing', $2)
        "#
    )
    .bind(queue.id)
    .bind(next_fire)
    .execute(store.pool())
    .await
    .unwrap();

    // Scan should detect the 'firing' state, recovery-trigger it, and reset state to 'idle'
    let triggered = scan_cron_once(&store).await.unwrap();
    assert!(triggered);

    // Verify cron trigger_state reset to idle
    let sched = sqlx::query_as::<_, SchedCheck>(
        "SELECT status, next_fire_at FROM pgqrs_cron WHERE name = $1",
    )
    .bind("crashed_cron")
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(sched.status, "active");

    let state: pgqrs::types::TriggerState =
        sqlx::query_scalar("SELECT trigger_state FROM pgqrs_cron WHERE name = $1")
            .bind("crashed_cron")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(state, pgqrs::types::TriggerState::Idle);

    // Verify message enqueued
    let messages = store.messages().filter_by_fk(queue.id).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload["input"]["recovered"], true);
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
