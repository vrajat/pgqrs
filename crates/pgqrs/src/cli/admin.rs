use pgqrs::Store;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, sqlx::FromRow)]
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

pub async fn run(
    dsn: String,
    schema: String,
    interval_ms: u64,
    heartbeat_timeout_secs: i64,
    workflow_timeout_secs: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting pgqrs admin coordinator daemon...");
    println!("Schema: {}", schema);
    println!("Check Interval: {}ms", interval_ms);
    println!("Heartbeat Timeout: {}s", heartbeat_timeout_secs);
    println!("Workflow Timeout: {}s", workflow_timeout_secs);

    let config = pgqrs::Config::from_dsn_with_schema(&dsn, &schema)?;
    let store = pgqrs::connect_with_config(&config).await?;

    // Make sure tables are migrated / bootstrapped
    store.bootstrap().await?;

    let interval = Duration::from_millis(interval_ms);
    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);

    println!("pgqrs admin daemon is running. Press Ctrl+C to stop.");

    loop {
        tokio::select! {
            _ = &mut sigint => {
                println!("Received shutdown signal. Stopping pgqrs admin...");
                break;
            }
            _ = sleep(interval) => {
                // 1. Run Cron Schedule Scanner loop
                loop {
                    match scan_schedules_once(&store).await {
                        Ok(true) => {
                            // Triggered a schedule, check immediately for more due schedules without delay
                            continue;
                        }
                        Ok(false) => {
                            // No schedules due
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error in schedule scanner: {}", e);
                            break;
                        }
                    }
                }

                // 2. Run Maintenance Sweeper
                if let Err(e) = run_maintenance_sweep(&store, heartbeat_timeout_secs, workflow_timeout_secs).await {
                    eprintln!("Error in maintenance sweep: {}", e);
                }
            }
        }
    }

    Ok(())
}

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

    let schedule = cron::Schedule::from_str(&cron_str).map_err(|e| {
        anyhow::anyhow!("Invalid cron or interval expression '{}': {}", expr, e)
    })?;

    let next = schedule
        .upcoming(chrono::Utc)
        .next()
        .ok_or_else(|| anyhow::anyhow!("No upcoming execution time for schedule '{}'", expr))?;

    Ok(next)
}

async fn scan_schedules_once(store: &Store) -> anyhow::Result<bool> {
    let mut tx = store.pool().begin().await?;

    // 1. SELECT next active schedule due for fire
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
            return Ok(false); // No due schedule found
        }
    };

    // 2. Resolve queue_id for the workflow (workflow backing queue name is same as workflow_name)
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

    // 3. Enqueue trigger message
    let payload = serde_json::json!({
        "input": row.input.unwrap_or(serde_json::Value::Null)
    });

    let message_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO pgqrs_messages (queue_id, payload, vt, enqueued_at)
        VALUES ($1, $2, NOW(), NOW())
        RETURNING id
        "#,
    )
    .bind(queue_id)
    .bind(&payload)
    .fetch_one(&mut *tx)
    .await?;

    // 4. Calculate next fire time
    let next_fire = match parse_cron_or_interval(&row.cron_expression, chrono::Utc::now()) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "Error parsing schedule expression '{}' for schedule '{}': {}",
                row.cron_expression, row.name, e
            );
            // If the expression is invalid, pause the schedule to avoid looping indefinitely on an invalid config
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
            return Ok(true); // Handled invalid schedule
        }
    };

    // 5. Update next fire time
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
    println!(
        "Triggered schedule '{}' (workflow: '{}'), enqueued message_id={}, next_fire_at={}",
        row.name, row.workflow_name, message_id, next_fire
    );
    Ok(true)
}

async fn run_maintenance_sweep(
    store: &Store,
    heartbeat_timeout_secs: i64,
    workflow_timeout_secs: i64,
) -> anyhow::Result<()> {
    // 1. Worker Health: Mark stale workers as stopped.
    let stopped_count = sqlx::query(
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
    .await?
    .rows_affected();

    if stopped_count > 0 {
        println!("Marked {} stale worker(s) as stopped", stopped_count);
    }

    // 2. Lease Reclamation: Reset visibility timeouts and worker assignments
    // for active messages leased to stale or stopped workers.
    let reclaimed_count = sqlx::query(
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
    .await?
    .rows_affected();

    if reclaimed_count > 0 {
        println!(
            "Reclaimed {} expired lease(s) from stale/stopped workers",
            reclaimed_count
        );
    }

    // 3. Workflow Timeout: Scan running workflow runs that have exceeded their timeout
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
        println!(
            "Timed out {} workflow run(s): {:?}",
            timed_out_ids.len(),
            timed_out_ids
        );

        // Abort outstanding steps for these timed-out workflow runs
        let aborted_steps = sqlx::query(
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
        .await?
        .rows_affected();

        if aborted_steps > 0 {
            println!(
                "Aborted {} outstanding step(s) for timed-out workflow runs",
                aborted_steps
            );
        }
    }

    Ok(())
}
