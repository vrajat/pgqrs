use clap::{Parser, Subcommand};
use serde_json::Value;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

use pgqrs::{connect_with_config, QueueMessage, Store};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, Row, TypeInfo};

#[derive(Parser, Debug)]
#[command(name = "pgqrs")]
#[command(about = "pgqrs command line utility", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the pgqrs-admin coordinator daemon
    Admin {
        /// Database DSN (can also be set via PGQRS_DSN or DATABASE_URL env vars)
        #[arg(long)]
        dsn: Option<String>,

        /// Database schema namespace
        #[arg(long, default_value = "public", env = "PGQRS_SCHEMA")]
        schema: String,

        /// Check interval in milliseconds for schedules and sweeps
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,

        /// Stale worker heartbeat timeout in seconds
        #[arg(long, default_value_t = 30)]
        heartbeat_timeout_secs: i64,

        /// Workflow run execution timeout in seconds
        #[arg(long, default_value_t = 3600)]
        workflow_timeout_secs: i64,
    },

    /// Start the pgqrs-sql-worker external SQL executor daemon
    SqlWorker {
        /// Database DSN (can also be set via PGQRS_DSN or DATABASE_URL env vars)
        #[arg(long)]
        dsn: Option<String>,

        /// Database schema namespace
        #[arg(long, default_value = "public", env = "PGQRS_SCHEMA")]
        schema: String,

        /// Comma-separated list of queues to poll
        #[arg(long, value_delimiter = ',', required = true)]
        queues: Vec<String>,

        /// Polling interval in milliseconds
        #[arg(long, default_value_t = 250)]
        interval_ms: u64,

        /// Unique name for this worker (defaults to auto-generated UUID)
        #[arg(long)]
        worker_name: Option<String>,
    },

    /// Setup test schemas for integration tests
    SetupTestSchemas {
        /// Database DSN (can also be set via PGQRS_TEST_DSN env var)
        #[arg(long)]
        dsn: Option<String>,

        /// Clean up (drop) all test schemas instead of provisioning them
        #[arg(long)]
        cleanup: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Admin {
            dsn,
            schema,
            interval_ms,
            heartbeat_timeout_secs,
            workflow_timeout_secs,
        } => {
            let dsn = dsn
                .or_else(|| std::env::var("PGQRS_DSN").ok())
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or("Database DSN is required. Set via --dsn argument, PGQRS_DSN env var, or DATABASE_URL env var.")?;

            admin::run(
                dsn,
                schema,
                interval_ms,
                heartbeat_timeout_secs,
                workflow_timeout_secs,
            )
            .await?;
        }
        Commands::SqlWorker {
            dsn,
            schema,
            queues,
            interval_ms,
            worker_name,
        } => {
            let dsn = dsn
                .or_else(|| std::env::var("PGQRS_DSN").ok())
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or("Database DSN is required. Set via --dsn argument, PGQRS_DSN env var, or DATABASE_URL env var.")?;

            let worker_name =
                worker_name.unwrap_or_else(|| format!("sql-worker-{}", uuid::Uuid::new_v4()));

            sql_worker::run(dsn, schema, queues, interval_ms, worker_name).await?;
        }
        Commands::SetupTestSchemas { dsn, cleanup } => {
            let dsn = dsn
                .or_else(|| std::env::var("PGQRS_TEST_DSN").ok())
                .unwrap_or_else(|| {
                    "postgres://postgres:postgres@localhost:5432/postgres".to_string()
                });

            setup_test_schemas::run(dsn, cleanup).await?;
        }
    }
    Ok(())
}

mod admin {
    use super::*;

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
        let store = connect_with_config(&config).await?;

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
}

mod sql_worker {
    use super::*;

    pub async fn run(
        dsn: String,
        schema: String,
        queues: Vec<String>,
        interval_ms: u64,
        worker_name: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting pgqrs sql-worker daemon...");
        println!("Worker Name: {}", worker_name);
        println!("Schema: {}", schema);
        println!("Queues: {:?}", queues);
        println!("Poll Interval: {}ms", interval_ms);

        let config = pgqrs::Config::from_dsn_with_schema(&dsn, &schema)?;
        let store = connect_with_config(&config).await?;

        // Bootstrap tables and function migrations
        store.bootstrap().await?;

        // Spawn a polling task for each queue
        for queue in queues {
            let store = store.clone();
            let queue_name = queue.clone();
            let worker_name = worker_name.clone();

            tokio::spawn(async move {
                println!(
                    "Registering worker '{}' for queue '{}'",
                    worker_name, queue_name
                );
                let consumer = match store.consumer(&queue_name, &worker_name).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!(
                            "Failed to register consumer for queue {}: {}",
                            queue_name, e
                        );
                        return;
                    }
                };

                let handler = {
                    let store = store.clone();
                    let queue_name = queue_name.clone();
                    move |msg| {
                        let store = store.clone();
                        let queue_name = queue_name.clone();
                        Box::pin(async move { process_message(&store, msg, &queue_name).await })
                    }
                };

                let result = pgqrs::dequeue()
                    .worker(&consumer)
                    .batch(1)
                    .poll_interval(Duration::from_millis(interval_ms))
                    .handle(handler)
                    .poll(&store)
                    .await;

                if let Err(e) = result {
                    eprintln!("Error in dequeue poll loop for queue {}: {}", queue_name, e);
                }
            });
        }

        let sigint = tokio::signal::ctrl_c();
        tokio::pin!(sigint);

        println!("pgqrs sql-worker daemon is running. Press Ctrl+C to stop.");

        tokio::select! {
            _ = &mut sigint => {
                println!("Received shutdown signal. Stopping pgqrs sql-worker...");
            }
        }

        Ok(())
    }

    async fn process_message(
        store: &Store,
        msg: QueueMessage,
        queue_name: &str,
    ) -> pgqrs::error::Result<()> {
        // 1. Try to treat it as a workflow execution
        match store.run(msg.clone()).await {
            Ok(run) => {
                println!(
                    "Executing SQL workflow run {} for queue '{}'",
                    run.id(),
                    queue_name
                );
                let run = run.start().await?;

                // Prepare the dynamic function call. The PL/pgSQL function name is same as workflow name (queue_name).
                let sql = format!("SELECT {} ($1, $2)", queue_name);

                let execute_result = sqlx::query_scalar::<_, Option<Value>>(&sql)
                    .bind(run.id())
                    .bind(run.record().input.clone().unwrap_or(Value::Null))
                    .fetch_one(store.pool())
                    .await;

                match execute_result {
                    Ok(output) => {
                        let out_val = output.unwrap_or(Value::Null);
                        run.complete(out_val).await?;
                        println!("Completed SQL workflow run {}", run.id());
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("Error executing SQL workflow run {}: {}", run.id(), e);
                        let err_payload = serde_json::json!({
                            "message": e.to_string()
                        });
                        run.fail_with_json(err_payload).await?;
                        // Return the error to trigger standard retry/DLQ behavior
                        Err(pgqrs::error::Error::QueryFailed {
                            query: sql,
                            source: Box::new(e),
                            context: format!("Workflow run {} failed", run.id()),
                        })
                    }
                }
            }
            Err(e) => {
                let is_not_found = match &e {
                    pgqrs::error::Error::QueryFailed { source, .. } => {
                        if let Some(sqlx_err) = source.downcast_ref::<sqlx::Error>() {
                            matches!(sqlx_err, sqlx::Error::RowNotFound)
                        } else {
                            false
                        }
                    }
                    _ => false,
                };

                if !is_not_found {
                    return Err(e);
                }

                // 2. Not a workflow -> treat as raw standalone SQL job
                let payload = msg.payload;
                let statement = payload
                    .get("statement")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| pgqrs::error::Error::ValidationFailed {
                        reason: "Missing 'statement' string field in SQL job payload".to_string(),
                    })?;

                // Safety check: no transaction control commands
                let stmt_upper = statement.trim().to_uppercase();
                if stmt_upper.starts_with("BEGIN")
                    || stmt_upper.starts_with("COMMIT")
                    || stmt_upper.starts_with("ROLLBACK")
                    || stmt_upper.starts_with("ABORT")
                    || stmt_upper.starts_with("SAVEPOINT")
                {
                    return Err(pgqrs::error::Error::ValidationFailed {
                        reason: "Transaction control commands (BEGIN, COMMIT, ROLLBACK, ABORT, SAVEPOINT) are not allowed".to_string(),
                    });
                }

                let use_tx = payload.get("tx").and_then(|v| v.as_bool()).unwrap_or(true);
                let timeout_ms = payload
                    .get("statement_timeout_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(30000);

                println!(
                    "Executing standalone SQL job {} (tx={}): {}",
                    msg.id, use_tx, statement
                );

                let exec_outcome = if use_tx {
                    let mut tx = store.pool().begin().await.map_err(|e| {
                        pgqrs::error::Error::QueryFailed {
                            query: "BEGIN TRANSACTION".into(),
                            source: Box::new(e),
                            context: "Failed to begin transaction for SQL job".into(),
                        }
                    })?;

                    let outcome = run_statement_with_timeout(
                        &mut tx,
                        statement,
                        payload.get("params"),
                        timeout_ms,
                    )
                    .await;
                    if outcome.is_ok() {
                        tx.commit()
                            .await
                            .map_err(|e| pgqrs::error::Error::QueryFailed {
                                query: "COMMIT TRANSACTION".into(),
                                source: Box::new(e),
                                context: "Failed to commit transaction for SQL job".into(),
                            })?;
                    } else {
                        let _ = tx.rollback().await;
                    }
                    outcome
                } else {
                    let mut conn = store.pool().acquire().await.map_err(|e| {
                        pgqrs::error::Error::QueryFailed {
                            query: "ACQUIRE CONNECTION".into(),
                            source: Box::new(e),
                            context: "Failed to acquire connection for SQL job".into(),
                        }
                    })?;
                    run_statement_with_timeout(
                        &mut conn,
                        statement,
                        payload.get("params"),
                        timeout_ms,
                    )
                    .await
                };

                match exec_outcome {
                    Ok(result_value) => {
                        println!(
                            "Successfully executed SQL job {}. Result: {}",
                            msg.id, result_value
                        );
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("Error executing SQL job {}: {}", msg.id, e);
                        Err(e)
                    }
                }
            }
        }
    }

    async fn run_statement_with_timeout(
        conn: &mut sqlx::PgConnection,
        statement: &str,
        params: Option<&Value>,
        timeout_ms: u64,
    ) -> pgqrs::error::Result<Value> {
        // Set statement timeout
        let timeout_sql = format!("SET LOCAL statement_timeout = {}", timeout_ms);
        sqlx::query(&timeout_sql)
            .execute(&mut *conn)
            .await
            .map_err(|e| pgqrs::error::Error::QueryFailed {
                query: timeout_sql,
                source: Box::new(e),
                context: "Failed to set statement timeout".into(),
            })?;

        // Prepare query
        let mut query = sqlx::query(statement);
        if let Some(params_val) = params {
            if let Some(params_array) = params_val.as_array() {
                for param in params_array {
                    if let Some(s) = param.as_str() {
                        query = query.bind(s.to_string());
                    } else if let Some(n) = param.as_i64() {
                        query = query.bind(n);
                    } else if let Some(f) = param.as_f64() {
                        query = query.bind(f);
                    } else if let Some(b) = param.as_bool() {
                        query = query.bind(b);
                    } else if param.is_null() {
                        query = query.bind(None::<String>);
                    } else {
                        query = query.bind(param.to_string());
                    }
                }
            }
        }

        // Execute statement and capture rows
        let rows =
            query
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| pgqrs::error::Error::QueryFailed {
                    query: statement.to_string(),
                    source: Box::new(e),
                    context: "Failed to execute SQL statement".into(),
                })?;

        let mut result_rows = Vec::new();
        for row in rows {
            let mut row_obj = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name();
                let val = match col.type_info().name() {
                    "INT8" | "BIGINT" => row
                        .try_get::<i64, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "INT4" | "INTEGER" => row
                        .try_get::<i32, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "INT2" | "SMALLINT" => row
                        .try_get::<i16, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "FLOAT8" | "DOUBLE PRECISION" => row
                        .try_get::<f64, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "FLOAT4" | "REAL" => row
                        .try_get::<f32, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "BOOL" | "BOOLEAN" => row
                        .try_get::<bool, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "TEXT" | "VARCHAR" | "CHAR" | "NAME" => row
                        .try_get::<String, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                    "JSON" | "JSONB" => row.try_get::<Value, _>(name).unwrap_or(Value::Null),
                    _ => row
                        .try_get::<String, _>(name)
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                };
                row_obj.insert(name.to_string(), val);
            }
            result_rows.push(Value::Object(row_obj));
            if result_rows.len() >= 100 {
                break; // Truncate result at 100 rows to safeguard memory
            }
        }

        Ok(Value::Array(result_rows))
    }
}

mod setup_test_schemas {
    use super::*;

    const TEST_SCHEMAS: &[&str] = &[
        "pgqrs_admin_scan_interval_test",
        "pgqrs_admin_scan_cron_test",
        "pgqrs_admin_reclaim_test",
        "pgqrs_admin_timeout_test",
        "pgqrs_builder_test",
        "pgqrs_builder_ergonomics_test",
        "pgqrs_concurrent_test",
        "pgqrs_error_test",
        "pgqrs_lib_test",
        "pgqrs_zombie_tests",
        "pgqrs_lib_stat_test",
        "pgqrs_pgbouncer_test",
        "pgqrs_cli_test",
        "pgqrs_worker_test",
        "pgqrs_workflow_test",
        "pgqrs_workflow_creation_test",
        "pgqrs_workflow_retry_test",
        "macro_test_creation",
        "macro_test_success",
        "macro_test_idempotency",
        "macro_test_step_failure",
        "macro_test_workflow_failure",
        "macro_test_run_metadata",
        "workflow_tests",
        "workflow_get_tests",
        "workflow_retrieval_tests",
        "workflow_polling_tests",
        "workflow_error_polling_tests",
        "workflow_fk_tests",
        "workflow_retry_integration_tests",
        "guide_tests",
        "test_sql_job_success",
        "test_sql_job_select",
        "test_sql_job_safety",
        "test_sql_workflow",
        "test_sql_wf_fail",
    ];

    pub async fn run(dsn: String, cleanup_mode: bool) -> Result<(), Box<dyn std::error::Error>> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&dsn)
            .await?;

        if cleanup_mode {
            println!("Cleaning up test schemas using DSN: {}", dsn);

            for schema in TEST_SCHEMAS {
                println!("Dropping schema: {}", schema);
                let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
                sqlx::query(&drop_sql).execute(&pool).await?;
            }

            println!("All test schemas cleaned up successfully!");
        } else {
            println!("Setting up test databases using DSN: {}", dsn);
            println!("Connected to database.");

            for schema in TEST_SCHEMAS {
                println!("Provisioning schema: {}", schema);

                // 1. Drop and Recreate Schema (Clean Slate for Suite)
                let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
                sqlx::query(&drop_sql).execute(&pool).await?;

                let create_sql = format!("CREATE SCHEMA \"{}\"", schema);
                sqlx::query(&create_sql).execute(&pool).await?;

                // 2. Install Migration
                // We rely on search_path to install tables into the new schema
                let config = pgqrs::config::Config::from_dsn_with_schema(&dsn, *schema)?;
                let store = pgqrs::connect_with_config(&config).await?;

                pgqrs::admin(&store).install().await?;
                println!("  -> Installed pgqrs tables.");
            }

            println!("All test schemas provisioned successfully!");
        }

        Ok(())
    }
}
