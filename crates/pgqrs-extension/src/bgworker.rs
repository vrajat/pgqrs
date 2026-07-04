use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use std::ffi::CStr;
use std::time::Duration;

pub static COORDINATOR_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static COORDINATOR_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(1000);
pub static HEARTBEAT_TIMEOUT_SECS: GucSetting<i32> = GucSetting::<i32>::new(30);
pub static WORKFLOW_TIMEOUT_SECS: GucSetting<i32> = GucSetting::<i32>::new(3600);

pub static DATABASE_NAME: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static USER_NAME: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub fn init_gucs() {
    GucRegistry::define_bool_guc(
        "pgqrs.coordinator_enabled",
        "Enable pgqrs coordinator background worker",
        "When true, pgqrs runs a background worker loop inside Postgres to orchestrate task/workflow scheduling and maintenance",
        &COORDINATOR_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pgqrs.coordinator_interval_ms",
        "Coordinator check interval in milliseconds",
        "Interval between checking for due schedules and maintenance sweeps",
        &COORDINATOR_INTERVAL_MS,
        100,
        3600000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pgqrs.heartbeat_timeout_secs",
        "Heartbeat timeout in seconds",
        "Stale worker threshold for lease reclamation",
        &HEARTBEAT_TIMEOUT_SECS,
        1,
        86400,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pgqrs.workflow_timeout_secs",
        "Workflow timeout in seconds",
        "Maximum run duration for an active workflow run before being timed out",
        &WORKFLOW_TIMEOUT_SECS,
        1,
        604800,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        "pgqrs.database",
        "Target database containing pgqrs schema",
        "The database to connect to via SPI to run coordinator jobs",
        &DATABASE_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        "pgqrs.user",
        "Postgres user for coordinator execution",
        "The database user under whose privileges the coordinator runs",
        &USER_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

#[pg_guard]
pub extern "C" fn pgqrs_coordinator_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db = DATABASE_NAME
        .get()
        .and_then(|s| s.to_str().ok())
        .unwrap_or("postgres");
    let user = USER_NAME
        .get()
        .and_then(|s| s.to_str().ok())
        .unwrap_or("postgres");
    BackgroundWorker::connect_worker_to_spi(Some(db), Some(user));

    pgrx::log!(
        "pgqrs coordinator started on database '{}' under user '{}'",
        db,
        user
    );

    while BackgroundWorker::worker_continue() {
        let interval_ms = COORDINATOR_INTERVAL_MS.get() as u64;

        let _ = BackgroundWorker::transaction(|| {
            // Acquire transaction advisory lock to guarantee exactly-once execution
            // Key: 5784604930263089153 (0x5047515253000001)
            let has_lock =
                Spi::get_one::<bool>("SELECT pg_try_advisory_xact_lock(5784604930263089153)")
                    .unwrap_or(Some(false))
                    .unwrap_or(false);

            if !has_lock {
                return;
            }

            // 1. Scan cron schedules
            loop {
                match scan_cron_once() {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        pgrx::log!("Error in pgqrs cron scanner: {:?}", e);
                        break;
                    }
                }
            }

            // 2. Run maintenance sweep
            let hb_timeout = HEARTBEAT_TIMEOUT_SECS.get() as f64;
            let wf_timeout = WORKFLOW_TIMEOUT_SECS.get() as f64;
            if let Err(e) = run_maintenance_sweep(hb_timeout, wf_timeout) {
                pgrx::log!("Error in pgqrs maintenance sweep: {:?}", e);
            }
        });

        // 3. Process built-in worker queues
        if let Some(queues_str) = crate::builtins::BUILTIN_QUEUES
            .get()
            .and_then(|s| s.to_str().ok())
        {
            if !queues_str.is_empty() {
                let queues: Vec<&str> = queues_str
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();
                for queue in queues {
                    crate::builtins::process_builtin_queue_once(queue);
                }
            }
        }

        BackgroundWorker::wait_latch(Some(Duration::from_millis(interval_ms)));
    }

    pgrx::log!("pgqrs coordinator shutting down...");
}

fn parse_cron(expr: &str) -> Result<chrono::DateTime<chrono::Utc>, String> {
    let expr = expr.trim();
    let parts: Vec<&str> = expr.split_whitespace().collect();

    let cron_str = if parts.len() == 5 {
        format!("0 {}", expr)
    } else {
        expr.to_string()
    };

    use std::str::FromStr;
    let schedule = cron::Schedule::from_str(&cron_str)
        .map_err(|e| format!("Invalid cron expression '{}': {}", expr, e))?;

    let next = schedule
        .upcoming(chrono::Utc)
        .next()
        .ok_or_else(|| format!("No upcoming execution time for cron expression '{}'", expr))?;

    Ok(next)
}

pub(crate) fn scan_cron_once() -> Result<bool, pgrx::spi::Error> {
    // 1. SELECT next active cron due for fire
    let row_opt = Spi::connect(|client| -> Result<_, pgrx::spi::Error> {
        let select_sql = r#"
            SELECT c.id, c.name, c.cron_expression, c.queue_id, q.queue_name as workflow_name, c.input
            FROM pgqrs_cron c
            JOIN pgqrs_queues q ON c.queue_id = q.id
            WHERE c.status = 'active'
              AND c.next_fire_at <= NOW()
            ORDER BY c.next_fire_at ASC, c.id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        "#;
        let mut tuple_table = client.select(select_sql, None, None)?;
        if tuple_table.is_empty() {
            return Ok(None);
        }
        let row = tuple_table.next().unwrap();
        let id: i64 = row.get_by_name("id")?.unwrap();
        let name: String = row.get_by_name("name")?.unwrap();
        let cron_expression: String = row.get_by_name("cron_expression")?.unwrap();
        let queue_id: i64 = row.get_by_name("queue_id")?.unwrap();
        let workflow_name: String = row.get_by_name("workflow_name")?.unwrap();
        let input: Option<pgrx::JsonB> = row.get_by_name("input")?;

        Ok(Some((
            id,
            name,
            cron_expression,
            queue_id,
            workflow_name,
            input,
        )))
    })?;

    let (id, name, cron_expression, queue_id, workflow_name, input) = match row_opt {
        Some(val) => val,
        None => return Ok(false),
    };

    // 2. Enqueue trigger message directly using queue_id
    let payload = serde_json::json!({
        "input": input.map(|j| j.0).unwrap_or(serde_json::Value::Null)
    });
    let payload_str = serde_json::to_string(&payload).unwrap();

    let message_id = Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pgqrs_messages (queue_id, payload, vt, enqueued_at)
        VALUES ($1, $2::jsonb, NOW(), NOW())
        RETURNING id
        "#,
        vec![
            (PgBuiltInOids::INT8OID.oid(), queue_id.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), payload_str.into_datum()),
        ],
    )?
    .unwrap();

    // 3. Calculate next fire time
    let next_fire = match parse_cron(&cron_expression) {
        Ok(t) => t,
        Err(e) => {
            pgrx::log!(
                "Error parsing cron expression '{}' for cron '{}': {:?}",
                cron_expression,
                name,
                e
            );
            // Pause the cron
            let _ = Spi::run_with_args(
                r#"
                UPDATE pgqrs_cron
                SET status = 'paused',
                    updated_at = NOW()
                WHERE id = $1
                "#,
                Some(vec![(PgBuiltInOids::INT8OID.oid(), id.into_datum())]),
            );
            return Ok(true);
        }
    };

    // 4. Update next fire time and reset trigger_state to idle
    let next_fire_str = next_fire.to_rfc3339();
    let _ = Spi::run_with_args(
        r#"
        UPDATE pgqrs_cron
        SET next_fire_at = $2::timestamptz,
            trigger_state = 'idle',
            updated_at = NOW()
        WHERE id = $1
        "#,
        Some(vec![
            (PgBuiltInOids::INT8OID.oid(), id.into_datum()),
            (
                PgBuiltInOids::TEXTOID.oid(),
                next_fire_str.clone().into_datum(),
            ),
        ]),
    )?;

    pgrx::log!(
        "Triggered cron '{}' (workflow: '{}'), enqueued message_id={}, next_fire_at={}",
        name,
        workflow_name,
        message_id,
        next_fire_str
    );

    Ok(true)
}

pub(crate) fn run_maintenance_sweep(
    heartbeat_timeout_secs: f64,
    workflow_timeout_secs: f64,
) -> Result<(), pgrx::spi::Error> {
    // 1. Worker Health: Mark stale workers as stopped.
    let stopped_count = Spi::get_one_with_args::<i64>(
        r#"
        WITH updated AS (
            UPDATE pgqrs_workers
            SET status = 'stopped'::worker_status,
                shutdown_at = NOW()
            WHERE status IN ('ready'::worker_status, 'polling'::worker_status, 'suspended'::worker_status, 'interrupted'::worker_status)
              AND heartbeat_at < NOW() - make_interval(secs => $1::double precision)
            RETURNING id
        )
        SELECT COUNT(*) FROM updated
        "#,
        vec![(PgBuiltInOids::FLOAT8OID.oid(), heartbeat_timeout_secs.into_datum())],
    )?.unwrap_or(0);

    if stopped_count > 0 {
        pgrx::log!("Marked {} stale worker(s) as stopped", stopped_count);
    }

    // 2. Lease Reclamation: Reset visibility timeouts and worker assignments
    let reclaimed_count = Spi::get_one_with_args::<i64>(
        r#"
        WITH updated AS (
            UPDATE pgqrs_messages
            SET vt = NOW(),
                consumer_worker_id = NULL
            WHERE consumer_worker_id IS NOT NULL
              AND archived_at IS NULL
              AND consumer_worker_id IN (
                  SELECT id FROM pgqrs_workers
                  WHERE status = 'stopped'
                     OR heartbeat_at < NOW() - make_interval(secs => $1::double precision)
              )
            RETURNING id
        )
        SELECT COUNT(*) FROM updated
        "#,
        vec![(
            PgBuiltInOids::FLOAT8OID.oid(),
            heartbeat_timeout_secs.into_datum(),
        )],
    )?
    .unwrap_or(0);

    if reclaimed_count > 0 {
        pgrx::log!(
            "Reclaimed {} expired lease(s) from stale/stopped workers",
            reclaimed_count
        );
    }

    // 3. Workflow Timeout: Scan running workflow runs that have exceeded their timeout
    let timed_out_runs = Spi::connect(|client| -> Result<_, pgrx::spi::Error> {
        let sql = r#"
            UPDATE pgqrs_workflow_runs
            SET status = 'ERROR'::pgqrs_workflow_status,
                error = '{"message": "Workflow run execution timed out"}'::jsonb,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE status = 'RUNNING'::pgqrs_workflow_status
              AND started_at < NOW() - make_interval(secs => $1::double precision)
            RETURNING id
        "#;
        let mut table = client.select(
            sql,
            None,
            Some(vec![(
                PgBuiltInOids::FLOAT8OID.oid(),
                workflow_timeout_secs.into_datum(),
            )]),
        )?;
        let mut ids = Vec::new();
        while let Some(row) = table.next() {
            let id: i64 = row.get_by_name("id")?.unwrap();
            ids.push(id);
        }
        Ok(ids)
    })?;

    if !timed_out_runs.is_empty() {
        pgrx::log!(
            "Timed out {} workflow run(s): {:?}",
            timed_out_runs.len(),
            timed_out_runs
        );

        // Abort outstanding steps for these timed-out workflow runs
        let _ = Spi::connect(|client| -> Result<_, pgrx::spi::Error> {
            let sql = r#"
                UPDATE pgqrs_workflow_steps
                SET status = 'ERROR'::pgqrs_workflow_status,
                    error = '{"message": "Workflow run execution timed out"}'::jsonb,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE run_id = ANY($1)
                  AND status IN ('RUNNING'::pgqrs_workflow_status, 'QUEUED'::pgqrs_workflow_status)
            "#;
            client.select(
                sql,
                None,
                Some(vec![(
                    PgBuiltInOids::INT8ARRAYOID.oid(),
                    timed_out_runs.into_datum(),
                )]),
            )?;
            Ok(())
        })?;
    }

    Ok(())
}
