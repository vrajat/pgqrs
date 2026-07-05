use pgrx::bgworkers::BackgroundWorker;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use std::ffi::CStr;
use std::time::Duration;

// Define GUC for built-in worker queues
pub static BUILTIN_QUEUES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub fn init_gucs() {
    GucRegistry::define_string_guc(
        "pgqrs.builtin_queues",
        "Comma-separated list of queues for built-in worker execution",
        "When set, the resident background worker polls and executes jobs on these queues using the built-in executor",
        &BUILTIN_QUEUES,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

/// Helper to wrap execution in a transaction if running inside a BackgroundWorker,
/// or directly execute if already inside a backend process transaction.
fn in_transaction<F, R>(f: F) -> R
where
    F: FnOnce() -> R + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
{
    let is_bgworker = unsafe { !pg_sys::MyBgworkerEntry.is_null() };
    if is_bgworker {
        BackgroundWorker::transaction(f)
    } else {
        f()
    }
}

/// Expose registered built-in capabilities and versions
#[pg_extern]
fn pgqrs_builtins() -> TableIterator<
    'static,
    (
        name!(capability, String),
        name!(version, String),
        name!(description, String),
    ),
> {
    let list = vec![
        (
            "sql".to_string(),
            "1.0.0".to_string(),
            "Standalone raw SQL statement execution".to_string(),
        ),
        (
            "timer".to_string(),
            "1.0.0".to_string(),
            "Non-blocking execution delays and sleeps".to_string(),
        ),
        (
            "maintenance".to_string(),
            "1.0.0".to_string(),
            "Force execution of zombie lease reclamation and sweep".to_string(),
        ),
        (
            "metrics".to_string(),
            "1.0.0".to_string(),
            "Log current queue size, worker status, and delay statistics".to_string(),
        ),
    ];
    TableIterator::new(list)
}

/// Process a single message from the specified queue if available
pub fn process_builtin_queue_once(queue_name: &str) {
    // 1. Get or create queue_id and worker_id
    let registration = in_transaction(|| {
        let queue_id = Spi::get_one_with_args::<i64>(
            r#"
            INSERT INTO pgqrs_queues (queue_name)
            VALUES ($1)
            ON CONFLICT (queue_name) DO UPDATE SET queue_name = EXCLUDED.queue_name
            RETURNING id
            "#,
            vec![(PgBuiltInOids::TEXTOID.oid(), queue_name.into_datum())],
        )?
        .unwrap();

        let db = crate::bgworker::DATABASE_NAME
            .get()
            .and_then(|s| s.to_str().ok())
            .unwrap_or("postgres");
        let worker_name = format!("pgqrs-builtin-worker-{}-{}", db, queue_name);

        let worker_id = Spi::get_one_with_args::<i64>(
            r#"
            INSERT INTO pgqrs_workers (name, queue_id, status, heartbeat_at)
            VALUES ($1, $2, 'ready'::worker_status, NOW())
            ON CONFLICT (name) DO UPDATE SET heartbeat_at = NOW(), status = 'ready'::worker_status
            RETURNING id
            "#,
            vec![
                (PgBuiltInOids::TEXTOID.oid(), worker_name.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), queue_id.into_datum()),
            ],
        )?
        .unwrap();

        Ok::<_, pgrx::spi::Error>((queue_id, worker_id))
    });

    let (queue_id, worker_id) = match registration {
        Ok(val) => val,
        Err(e) => {
            pgrx::log!(
                "Failed to register builtin worker for queue '{}': {:?}",
                queue_name,
                e
            );
            return;
        }
    };

    // 2. Dequeue 1 message
    let message_opt = in_transaction(|| {
        let row_opt = Spi::connect(|client| -> Result<_, pgrx::spi::Error> {
            let sql = r#"
                UPDATE pgqrs_messages
                SET vt = NOW() + make_interval(secs => 30.0),
                    read_ct = read_ct + 1,
                    dequeued_at = COALESCE(dequeued_at, NOW()),
                    consumer_worker_id = $2
                WHERE id IN (
                    SELECT id
                    FROM pgqrs_messages
                    WHERE queue_id = $1
                      AND (vt IS NULL OR vt <= NOW())
                      AND consumer_worker_id IS NULL
                      AND archived_at IS NULL
                      AND read_ct < $3
                    ORDER BY enqueued_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, payload::text, read_ct;
            "#;
            let mut table = client.select(
                sql,
                None,
                Some(vec![
                    (PgBuiltInOids::INT8OID.oid(), queue_id.into_datum()),
                    (PgBuiltInOids::INT8OID.oid(), worker_id.into_datum()),
                    (PgBuiltInOids::INT4OID.oid(), 5.into_datum()),
                ]),
            )?;

            if table.is_empty() {
                return Ok(None);
            }
            let row = table.next().unwrap();
            let id: i64 = row.get_by_name("id")?.unwrap();
            let payload_str: String = row.get_by_name("payload")?.unwrap();
            let read_ct: i32 = row.get_by_name("read_ct")?.unwrap();

            Ok(Some((id, payload_str, read_ct)))
        })?;
        Ok::<_, pgrx::spi::Error>(row_opt)
    });

    let (msg_id, payload_str, read_ct) = match message_opt {
        Ok(Some(msg)) => msg,
        _ => return, // No message or error
    };

    // Parse payload JSON
    let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
        Ok(val) => val,
        Err(e) => {
            pgrx::log!(
                "Built-in worker failed to parse payload for message {}: {:?}",
                msg_id,
                e
            );
            in_transaction(|| {
                let _ = Spi::run_with_args(
                    "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                    Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                );
            });
            return;
        }
    };

    // 3. Check if it corresponds to a workflow run
    let workflow_id_opt = in_transaction(|| {
        Spi::get_one_with_args::<i64>(
            "SELECT id FROM pgqrs_workflows WHERE name = $1",
            vec![(
                PgBuiltInOids::TEXTOID.oid(),
                queue_name.to_string().into_datum(),
            )],
        )
    });

    let workflow_id = match workflow_id_opt {
        Ok(Some(id)) => Some(id),
        _ => None,
    };

    if let Some(wf_id) = workflow_id {
        // Run workflow execution
        let exec_result = in_transaction(|| {
            // Get or create run_id
            let run_id = Spi::connect(|client| -> Result<i64, pgrx::spi::Error> {
                let mut table = client.select(
                    "SELECT id FROM pgqrs_workflow_runs WHERE message_id = $1",
                    None,
                    Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())]),
                )?;
                if !table.is_empty() {
                    let id: i64 = table.next().unwrap().get_by_name("id")?.unwrap();
                    client.select(
                        "UPDATE pgqrs_workflow_runs SET status = 'RUNNING'::pgqrs_workflow_status, started_at = NOW(), worker_id = $2 WHERE id = $1",
                        None, Some(vec![
                            (PgBuiltInOids::INT8OID.oid(), id.into_datum()),
                            (PgBuiltInOids::INT8OID.oid(), worker_id.into_datum()),
                        ])
                    )?;
                    Ok(id)
                } else {
                    let id: i64 = client.select(
                        "INSERT INTO pgqrs_workflow_runs (workflow_id, message_id, status, input, worker_id, started_at) VALUES ($1, $2, 'RUNNING'::pgqrs_workflow_status, $3::jsonb, $4, NOW()) RETURNING id",
                        None, Some(vec![
                            (PgBuiltInOids::INT8OID.oid(), wf_id.into_datum()),
                            (PgBuiltInOids::INT8OID.oid(), msg_id.into_datum()),
                            (PgBuiltInOids::TEXTOID.oid(), payload_str.clone().into_datum()),
                            (PgBuiltInOids::INT8OID.oid(), worker_id.into_datum()),
                        ])
                    )?.next().unwrap().get_by_name("id")?.unwrap();
                    Ok(id)
                }
            })?;

            // Execute {queue_name}(run_id, input)
            let sql = format!("SELECT {} ($1, $2::jsonb)", queue_name);
            let run_res = Spi::connect(|client| -> Result<Option<pgrx::JsonB>, pgrx::spi::Error> {
                let mut table = client.select(
                    &sql,
                    None,
                    Some(vec![
                        (PgBuiltInOids::INT8OID.oid(), run_id.into_datum()),
                        (
                            PgBuiltInOids::TEXTOID.oid(),
                            payload_str.clone().into_datum(),
                        ),
                    ]),
                )?;
                if table.is_empty() {
                    Ok(None)
                } else {
                    let row = table.next().unwrap();
                    row.get::<pgrx::JsonB>(1)
                }
            });

            match run_res {
                Ok(output) => {
                    let output_json = output.map(|j| j.0).unwrap_or(serde_json::Value::Null);
                    let output_str = serde_json::to_string(&output_json).unwrap();
                    // Complete run and archive message
                    let _ = Spi::run_with_args(
                        r#"
                        UPDATE pgqrs_workflow_runs
                        SET status = 'SUCCESS'::pgqrs_workflow_status,
                            output = $2::jsonb,
                            completed_at = NOW(),
                            updated_at = NOW()
                        WHERE id = $1
                        "#,
                        Some(vec![
                            (PgBuiltInOids::INT8OID.oid(), run_id.into_datum()),
                            (PgBuiltInOids::TEXTOID.oid(), output_str.into_datum()),
                        ]),
                    );
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                    Ok(())
                }
                Err(e) => {
                    // Fail run
                    let err_payload = serde_json::json!({ "message": format!("{:?}", e) });
                    let err_str = serde_json::to_string(&err_payload).unwrap();
                    let _ = Spi::run_with_args(
                        r#"
                        UPDATE pgqrs_workflow_runs
                        SET status = 'ERROR'::pgqrs_workflow_status,
                            error = $2::jsonb,
                            completed_at = NOW(),
                            updated_at = NOW()
                        WHERE id = $1
                        "#,
                        Some(vec![
                            (PgBuiltInOids::INT8OID.oid(), run_id.into_datum()),
                            (PgBuiltInOids::TEXTOID.oid(), err_str.into_datum()),
                        ]),
                    );
                    Err(e)
                }
            }
        });

        if let Err(e) = exec_result {
            pgrx::log!("Workflow execution failed for message {}: {:?}", msg_id, e);
            // Release or DLQ message
            in_transaction(|| {
                if read_ct >= 5 {
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                } else {
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET vt = NOW() + make_interval(secs => 5.0), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                }
            });
        }
    } else {
        // Process standalone task or built-in capability
        let exec_outcome = in_transaction(|| {
            let cap = payload.get("capability").and_then(|v| v.as_str());
            match cap {
                Some("timer") => {
                    let duration = payload
                        .get("duration_ms")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(1000);
                    pgrx::log!("Built-in timer capability: sleeping for {} ms", duration);
                    std::thread::sleep(Duration::from_millis(duration));
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                    Ok(())
                }
                Some("maintenance") => {
                    pgrx::log!("Built-in maintenance capability: running sweep");
                    crate::bgworker::run_maintenance_sweep(30.0, 3600.0)?;
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                    Ok(())
                }
                Some("metrics") => {
                    let pending: i64 = Spi::get_one("SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NULL AND (vt IS NULL OR vt <= NOW())")?.unwrap_or(0);
                    let leased: i64 = Spi::get_one("SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NULL AND vt > NOW() AND consumer_worker_id IS NOT NULL")?.unwrap_or(0);
                    let archived: i64 = Spi::get_one(
                        "SELECT COUNT(*) FROM pgqrs_messages WHERE archived_at IS NOT NULL",
                    )?
                    .unwrap_or(0);
                    let workers: i64 =
                        Spi::get_one("SELECT COUNT(*) FROM pgqrs_workers")?.unwrap_or(0);
                    pgrx::log!(
                        "Built-in metrics capability: pending={}, leased={}, archived={}, active_workers={}",
                        pending, leased, archived, workers
                    );
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                    Ok(())
                }
                _ => {
                    // Check raw SQL statement execution
                    if let Some(statement) = payload.get("statement").and_then(|v| v.as_str()) {
                        let stmt_upper = statement.trim().to_uppercase();
                        if stmt_upper.starts_with("BEGIN")
                            || stmt_upper.starts_with("COMMIT")
                            || stmt_upper.starts_with("ROLLBACK")
                            || stmt_upper.starts_with("ABORT")
                            || stmt_upper.starts_with("SAVEPOINT")
                        {
                            return Err(pgrx::spi::SpiError::SpiError(
                                pgrx::spi::SpiErrorCodes::Transaction,
                            ));
                        }

                        let timeout_ms = payload
                            .get("statement_timeout_ms")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(30000);
                        let _ = Spi::run(&format!("SET LOCAL statement_timeout = {}", timeout_ms));

                        let params = payload.get("params");
                        let mut spi_args = Vec::new();
                        if let Some(params_val) = params {
                            if let Some(params_array) = params_val.as_array() {
                                for param in params_array {
                                    if let Some(s) = param.as_str() {
                                        spi_args.push((
                                            PgBuiltInOids::TEXTOID.oid(),
                                            s.to_string().into_datum(),
                                        ));
                                    } else if let Some(n) = param.as_i64() {
                                        spi_args
                                            .push((PgBuiltInOids::INT8OID.oid(), n.into_datum()));
                                    } else if let Some(f) = param.as_f64() {
                                        spi_args
                                            .push((PgBuiltInOids::FLOAT8OID.oid(), f.into_datum()));
                                    } else if let Some(b) = param.as_bool() {
                                        spi_args
                                            .push((PgBuiltInOids::BOOLOID.oid(), b.into_datum()));
                                    } else if param.is_null() {
                                        spi_args.push((
                                            PgBuiltInOids::TEXTOID.oid(),
                                            None::<String>.into_datum(),
                                        ));
                                    } else {
                                        spi_args.push((
                                            PgBuiltInOids::TEXTOID.oid(),
                                            param.to_string().into_datum(),
                                        ));
                                    }
                                }
                            }
                        }

                        pgrx::log!("Executing builtin raw SQL job {}: {}", msg_id, statement);
                        Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
                            let _table = client.select(statement, None, Some(spi_args))?;
                            Ok(())
                        })?;

                        let _ = Spi::run_with_args(
                            "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                            Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                        );
                        Ok(())
                    } else {
                        // Unsupported standalone message format
                        pgrx::log!("Unsupported built-in payload format for message {}", msg_id);
                        let _ = Spi::run_with_args(
                            "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                            Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                        );
                        Ok(())
                    }
                }
            }
        });

        if let Err(e) = exec_outcome {
            pgrx::log!("Built-in standalone job {} failed: {:?}", msg_id, e);
            in_transaction(|| {
                if read_ct >= 5 {
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET archived_at = NOW(), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                } else {
                    let _ = Spi::run_with_args(
                        "UPDATE pgqrs_messages SET vt = NOW() + make_interval(secs => 5.0), consumer_worker_id = NULL WHERE id = $1",
                        Some(vec![(PgBuiltInOids::INT8OID.oid(), msg_id.into_datum())])
                    );
                }
            });
        }
    }
}
