use serde_json::Value;
use std::env;
use std::time::Duration;

use pgqrs::{connect_with_config, QueueMessage, Store};
use sqlx::{Column, Row, TypeInfo};

#[derive(Debug)]
struct Args {
    dsn: String,
    schema: String,
    queues: Vec<String>,
    interval_ms: u64,
    worker_name: String,
}

fn parse_args() -> Result<Args, String> {
    let mut args = env::args().skip(1);
    let mut dsn = env::var("PGQRS_DSN")
        .ok()
        .or_else(|| env::var("DATABASE_URL").ok());
    let mut schema = env::var("PGQRS_SCHEMA").unwrap_or_else(|_| "public".to_string());
    let mut queues = Vec::new();
    let mut interval_ms = 250;
    let mut worker_name = format!("sql-worker-{}", uuid::Uuid::new_v4());

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dsn" => {
                dsn = Some(args.next().ok_or("Missing value for --dsn")?);
            }
            "--schema" => {
                schema = args.next().ok_or("Missing value for --schema")?;
            }
            "--queues" => {
                let list = args.next().ok_or("Missing value for --queues")?;
                queues = list
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            "--interval-ms" => {
                let val = args.next().ok_or("Missing value for --interval-ms")?;
                interval_ms = val
                    .parse::<u64>()
                    .map_err(|_| "Invalid integer for --interval-ms")?;
            }
            "--worker-name" => {
                worker_name = args.next().ok_or("Missing value for --worker-name")?;
            }
            "-h" | "--help" => {
                return Err("Usage: pgqrs-sql-worker --queues Q1,Q2 [--dsn DSN] [--schema SCHEMA] [--interval-ms MS] [--worker-name NAME]".to_string());
            }
            other => {
                return Err(format!("Unknown argument: {}", other));
            }
        }
    }

    let dsn = dsn.ok_or("Database DSN is required. Set via --dsn argument, PGQRS_DSN env var, or DATABASE_URL env var.")?;
    if queues.is_empty() {
        return Err("At least one queue must be specified via --queues argument.".to_string());
    }

    Ok(Args {
        dsn,
        schema,
        queues,
        interval_ms,
        worker_name,
    })
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
                let mut tx =
                    store
                        .pool()
                        .begin()
                        .await
                        .map_err(|e| pgqrs::error::Error::QueryFailed {
                            query: "BEGIN TRANSACTION".into(),
                            source: Box::new(e),
                            context: "Failed to begin transaction for SQL job".into(),
                        })?;

                let outcome = run_statement_with_timeout(
                    &mut *tx,
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
                let mut conn =
                    store
                        .pool()
                        .acquire()
                        .await
                        .map_err(|e| pgqrs::error::Error::QueryFailed {
                            query: "ACQUIRE CONNECTION".into(),
                            source: Box::new(e),
                            context: "Failed to acquire connection for SQL job".into(),
                        })?;
                run_statement_with_timeout(&mut *conn, statement, payload.get("params"), timeout_ms)
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
    let rows = query
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    };

    println!("Starting pgqrs-sql-worker daemon...");
    println!("Worker Name: {}", args.worker_name);
    println!("Schema: {}", args.schema);
    println!("Queues: {:?}", args.queues);
    println!("Poll Interval: {}ms", args.interval_ms);

    let config = pgqrs::Config::from_dsn_with_schema(&args.dsn, &args.schema)?;
    let store = connect_with_config(&config).await?;

    // Bootstrap tables and function migrations
    store.bootstrap().await?;

    // Spawn a polling task for each queue
    for queue in args.queues {
        let store = store.clone();
        let queue_name = queue.clone();
        let interval_ms = args.interval_ms;
        let worker_name = args.worker_name.clone();

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

    println!("pgqrs-sql-worker daemon is running. Press Ctrl+C to stop.");

    tokio::select! {
        _ = &mut sigint => {
            println!("Received shutdown signal. Stopping pgqrs-sql-worker...");
        }
    }

    Ok(())
}
