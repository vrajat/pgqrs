use pgqrs::QueueMessage;
use pgqrs::Store;
use serde_json::Value;
use sqlx::{Column, Row, TypeInfo};
use std::time::Duration;

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
    let store = pgqrs::connect_with_config(&config).await?;

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
                run_statement_with_timeout(&mut conn, statement, payload.get("params"), timeout_ms)
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
