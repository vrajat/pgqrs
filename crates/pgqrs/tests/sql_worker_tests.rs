use serde_json::{json, Value};
use sqlx::{Column, Row, TypeInfo};
use pgqrs::{Store, QueueMessage};

mod common;

// Embed the executor logic from the binary for integration testing
async fn process_message(store: &Store, msg: QueueMessage, queue_name: &str) -> pgqrs::error::Result<()> {
    // 1. Try to treat it as a workflow execution
    match store.run(msg.clone()).await {
        Ok(run) => {
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
                    Ok(())
                }
                Err(e) => {
                    let err_payload = serde_json::json!({
                        "message": e.to_string()
                    });
                    run.fail_with_json(err_payload).await?;
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
            let statement = payload.get("statement").and_then(|v| v.as_str()).ok_or_else(|| {
                pgqrs::error::Error::ValidationFailed {
                    reason: "Missing 'statement' string field in SQL job payload".to_string(),
                }
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
            let timeout_ms = payload.get("statement_timeout_ms").and_then(|v| v.as_u64()).unwrap_or(30000);

            let exec_outcome = if use_tx {
                let mut tx = store.pool().begin().await.map_err(|e| pgqrs::error::Error::QueryFailed {
                    query: "BEGIN TRANSACTION".into(),
                    source: Box::new(e),
                    context: "Failed to begin transaction for SQL job".into(),
                })?;
                
                let outcome = run_statement_with_timeout(&mut *tx, statement, payload.get("params"), timeout_ms).await;
                if outcome.is_ok() {
                    tx.commit().await.map_err(|e| pgqrs::error::Error::QueryFailed {
                        query: "COMMIT TRANSACTION".into(),
                        source: Box::new(e),
                        context: "Failed to commit transaction for SQL job".into(),
                    })?;
                } else {
                    let _ = tx.rollback().await;
                }
                outcome
            } else {
                let mut conn = store.pool().acquire().await.map_err(|e| pgqrs::error::Error::QueryFailed {
                    query: "ACQUIRE CONNECTION".into(),
                    source: Box::new(e),
                    context: "Failed to acquire connection for SQL job".into(),
                })?;
                run_statement_with_timeout(&mut *conn, statement, payload.get("params"), timeout_ms).await
            };

            match exec_outcome {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
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
    let timeout_sql = format!("SET LOCAL statement_timeout = {}", timeout_ms);
    sqlx::query(&timeout_sql).execute(&mut *conn).await.map_err(|e| {
        pgqrs::error::Error::QueryFailed {
            query: timeout_sql,
            source: Box::new(e),
            context: "Failed to set statement timeout".into(),
        }
    })?;

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

    let rows = query.fetch_all(&mut *conn).await.map_err(|e| {
        pgqrs::error::Error::QueryFailed {
            query: statement.to_string(),
            source: Box::new(e),
            context: "Failed to execute SQL statement".into(),
        }
    })?;

    let mut result_rows = Vec::new();
    for row in rows {
        let mut row_obj = serde_json::Map::new();
        for col in row.columns() {
            let name = col.name();
            let val = match col.type_info().name() {
                "INT8" | "BIGINT" => row.try_get::<i64, _>(name).map(Value::from).unwrap_or(Value::Null),
                "INT4" | "INTEGER" => row.try_get::<i32, _>(name).map(Value::from).unwrap_or(Value::Null),
                "INT2" | "SMALLINT" => row.try_get::<i16, _>(name).map(Value::from).unwrap_or(Value::Null),
                "FLOAT8" | "DOUBLE PRECISION" => row.try_get::<f64, _>(name).map(Value::from).unwrap_or(Value::Null),
                "FLOAT4" | "REAL" => row.try_get::<f32, _>(name).map(Value::from).unwrap_or(Value::Null),
                "BOOL" | "BOOLEAN" => row.try_get::<bool, _>(name).map(Value::from).unwrap_or(Value::Null),
                "TEXT" | "VARCHAR" | "CHAR" | "NAME" => row.try_get::<String, _>(name).map(Value::from).unwrap_or(Value::Null),
                "JSON" | "JSONB" => row.try_get::<Value, _>(name).unwrap_or(Value::Null),
                _ => {
                    row.try_get::<String, _>(name).map(Value::from).unwrap_or(Value::Null)
                }
            };
            row_obj.insert(name.to_string(), val);
        }
        result_rows.push(Value::Object(row_obj));
        if result_rows.len() >= 100 {
            break;
        }
    }

    Ok(Value::Array(result_rows))
}

async fn get_or_create_queue(store: &Store, queue_name: &str) -> pgqrs::types::QueueRecord {
    match store.queue(queue_name).await {
        Ok(q) => q,
        Err(pgqrs::error::Error::QueueAlreadyExists { .. }) => {
            pgqrs::tables(store).queues().get_by_name(queue_name).await.unwrap()
        }
        Err(e) => panic!("{}", e),
    }
}

async fn get_or_create_workflow(store: &Store, wf_name: &str) -> pgqrs::types::WorkflowRecord {
    match store.workflow(wf_name).await {
        Ok(w) => w,
        Err(pgqrs::error::Error::WorkflowAlreadyExists { .. }) => {
            pgqrs::tables(store).workflows().get_by_name(wf_name).await.unwrap()
        }
        Err(e) => panic!("{}", e),
    }
}

#[tokio::test]
async fn test_standalone_sql_job_success() {
    let store = common::create_store("test_sql_job_success").await;
    let queue_name = "test_sql_success_queue";
    
    // Create queue
    let _queue = get_or_create_queue(&store, queue_name).await;

    // Create a temporary table for verification (drop first to be idempotent)
    sqlx::query("DROP TABLE IF EXISTS test_job_table")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE test_job_table (val TEXT)")
        .execute(store.pool())
        .await
        .unwrap();

    // Enqueue statement
    let payload = json!({
        "statement": "INSERT INTO test_job_table (val) VALUES ($1)",
        "params": ["success_message"]
    });
    
    let producer_name = format!("test-producer-1-{}", uuid::Uuid::new_v4());
    let producer = pgqrs::producer(&producer_name, queue_name)
        .create(&store)
        .await
        .unwrap();

    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg = pgqrs::tables(&store).messages().get(msg_ids[0]).await.unwrap();

    // Process
    process_message(&store, msg, queue_name).await.unwrap();

    // Verify row was inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_job_table WHERE val = 'success_message'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_standalone_sql_job_select() {
    let store = common::create_store("test_sql_job_select").await;
    let queue_name = "test_sql_select_queue";
    
    let _queue = get_or_create_queue(&store, queue_name).await;

    // Enqueue SELECT query
    let payload = json!({
        "statement": "SELECT 100::int8 AS num, 'demo'::text AS name",
        "params": []
    });
    
    let producer_name = format!("test-producer-2-{}", uuid::Uuid::new_v4());
    let producer = pgqrs::producer(&producer_name, queue_name)
        .create(&store)
        .await
        .unwrap();

    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg = pgqrs::tables(&store).messages().get(msg_ids[0]).await.unwrap();
    process_message(&store, msg, queue_name).await.unwrap();
}

#[tokio::test]
async fn test_standalone_sql_job_safety_violation() {
    let store = common::create_store("test_sql_job_safety").await;
    let queue_name = "test_sql_safety_queue";
    
    let _queue = get_or_create_queue(&store, queue_name).await;

    // Try transaction block
    let payload = json!({
        "statement": "BEGIN; SELECT 1; COMMIT;",
        "params": []
    });
    
    let producer_name = format!("test-producer-3-{}", uuid::Uuid::new_v4());
    let producer = pgqrs::producer(&producer_name, queue_name)
        .create(&store)
        .await
        .unwrap();

    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg = pgqrs::tables(&store).messages().get(msg_ids[0]).await.unwrap();
    let res = process_message(&store, msg, queue_name).await;
    assert!(res.is_err());
    
    if let Err(pgqrs::error::Error::ValidationFailed { reason }) = res {
        assert!(reason.contains("Transaction control commands"));
    } else {
        panic!("Expected ValidationFailed error");
    }
}

#[tokio::test]
async fn test_sql_workflow_orchestration() {
    let store = common::create_store("test_sql_workflow").await;
    let wf_name = "test_wf_proc";
    
    // Register workflow definition (automatically creates queue)
    let wf = get_or_create_workflow(&store, wf_name).await;

    // Create the workflow PL/pgSQL function
    let func_sql = r#"
        CREATE OR REPLACE FUNCTION test_wf_proc(run_id BIGINT, input JSONB) RETURNS JSONB AS $$
        DECLARE
            r1 JSONB;
            r2 JSONB;
        BEGIN
            r1 := execute_sql_step(run_id, 'step_one', 'SELECT $1->>''param_x'' AS value', input);
            r2 := execute_sql_step(run_id, 'step_two', 'SELECT $1->>''param_y'' AS value', input);
            RETURN jsonb_build_object('step_one_out', r1, 'step_two_out', r2);
        END;
        $$ LANGUAGE plpgsql;
    "#;
    sqlx::query(func_sql).execute(store.pool()).await.unwrap();

    // Trigger workflow (enqueue message)
    let input = json!({
        "param_x": "val_x",
        "param_y": "val_y"
    });
    
    let producer_name = format!("test-producer-4-{}", uuid::Uuid::new_v4());
    let producer = pgqrs::producer(&producer_name, wf_name)
        .create(&store)
        .await
        .unwrap();

    let payload = json!({ "input": input });
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg = pgqrs::tables(&store).messages().get(msg_ids[0]).await.unwrap();

    // Insert workflow run record manually matching the enqueued message
    let run = pgqrs::tables(&store).workflow_runs().insert(pgqrs::types::NewRunRecord {
        workflow_id: wf.id,
        message_id: msg.id,
        input: Some(input),
    }).await.unwrap();

    // Process workflow message
    process_message(&store, msg, wf_name).await.unwrap();

    // Verify run succeeded
    let run = pgqrs::tables(&store).workflow_runs().get(run.id).await.unwrap();
    assert_eq!(run.status, pgqrs::types::WorkflowStatus::Success);
    
    let output = run.output.unwrap();
    assert_eq!(output["step_one_out"][0]["value"], "val_x");
    assert_eq!(output["step_two_out"][0]["value"], "val_y");
}

#[tokio::test]
async fn test_sql_workflow_failure_rollback() {
    let store = common::create_store("test_sql_wf_fail").await;
    let wf_name = "test_wf_fail_proc";
    
    let wf = get_or_create_workflow(&store, wf_name).await;

    // PL/pgSQL function raising exception
    let func_sql = r#"
        CREATE OR REPLACE FUNCTION test_wf_fail_proc(run_id BIGINT, input JSONB) RETURNS JSONB AS $$
        BEGIN
            PERFORM execute_sql_step(run_id, 'step_ok', 'SELECT 1');
            RAISE EXCEPTION 'Fatal workflow error';
        END;
        $$ LANGUAGE plpgsql;
    "#;
    sqlx::query(func_sql).execute(store.pool()).await.unwrap();

    let producer_name = format!("test-producer-5-{}", uuid::Uuid::new_v4());
    let producer = pgqrs::producer(&producer_name, wf_name)
        .create(&store)
        .await
        .unwrap();

    let payload = json!({ "input": Value::Null });
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg = pgqrs::tables(&store).messages().get(msg_ids[0]).await.unwrap();

    // Insert run
    let run = pgqrs::tables(&store).workflow_runs().insert(pgqrs::types::NewRunRecord {
        workflow_id: wf.id,
        message_id: msg.id,
        input: None,
    }).await.unwrap();

    // Run processing
    let res = process_message(&store, msg, wf_name).await;
    assert!(res.is_err());

    // Verify run record shows ERROR
    let run = pgqrs::tables(&store).workflow_runs().get(run.id).await.unwrap();
    assert_eq!(run.status, pgqrs::types::WorkflowStatus::Error);
    assert!(run.error.unwrap()["message"].as_str().unwrap().contains("Fatal workflow error"));
}


