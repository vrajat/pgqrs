use serde_json::json;

mod common;

#[tokio::test]
async fn test_sql_workflow_orchestration() {
    let store = common::create_store("test_sql_workflow").await;
    let wf_name = "test_wf_proc";

    // Define the workflow step definitions
    let define_sql = r#"
        SELECT define_workflow(
            'test_wf_proc',
            ARRAY[
                step_def('step_one', 'SELECT :input->>''param_x'' AS value'),
                step_def('step_two', 'SELECT :input->>''param_y'' AS value'),
                step_def('step_three', 'SELECT :step_one->0->>''value'' AS val_from_one')
            ]
        );
    "#;
    sqlx::query(define_sql).execute(store.pool()).await.unwrap();

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

    // Create consumer using standard builder API
    let consumer_name = format!("test-consumer-{}", uuid::Uuid::new_v4());
    let consumer = pgqrs::consumer(&consumer_name, wf_name)
        .create(&store)
        .await
        .unwrap();

    // Get workflow handler from sql_worker library module
    let handler = pgqrs::sql_worker::workflow_handler(&store);

    // Dequeue and execute message using standard DequeueBuilder APIs
    pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .handle(handler)
        .execute(&store)
        .await
        .unwrap();

    // Verify run succeeded
    let run = pgqrs::tables(&store)
        .workflow_runs()
        .get_by_message_id(msg_ids[0])
        .await
        .unwrap();
    assert_eq!(run.status, pgqrs::types::WorkflowStatus::Success);

    let output = run.output.unwrap();
    assert_eq!(output["step_one"][0]["value"], "val_x");
    assert_eq!(output["step_two"][0]["value"], "val_y");
    assert_eq!(output["step_three"][0]["val_from_one"], "val_x");
}

#[tokio::test]
async fn test_sql_workflow_failure_rollback() {
    let store = common::create_store("test_sql_wf_fail").await;
    let wf_name = "test_wf_fail_proc";

    // Define the workflow step definitions where second step fails
    let define_sql = r#"
        SELECT define_workflow(
            'test_wf_fail_proc',
            ARRAY[
                step_def('step_ok', 'SELECT 1'),
                step_def('step_fail', 'SELECT 1 / 0')
            ]
        );
    "#;
    sqlx::query(define_sql).execute(store.pool()).await.unwrap();

    let producer_name = format!("test-producer-5-{}", uuid::Uuid::new_v4());
    let producer = pgqrs::producer(&producer_name, wf_name)
        .create(&store)
        .await
        .unwrap();

    let payload = json!({ "input": serde_json::Value::Null });
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    // Create consumer using standard builder API
    let consumer_name = format!("test-consumer-{}", uuid::Uuid::new_v4());
    let consumer = pgqrs::consumer(&consumer_name, wf_name)
        .create(&store)
        .await
        .unwrap();

    // Get workflow handler from sql_worker library module
    let handler = pgqrs::sql_worker::workflow_handler(&store);

    // Dequeue and execute message using standard DequeueBuilder APIs
    pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .handle(handler)
        .execute(&store)
        .await
        .unwrap();

    // Verify run record shows ERROR
    let run = pgqrs::tables(&store)
        .workflow_runs()
        .get_by_message_id(msg_ids[0])
        .await
        .unwrap();
    assert_eq!(run.status, pgqrs::types::WorkflowStatus::Error);
    let err_msg = run.error.unwrap().as_str().unwrap().to_string();
    assert!(err_msg.contains("division by zero"));
}

#[tokio::test]
async fn test_sql_workflow_dml_and_datatypes() {
    let store = common::create_store("test_sql_dml").await;

    // Create a temporary table for testing DML insert and updates
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_items ( \
            id SERIAL PRIMARY KEY, \
            name TEXT NOT NULL, \
            val INT NOT NULL \
         )",
    )
    .execute(store.pool())
    .await
    .unwrap();

    sqlx::query("TRUNCATE TABLE test_items RESTART IDENTITY CASCADE")
        .execute(store.pool())
        .await
        .unwrap();

    let wf_name = "test_dml_proc";

    // Define workflow step definitions demonstrating:
    // 1. DML with RETURNING (INSERT)
    // 2. DML without RETURNING (UPDATE)
    // 3. Complex Datatypes (Timestamp, UUID, Numeric)
    let define_sql = r#"
        SELECT define_workflow(
            'test_dml_proc',
            ARRAY[
                step_def('insert_item', 'INSERT INTO test_items (name, val) VALUES (:input->>''name'', (:input->>''val'')::int) RETURNING id, name'),
                step_def('update_item', 'UPDATE test_items SET val = val + 10 WHERE id = (:insert_item->0->>''id'')::int'),
                step_def('datatypes', 'SELECT NOW()::timestamp AS t_stamp, ''9b1deb4d-3b7d-4bad-9bdd-2b0d7b3d4bad''::uuid AS u_id, 123.45::numeric AS price')
            ]
        );
    "#;
    sqlx::query(define_sql).execute(store.pool()).await.unwrap();

    // Trigger workflow (enqueue message)
    let input = json!({
        "name": "widget",
        "val": 100
    });

    let producer_name = format!("test-producer-6-{}", uuid::Uuid::new_v4());
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

    // Create consumer using standard builder API
    let consumer_name = format!("test-consumer-{}", uuid::Uuid::new_v4());
    let consumer = pgqrs::consumer(&consumer_name, wf_name)
        .create(&store)
        .await
        .unwrap();

    // Get workflow handler from sql_worker library module
    let handler = pgqrs::sql_worker::workflow_handler(&store);

    // Dequeue and execute message using standard DequeueBuilder APIs
    pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .handle(handler)
        .execute(&store)
        .await
        .unwrap();

    let run = pgqrs::tables(&store)
        .workflow_runs()
        .get_by_message_id(msg_ids[0])
        .await
        .unwrap();

    if run.status == pgqrs::types::WorkflowStatus::Error {
        panic!("Workflow run failed with error: {:?}", run.error);
    }
    assert_eq!(run.status, pgqrs::types::WorkflowStatus::Success);

    let output = run.output.unwrap();

    // 1. DML with RETURNING: should contain a JSON array with the inserted row
    let insert_output = &output["insert_item"];
    assert!(insert_output.is_array());
    assert_eq!(insert_output[0]["name"], "widget");
    let inserted_id = insert_output[0]["id"].as_i64().unwrap();

    // 2. DML without RETURNING: should return an empty JSON array `[]`
    let update_output = &output["update_item"];
    assert!(update_output.is_array());
    assert!(update_output.as_array().unwrap().is_empty());

    // Verify the update actually took effect in the database
    let updated_val: i32 = sqlx::query_scalar("SELECT val FROM test_items WHERE id = $1")
        .bind(inserted_id as i32)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(updated_val, 110); // 100 + 10

    // 3. Complex Datatypes: timestamps, uuids, and numeric types should be serialized successfully
    let datatypes_output = &output["datatypes"];
    assert!(datatypes_output.is_array());
    assert_eq!(
        datatypes_output[0]["u_id"],
        "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3d4bad"
    );
    assert_eq!(datatypes_output[0]["price"], 123.45);
    assert!(datatypes_output[0]["t_stamp"].as_str().is_some());
}
