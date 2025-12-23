use pgqrs::{pgqrs_step, pgqrs_workflow, Admin, Config, Workflow};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod common;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

#[pgqrs_step]
async fn step_one(ctx: &Workflow, _input: &str) -> anyhow::Result<TestData> {
    Ok(TestData {
        msg: "step1_done".to_string(),
    })
}

#[pgqrs_workflow]
async fn my_workflow(ctx: &Workflow, input: &TestData) -> anyhow::Result<TestData> {
    // Step 1
    let s1 = step_one(ctx, "input").await?;

    // Return combined result
    Ok(TestData {
        msg: format!("{}, {}", input.msg, s1.msg),
    })
}

#[tokio::test]
async fn test_macro_workflow() -> anyhow::Result<()> {
    // Setup
    let schema = "macro_test";
    let dsn = common::get_postgres_dsn(Some(schema)).await;
    let config = Config::from_dsn_with_schema(&dsn, schema)?;
    let admin = Admin::new(&config).await?;
    admin.install().await?;
    let pool = admin.pool.clone();

    let workflow_id = Uuid::new_v4();
    let workflow = Workflow::new(pool.clone(), workflow_id);

    // Call workflow (implicit start)
    // The macro should call workflow.start(), then run body, then workflow.success()
    let input = TestData {
        msg: "start".to_string(),
    };
    let res = my_workflow(&workflow, &input).await?;

    assert_eq!(res.msg, "start, step1_done");

    // Start verification:
    // Verify that the workflow is marked as SUCCESS in the database
    let row: (String, serde_json::Value) = sqlx::query_as(
        "SELECT status::text, output FROM pgqrs_workflows WHERE workflow_id = $1"
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(row.0, "SUCCESS");

    let db_output: TestData = serde_json::from_value(row.1)?;
    assert_eq!(db_output.msg, "start, step1_done");

    Ok(())
}
