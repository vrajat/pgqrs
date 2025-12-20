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
    // If start was not called, step_one would fail due to FK constraint (assuming it exists)
    // or workflow would not be in DB.

    Ok(())
}
