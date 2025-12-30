use pgqrs::{pgqrs_step, pgqrs_workflow, Config, Workflow};

use serde::{Deserialize, Serialize};

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

#[pgqrs_step]
#[allow(unused_variables)] // Test attribute forwarding (should not warn about arg2)
async fn step_multi_args(ctx: &Workflow, arg1: &str, arg2: i32) -> anyhow::Result<TestData> {
    Ok(TestData {
        msg: format!("multi: {}", arg1),
    })
}

#[pgqrs_step]
async fn step_side_effect(_ctx: &Workflow, _input: &str) -> anyhow::Result<TestData> {
    // This step returns a value that we will manually tamper with in the DB
    // to prove that the second execution returns the DB value, not this value.
    Ok(TestData {
        msg: "original_value".to_string(),
    })
}

#[pgqrs_step]
async fn step_fail(ctx: &Workflow, _input: &str) -> anyhow::Result<TestData> {
    anyhow::bail!("step failed intentionally")
}

#[pgqrs_workflow]
async fn my_workflow(ctx: &Workflow, input: &TestData) -> anyhow::Result<TestData> {
    // Step 1
    let s1 = step_one(ctx, "input").await?;

    // Step with multiple args
    let s2 = step_multi_args(ctx, "arg", 42).await?;

    // Return combined result
    Ok(TestData {
        msg: format!("{}, {}, {}", input.msg, s1.msg, s2.msg),
    })
}

#[pgqrs_workflow]
async fn workflow_with_failing_step(ctx: &Workflow, _input: &TestData) -> anyhow::Result<TestData> {
    let _ = step_fail(ctx, "fail").await?;
    Ok(TestData {
        msg: "should not happen".to_string(),
    })
}

#[pgqrs_workflow]
async fn workflow_fail_at_end(ctx: &Workflow, _input: &TestData) -> anyhow::Result<TestData> {
    anyhow::bail!("workflow failed intentionally")
}

#[tokio::test]
async fn test_macro_suite() -> anyhow::Result<()> {
    // Setup
    let schema = "macro_test_suite_v3";
    let dsn = common::get_postgres_dsn(Some(schema)).await;
    let config = Config::from_dsn_with_schema(&dsn, schema)?;
    let store = pgqrs::store::AnyStore::connect(&config).await?;
    pgqrs::admin(&store).install().await?;
    let pool = store.pool();

    // --- CASE 0: Creation State (Pending) ---
    {
        let input = TestData {
            msg: "pending_check".to_string(),
        };
        let workflow = Workflow::create(pool.clone(), "pending_wf", &input).await?;
        let workflow_id = workflow.id();

        // Verify status is PENDING immediately after creation
        let record = pgqrs::tables(&store).workflows().get(workflow_id).await?;
        assert_eq!(
            record.status,
            pgqrs::workflow::WorkflowStatus::Pending,
            "Workflow should be PENDING upon creation"
        );
    }

    // --- CASE 1: Successful Workflow (with multi-arg step) ---
    {
        let input = TestData {
            msg: "start".to_string(),
        };
        let workflow = Workflow::create(pool.clone(), "my_workflow", &input).await?;
        let workflow_id = workflow.id();

        let res = my_workflow(&workflow, &input).await?;
        assert_eq!(res.msg, "start, step1_done, multi: arg");

        // Verify persisting SUCCESS
        let record = pgqrs::tables(&store).workflows().get(workflow_id).await?;
        assert_eq!(record.status, pgqrs::workflow::WorkflowStatus::Success);
        let db_output: TestData = serde_json::from_value(record.output.unwrap())?;
        assert_eq!(db_output.msg, "start, step1_done, multi: arg");
    }

    // --- CASE 2: Step Idempotency (Retry logic) ---
    {
        // 1. Create context
        let input = TestData {
            msg: "idempotency".to_string(),
        };
        let workflow = Workflow::create(pool.clone(), "idempotency_wf", &input).await?;
        let workflow_id = workflow.id();

        // 2. Run step first time -> Success
        let res1 = step_side_effect(&workflow, "run1").await?;
        assert_eq!(res1.msg, "original_value");

        // 3. Manually TAMPER with the step output in the database
        // This proves that the next call reads from DB instead of running function logic
        let tampered_json = serde_json::json!({ "msg": "tampered_value" });
        sqlx::query("UPDATE pgqrs_workflow_steps SET output = $1 WHERE workflow_id = $2 AND step_id = 'step_side_effect'")
            .bind(tampered_json)
            .bind(workflow_id)
            .execute(pool)
            .await?;

        // 4. Run step second time -> Should return TAMPERED value
        let res2 = step_side_effect(&workflow, "run2").await?;
        assert_eq!(
            res2.msg, "tampered_value",
            "Step should have returned cached (tampered) result from DB"
        );
    }

    // --- CASE 3: Step Failure ---
    {
        let input = TestData {
            msg: "fail_step".to_string(),
        };
        let workflow = Workflow::create(pool.clone(), "workflow_with_failing_step", &input).await?;
        let workflow_id = workflow.id();

        let res = workflow_with_failing_step(&workflow, &input).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "step failed intentionally");

        // Verify persistence
        let record = pgqrs::tables(&store).workflows().get(workflow_id).await?;
        assert_eq!(record.status, pgqrs::workflow::WorkflowStatus::Error);
        let error_val = record.error.expect("Should have error");
        let error_str = error_val.as_str().expect("Error should be string");
        assert!(error_str.contains("step failed intentionally"));
    }

    // --- CASE 4: Workflow Failure ---
    {
        let input = TestData {
            msg: "fail_wf".to_string(),
        };
        let workflow = Workflow::create(pool.clone(), "workflow_fail_at_end", &input).await?;
        let workflow_id = workflow.id();

        let res = workflow_fail_at_end(&workflow, &input).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "workflow failed intentionally"
        );

        // Verify persistence
        let record = pgqrs::tables(&store).workflows().get(workflow_id).await?;
        assert_eq!(record.status, pgqrs::workflow::WorkflowStatus::Error);
        let error_val = record.error.expect("Should have error");
        let error_str = error_val.as_str().expect("Error should be string");
        assert_eq!(error_str, "workflow failed intentionally");
    }

    Ok(())
}
