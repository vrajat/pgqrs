use pgqrs::{pgqrs_step, pgqrs_workflow, Run, Store};
use serde::{Deserialize, Serialize};

mod common;

async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    common::create_store(schema).await
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

#[pgqrs_step]
async fn step_one(ctx: &Run, _input: &str) -> anyhow::Result<TestData> {
    Ok(TestData {
        msg: "step1_done".to_string(),
    })
}

#[pgqrs_step]
#[allow(unused_variables)] // Test attribute forwarding (should not warn about arg2)
async fn step_multi_args(ctx: &Run, arg1: &str, arg2: i32) -> anyhow::Result<TestData> {
    Ok(TestData {
        msg: format!("multi: {}", arg1),
    })
}

#[pgqrs_step]
async fn step_side_effect(_ctx: &Run, _input: &str) -> anyhow::Result<TestData> {
    // This step returns a value that we will manually tamper with in the DB
    // to prove that the second execution returns the DB value, not this value.
    Ok(TestData {
        msg: "original_value".to_string(),
    })
}

#[pgqrs_step]
async fn step_fail(ctx: &Run, _input: &str) -> anyhow::Result<TestData> {
    anyhow::bail!("step failed intentionally")
}

#[pgqrs_workflow]
async fn my_workflow(ctx: &Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let input: TestData = serde_json::from_value(input)?;
    // Step 1
    let s1 = step_one(ctx, "input").await?;

    // Step with multiple args
    let s2 = step_multi_args(ctx, "arg", 42).await?;

    // Return combined result
    Ok(serde_json::to_value(TestData {
        msg: format!("{}, {}, {}", input.msg, s1.msg, s2.msg),
    })?)
}

#[pgqrs_workflow]
async fn workflow_with_failing_step(
    ctx: &Run,
    _input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let _ = step_fail(ctx, "fail").await?;
    Ok(serde_json::json!({"msg": "should not happen"}))
}

#[pgqrs_workflow]
async fn workflow_fail_at_end(
    _ctx: &Run,
    _input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    anyhow::bail!("workflow failed intentionally")
}

#[tokio::test]
async fn test_workflow_creation_state() -> anyhow::Result<()> {
    let store = create_store("macro_test_creation").await;
    let input = TestData {
        msg: "pending_check".to_string(),
    };
    // Use a JSON workflow def for the builder API.
    #[pgqrs_workflow(name = "pending_wf")]
    async fn pending_wf(_ctx: &Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        Ok(input)
    }

    pgqrs::workflow().name(pending_wf).create(&store).await?;
    let run_msg = pgqrs::workflow()
        .name(pending_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    // Verify status is QUEUED immediately after creation
    let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
    assert_eq!(
        record.status,
        pgqrs::WorkflowStatus::Queued,
        "Run should be QUEUED upon creation"
    );
    Ok(())
}

#[tokio::test]
async fn test_successful_workflow() -> anyhow::Result<()> {
    let store = create_store("macro_test_success").await;
    let input = TestData {
        msg: "start".to_string(),
    };
    pgqrs::workflow().name(my_workflow).create(&store).await?;
    let run_msg = pgqrs::workflow()
        .name(my_workflow)
        .trigger(&input)?
        .execute(&store)
        .await?;

    // Execute via the workflow engine (run lifecycle + persistence).
    let handler = pgqrs::workflow_handler(store.clone(), my_workflow.runner());
    handler(run_msg.clone()).await?;

    let my_wf_run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    // Verify persisting SUCCESS
    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get(my_wf_run.id())
        .await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Success);
    let db_output: TestData = serde_json::from_value(record.output.unwrap())?;
    assert_eq!(db_output.msg, "start, step1_done, multi: arg");
    Ok(())
}

#[tokio::test]
async fn test_step_idempotency() -> anyhow::Result<()> {
    let store = create_store("macro_test_idempotency").await;

    #[pgqrs_workflow(name = "idempotency_wf")]
    async fn idempotency_wf(
        _ctx: &Run,
        input: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(input)
    }

    // 1. Create context
    let input = TestData {
        msg: "idempotency".to_string(),
    };
    pgqrs::workflow()
        .name(idempotency_wf)
        .create(&store)
        .await?;
    let run_msg = pgqrs::workflow()
        .name(idempotency_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let mut idem_wf_run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    idem_wf_run = idem_wf_run.start().await?;

    // 2. Run step first time -> Success
    let res1 = step_side_effect(&idem_wf_run, "run1").await?;
    assert_eq!(res1.msg, "original_value");

    // 3. Manually TAMPER with the step output in the database
    // This proves that the next call reads from DB instead of running function logic
    let tampered_json = serde_json::json!({ "msg": "tampered_value" });
    let tampered_json_sql = tampered_json.to_string().replace('\'', "''");
    let step_col = "step_name";
    let update_sql = format!(
        "UPDATE pgqrs_workflow_steps SET output = '{}' WHERE run_id = {} AND {} = 'step_side_effect'",
        tampered_json_sql, idem_wf_run.id(), step_col
    );
    store.execute_raw(&update_sql).await?;

    // 4. Run step second time -> Should return TAMPERED value
    let res2 = step_side_effect(&idem_wf_run, "run2").await?;
    assert_eq!(
        res2.msg, "tampered_value",
        "Step should have returned cached (tampered) result from DB"
    );
    Ok(())
}

#[tokio::test]
async fn test_step_failure() -> anyhow::Result<()> {
    let store = create_store("macro_test_step_failure").await;
    let input = TestData {
        msg: "fail_step".to_string(),
    };
    pgqrs::workflow()
        .name(workflow_with_failing_step)
        .create(&store)
        .await?;
    let run_msg = pgqrs::workflow()
        .name(workflow_with_failing_step)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let handler = pgqrs::workflow_handler(store.clone(), workflow_with_failing_step.runner());
    let res = handler(run_msg.clone()).await;
    assert!(res.is_ok());

    let wf_failing_step_run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    // Verify persistence
    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get(wf_failing_step_run.id())
        .await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Error);
    let error_val = record.error.expect("Should have error");
    let error_str = error_val
        .as_str()
        .or_else(|| error_val.get("message").and_then(|v| v.as_str()))
        .unwrap_or("");
    assert!(error_str.contains("step failed intentionally"));
    Ok(())
}

#[tokio::test]
async fn test_workflow_failure() -> anyhow::Result<()> {
    let store = create_store("macro_test_workflow_failure").await;
    let input = TestData {
        msg: "fail_wf".to_string(),
    };
    pgqrs::workflow()
        .name(workflow_fail_at_end)
        .create(&store)
        .await?;
    let run_msg = pgqrs::workflow()
        .name(workflow_fail_at_end)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let handler = pgqrs::workflow_handler(store.clone(), workflow_fail_at_end.runner());
    let res = handler(run_msg.clone()).await;
    assert!(res.is_ok());

    let wf_fail_run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    // Verify persistence
    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get(wf_fail_run.id())
        .await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Error);
    let error_val = record.error.expect("Should have error");
    let error_str = error_val
        .as_str()
        .or_else(|| error_val.get("message").and_then(|v| v.as_str()))
        .unwrap_or("");
    assert!(error_str.contains("workflow failed intentionally"));
    Ok(())
}

#[pgqrs_workflow]
async fn workflow_no_args(_ctx: &Run) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::json!({"status": "no_args_success"}))
}

#[tokio::test]
async fn test_workflow_no_args() -> anyhow::Result<()> {
    let store = create_store("macro_test_run_metadata").await;
    pgqrs::workflow()
        .name(workflow_no_args)
        .create(&store)
        .await?;
    let run_msg = pgqrs::workflow()
        .name(workflow_no_args)
        .trigger(&())? // Trigger with unit payload
        .execute(&store)
        .await?;

    let handler = pgqrs::workflow_handler(store.clone(), workflow_no_args.runner());
    let res = handler(run_msg.clone()).await;
    assert!(res.is_ok());

    let wf_run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    // Verify persistence
    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get(wf_run.id())
        .await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Success);

    let db_output = record.output.unwrap();
    assert_eq!(db_output.get("status").unwrap(), "no_args_success");
    Ok(())
}
