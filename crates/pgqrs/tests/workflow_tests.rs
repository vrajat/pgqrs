use pgqrs::store::AnyStore;
use pgqrs::Run;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use pgqrs::pgqrs_workflow;

mod common;

#[pgqrs_workflow(name = "test_wf")]
async fn test_wf(_run: &pgqrs::Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "fail_wf")]
async fn fail_wf(_run: &pgqrs::Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "pause_wf")]
async fn pause_wf(
    _run: &pgqrs::Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "get_wf")]
async fn get_wf(_run: &pgqrs::Run, input: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "polling_wf")]
async fn polling_wf(
    _run: &pgqrs::Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "error_wf")]
async fn error_wf(
    _run: &pgqrs::Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

async fn create_store() -> AnyStore {
    common::create_store("workflow_tests").await
}

#[tokio::test]
async fn test_workflow_success_lifecycle() -> anyhow::Result<()> {
    let store = create_store().await;

    // Create definition
    pgqrs::workflow().name(test_wf).create(&store).await?;

    let input = TestData {
        msg: "start".to_string(),
    };

    let run_msg = pgqrs::workflow()
        .name(test_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let mut workflow = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    workflow = workflow.start().await?;

    // Step 1: Run
    let step1_name = "step1";
    let _step_rec = pgqrs::step()
        .run(&workflow)
        .name(step1_name)
        .execute()
        .await?;

    // Finish Workflow
    workflow = workflow
        .complete(serde_json::to_value(&TestData {
            msg: "done".to_string(),
        })?)
        .await?;

    // Restart Workflow (should adhere to SUCCESS terminal state)
    let res = workflow.start().await;
    assert!(
        res.is_err(),
        "Workflow start should fail if currently SUCCESS"
    );

    Ok(())
}

#[tokio::test]
async fn test_workflow_failure_lifecycle() -> anyhow::Result<()> {
    let store = create_store().await;

    // Verify Workflow Failure Logic using new workflow
    let input_fail = TestData {
        msg: "fail".to_string(),
    };
    pgqrs::workflow().name(fail_wf).create(&store).await?;

    let run_msg = pgqrs::workflow()
        .name(fail_wf)
        .trigger(&input_fail)?
        .execute(&store)
        .await?;

    let mut wf_fail = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    wf_fail = wf_fail.start().await?;

    wf_fail = wf_fail
        .fail(&TestData {
            msg: "failed".to_string(),
        })
        .await?;

    // Restart Failed Workflow (should fail because ERROR is terminal)
    let res = wf_fail.start().await;
    assert!(
        res.is_err(),
        "Workflow start should fail if currently ERROR"
    );

    Ok(())
}

#[tokio::test]
async fn test_workflow_pause_resume_lifecycle() -> anyhow::Result<()> {
    let store = create_store().await;

    pgqrs::workflow().name(pause_wf).create(&store).await?;

    let run_msg = pgqrs::workflow()
        .name(pause_wf)
        .trigger(&TestData {
            msg: "pause".to_string(),
        })?
        .execute(&store)
        .await?;

    let mut run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    run = run.start().await?;
    run = run
        .pause("wait".to_string(), Duration::from_secs(30))
        .await?;

    let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Paused);

    run = run.start().await?;

    let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Running);

    Ok(())
}

#[tokio::test]
async fn test_workflow_result_get_non_blocking() -> anyhow::Result<()> {
    let store = create_store().await;

    pgqrs::workflow().name(get_wf).create(&store).await?;

    let input = TestData {
        msg: "get_test".to_string(),
    };

    let run_msg = pgqrs::workflow()
        .name(get_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    // 1. Check immediately - should be None (not even created yet)
    let res: Option<TestData> = pgqrs::run()
        .store(&store)
        .message(run_msg.clone())
        .get()
        .await?;
    assert!(res.is_none());

    // 2. Start the run
    let mut run = pgqrs::run()
        .message(run_msg.clone())
        .store(&store)
        .execute()
        .await?;
    run = run.start().await?;

    // 3. Check again - still None (Running)
    let res: Option<TestData> = pgqrs::run()
        .store(&store)
        .message(run_msg.clone())
        .get()
        .await?;
    assert!(res.is_none());

    // 4. Complete the run
    run.complete(serde_json::json!({"msg": "finished"})).await?;

    // 5. Check again - should be Some
    let res: Option<TestData> = pgqrs::run()
        .store(&store)
        .message(run_msg.clone())
        .get()
        .await?;
    assert_eq!(res.unwrap().msg, "finished");

    Ok(())
}

#[tokio::test]
async fn test_workflow_result_polling() -> anyhow::Result<()> {
    let store = create_store().await;

    pgqrs::workflow().name(polling_wf).create(&store).await?;

    let input = TestData {
        msg: "poll_me".to_string(),
    };

    let run_msg = pgqrs::workflow()
        .name(polling_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let expected_output = TestData {
        msg: "polled_success".to_string(),
    };

    let run_msg_clone = run_msg.clone();
    let store_clone = store.clone();
    let output_clone = expected_output.msg.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut wf = pgqrs::run()
            .message(run_msg_clone)
            .store(&store_clone)
            .execute()
            .await
            .expect("Failed to get run handle in spawned task");

        wf = wf.start().await.expect("Failed to start run");
        wf.complete(serde_json::json!({"msg": output_clone}))
            .await
            .expect("Failed to complete run");
    });

    // Use the polling API
    let actual_output: TestData = pgqrs::run().store(&store).message(run_msg).result().await?;

    assert_eq!(actual_output, expected_output);

    Ok(())
}

#[tokio::test]
async fn test_workflow_error_polling() -> anyhow::Result<()> {
    let store = create_store().await;

    pgqrs::workflow().name(error_wf).create(&store).await?;

    let run_msg = pgqrs::workflow()
        .name(error_wf)
        .trigger(&TestData {
            msg: "fail".to_string(),
        })?
        .execute(&store)
        .await?;

    let run_msg_clone = run_msg.clone();
    let store_clone = store.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut wf = pgqrs::run()
            .message(run_msg_clone)
            .store(&store_clone)
            .execute()
            .await
            .expect("Failed to get run handle");

        wf = wf.start().await.expect("Failed to start run");
        wf.fail(&serde_json::json!({"error": "intentional failure"}))
            .await
            .expect("Failed to fail run");
    });

    // Use the polling API and expect error
    let res: Result<TestData, _> = pgqrs::run().store(&store).message(run_msg).result().await;

    match res {
        Err(pgqrs::error::Error::ExecutionFailed { error, .. }) => {
            assert_eq!(error, serde_json::json!({"error": "intentional failure"}));
        }
        other => panic!("Expected ExecutionFailed error, got {:?}", other),
    }

    Ok(())
}
