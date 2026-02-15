use pgqrs::store::AnyStore;
use pgqrs::RunExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod common;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

async fn create_store(schema: &str) -> AnyStore {
    common::create_store(schema).await
}

#[tokio::test]
async fn test_workflow_success_lifecycle() -> anyhow::Result<()> {
    let store = create_store("workflow_tests").await;

    // Create definition
    pgqrs::workflow()
        .name("test_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "start".to_string(),
    };

    let run_msg = pgqrs::workflow()
        .name("test_wf")
        .store(&store)
        .trigger(&input)?
        .execute()
        .await?;

    let mut workflow = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    workflow.start().await?;

    // Step 1: Run
    let step1_name = "step1";
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step1_name)
        .execute()
        .await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);

    let output = TestData {
        msg: "step1_done".to_string(),
    };
    let val = serde_json::to_value(&output)?;
    workflow.complete_step(step1_name, val).await?;

    // Step 1: Rerun (should skip)
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step1_name)
        .execute()
        .await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Success);
    let val: TestData = serde_json::from_value(step_rec.output.unwrap())?;
    assert_eq!(val.msg, "step1_done");

    // Step 2: Acquire
    let step2_name = "step2";
    let _step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step2_name)
        .execute()
        .await?;

    // Finish Workflow
    workflow
        .complete(serde_json::to_value(&TestData {
            msg: "done".to_string(),
        })?)
        .await?;

    // Restart Workflow (should adhere to SUCCESS terminal state)
    workflow.start().await?;

    Ok(())
}

#[tokio::test]
async fn test_workflow_failure_lifecycle() -> anyhow::Result<()> {
    let store = create_store("workflow_tests").await;

    // Verify Workflow Failure Logic using new workflow
    let input_fail = TestData {
        msg: "fail".to_string(),
    };
    pgqrs::workflow()
        .name("fail_wf")
        .store(&store)
        .create()
        .await?;

    let run_msg = pgqrs::workflow()
        .name("fail_wf")
        .store(&store)
        .trigger(&input_fail)?
        .execute()
        .await?;

    let mut wf_fail = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;
    wf_fail.start().await?;
    wf_fail
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
    let store = create_store("workflow_tests").await;

    pgqrs::workflow()
        .name("pause_wf")
        .store(&store)
        .create()
        .await?;

    let run_msg = pgqrs::workflow()
        .name("pause_wf")
        .store(&store)
        .trigger(&TestData {
            msg: "pause".to_string(),
        })?
        .execute()
        .await?;

    let mut run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;

    run.start().await?;
    run.pause("wait".to_string(), Duration::from_secs(30))
        .await?;

    let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Paused);

    run.start().await?;

    let record = pgqrs::tables(&store).workflow_runs().get(run.id()).await?;
    assert_eq!(record.status, pgqrs::WorkflowStatus::Running);

    Ok(())
}
