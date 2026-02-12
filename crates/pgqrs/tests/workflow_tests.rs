use pgqrs::store::AnyStore;
use pgqrs::{RunExt, StepGuardExt, StepResult};
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
}

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_workflow_test").await
}

#[tokio::test]
async fn test_workflow_lifecycle() -> anyhow::Result<()> {
    let store = create_store().await;

    // Start workflow
    let input = TestData {
        msg: "start".to_string(),
    };
    // Use create to get valid ID
    let mut workflow = pgqrs::workflow()
        .name("test_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    let workflow_id = workflow.id();

    workflow.start().await?;

    // Step 1: Run
    let step1_id = "step1";
    // step_id is String in macro, but &str here. acquire takes &str.
    let step_res = pgqrs::step(workflow_id, step1_id)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            let output = TestData {
                msg: "step1_done".to_string(),
            };
            let val = serde_json::to_value(&output)?;
            guard.success(&val).await?;
        }
        StepResult::Skipped(_) => panic!("Step 1 should execute first time"),
    }

    // Step 1: Rerun (should skip)
    let step_res = pgqrs::step(workflow_id, step1_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Skipped(val) => {
            assert_eq!(val.msg, "step1_done");
        }
        StepResult::Execute(_) => panic!("Step 1 should skip on rerun"),
    }

    // Step 2: Drop (Panic simulation)
    let step2_id = "step2";
    let step_res = pgqrs::step(workflow_id, step2_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(guard) => {
            // Explicitly drop without calling success/fail
            drop(guard);
        }
        StepResult::Skipped(_) => panic!("Step 2 should execute"),
    }

    // Allow async drop to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Step 2: Rerun (should be ERROR state because of drop)
    let step_res = pgqrs::step(workflow_id, step2_id)
        .acquire::<TestData, _>(&store)
        .await;
    assert!(
        step_res.is_err(),
        "Step 2 should be in terminal ERROR state after drop"
    );

    // Finish Workflow
    workflow
        .complete(serde_json::to_value(&TestData {
            msg: "done".to_string(),
        })?)
        .await?;

    // Restart Workflow (should adhere to SUCCESS terminal state)
    workflow.start().await?;

    // Verify Workflow Failure Logic using new workflow
    let input_fail = TestData {
        msg: "fail".to_string(),
    };
    let mut wf_fail = pgqrs::workflow()
        .name("fail_wf")
        .arg(&input_fail)?
        .create(&store)
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
