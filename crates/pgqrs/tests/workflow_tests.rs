use pgqrs::store::AnyStore;
use pgqrs::RunExt;
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
    let workflow_id = workflow.id();

    workflow.start().await?;

    // Step 1: Run
    let step1_id = "step1";
    let step_rec = pgqrs::step().run(&*workflow).id(step1_id).execute().await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);

    let output = TestData {
        msg: "step1_done".to_string(),
    };
    let val = serde_json::to_value(&output)?;
    workflow.complete_step(step1_id, val).await?;

    // Step 1: Rerun (should skip)
    let step_rec = pgqrs::step().run(&*workflow).id(step1_id).execute().await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Success);
    let val: TestData = serde_json::from_value(step_rec.output.unwrap())?;
    assert_eq!(val.msg, "step1_done");

    // Step 2: Acquire
    let step2_id = "step2";
    let _step_rec = pgqrs::step().run(&*workflow).id(step2_id).execute().await?;

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
