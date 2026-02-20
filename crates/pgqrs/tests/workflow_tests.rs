use pgqrs::store::AnyStore;
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
    let store = create_store("workflow_get_tests").await;

    pgqrs::workflow()
        .name("get_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "get_test".to_string(),
    };

    let run_msg = pgqrs::workflow()
        .name("get_wf")
        .store(&store)
        .trigger(&input)?
        .execute()
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
    let store = create_store("workflow_polling_tests").await;

    pgqrs::workflow()
        .name("polling_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "poll_me".to_string(),
    };

    let run_msg = pgqrs::workflow()
        .name("polling_wf")
        .store(&store)
        .trigger(&input)?
        .execute()
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
    let store = create_store("workflow_error_polling_tests").await;

    pgqrs::workflow()
        .name("error_wf")
        .store(&store)
        .create()
        .await?;

    let run_msg = pgqrs::workflow()
        .name("error_wf")
        .store(&store)
        .trigger(&TestData {
            msg: "fail".to_string(),
        })?
        .execute()
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

#[tokio::test]
async fn test_workflow_message_fk_integrity() -> anyhow::Result<()> {
    let store = create_store("workflow_fk_tests").await;

    pgqrs::workflow()
        .name("fk_wf")
        .store(&store)
        .create()
        .await?;

    let run_msg = pgqrs::workflow()
        .name("fk_wf")
        .store(&store)
        .trigger(&TestData {
            msg: "fk_test".to_string(),
        })?
        .execute()
        .await?;

    // Create the run record
    let _run = pgqrs::run()
        .message(run_msg.clone())
        .store(&store)
        .execute()
        .await?;

    // Attempt to delete the message. This should fail due to foreign key constraint.
    let res = pgqrs::tables(&store).messages().delete(run_msg.id).await;

    assert!(
        res.is_err(),
        "Deleting a message referenced by a run should fail with FK violation"
    );
    let err_msg = res.unwrap_err().to_string().to_lowercase();
    assert!(
        err_msg.contains("foreign key constraint failed")
            || err_msg.contains("violates foreign key constraint"),
        "Error should mention foreign key violation, got: {}",
        err_msg
    );

    Ok(())
}
