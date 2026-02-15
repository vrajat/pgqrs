use chrono::Utc;
use pgqrs::error::Error;
use pgqrs::store::AnyStore;
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestData {
    msg: String,
}

async fn create_store(schema: &str) -> AnyStore {
    common::create_store(schema).await
}

/// Test that a step with transient error returns StepNotReady
#[tokio::test]
async fn test_step_returns_not_ready_on_transient_error() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_integration_tests").await;

    // Create definition
    pgqrs::workflow()
        .name("not_ready_test_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name("not_ready_test_wf")
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

    // Step 1: Fail with transient error
    let step_name = "transient_step";
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);

    // Fail with transient error
    let error = serde_json::json!({
        "is_transient": true,
        "code": "NETWORK_TIMEOUT",
        "message": "Connection timeout",
    });
    workflow.fail_step(step_name, error, Utc::now()).await?;

    // Try to acquire again - should return StepNotReady immediately
    let step_res = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await;

    // Should get StepNotReady error
    match step_res {
        Err(Error::StepNotReady {
            retry_at,
            retry_count,
        }) => {
            assert!(retry_at > Utc::now(), "retry_at should be in future");
            assert_eq!(retry_count, 1, "First retry should be scheduled");
        }
        Ok(_) => panic!("Expected StepNotReady, got Ok"),
        Err(e) => panic!("Expected StepNotReady, got: {:?}", e),
    }

    Ok(())
}

/// Test that step becomes ready after retry_at timestamp passes
#[tokio::test]
async fn test_step_ready_after_retry_at() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_integration_tests").await;

    // Create definition
    pgqrs::workflow()
        .name("becomes_ready_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name("becomes_ready_wf")
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

    let step_name = "delayed_step";

    // Fail with transient error
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await?;
    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);

    let error = serde_json::json!({
        "is_transient": true,
        "code": "TIMEOUT",
        "message": "Initial failure",
    });
    workflow.fail_step(step_name, error, Utc::now()).await?;

    // Get the retry_at timestamp
    let step_res = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await;

    let retry_at = match step_res {
        Err(Error::StepNotReady { retry_at, .. }) => retry_at,
        _ => panic!("Expected StepNotReady"),
    };

    // Simulate time advancing past retry_at by using with_time()
    let simulated_time = retry_at + chrono::Duration::milliseconds(100);

    // Now should be able to acquire for execution
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .with_time(simulated_time)
        .execute()
        .await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);

    // This time succeed
    let output = TestData {
        msg: "success_after_retry".to_string(),
    };
    workflow
        .complete_step(step_name, serde_json::to_value(&output)?)
        .await?;

    // Verify step now succeeds on next acquire
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Success);
    let data: TestData = serde_json::from_value(step_rec.output.unwrap())?;
    assert_eq!(data.msg, "success_after_retry");

    Ok(())
}

/// Test that a step exhausts retries and fails permanently
#[tokio::test]
async fn test_step_exhausts_retries() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_integration_tests").await;

    // Create definition
    pgqrs::workflow()
        .name("exhaust_retry_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name("exhaust_retry_wf")
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

    let step_name = "exhausting_step";

    // Initial execution (attempt 0)
    let _ = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await?;

    let error = serde_json::json!({
        "is_transient": true,
        "code": "TIMEOUT",
        "message": "Initial failure",
    });
    workflow.fail_step(step_name, error, Utc::now()).await?;

    // Retry 1
    {
        let step_res = pgqrs::step()
            .run(&*workflow)
            .name(step_name)
            .execute()
            .await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 1"),
        };

        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let _ = pgqrs::step()
            .run(&*workflow)
            .name(step_name)
            .with_time(simulated_time)
            .execute()
            .await?;

        let error = serde_json::json!({
            "is_transient": true,
            "code": "TIMEOUT",
            "message": "Retry 1",
        });
        workflow.fail_step(step_name, error, simulated_time).await?;
    }

    // Retry 2
    {
        let step_res = pgqrs::step()
            .run(&*workflow)
            .name(step_name)
            .execute()
            .await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 2"),
        };

        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let _ = pgqrs::step()
            .run(&*workflow)
            .name(step_name)
            .with_time(simulated_time)
            .execute()
            .await?;

        let error = serde_json::json!({
            "is_transient": true,
            "code": "TIMEOUT",
            "message": "Retry 2",
        });
        workflow.fail_step(step_name, error, simulated_time).await?;
    }

    // Retry 3
    {
        let step_res = pgqrs::step()
            .run(&*workflow)
            .name(step_name)
            .execute()
            .await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 3"),
        };

        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let _ = pgqrs::step()
            .run(&*workflow)
            .name(step_name)
            .with_time(simulated_time)
            .execute()
            .await?;

        let error = serde_json::json!({
            "is_transient": true,
            "code": "TIMEOUT",
            "message": "Retry 3",
        });
        workflow.fail_step(step_name, error, simulated_time).await?;
    }

    // Now should fail with RetriesExhausted
    let step_res = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(attempts, 3);
        }
        _ => panic!("Expected RetriesExhausted"),
    }

    Ok(())
}

/// Test that non-transient errors fail immediately without retry
#[tokio::test]
async fn test_non_transient_error_no_retry() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_integration_tests").await;

    pgqrs::workflow()
        .name("non_transient_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name("non_transient_wf")
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

    let step_name = "non_transient_step";

    let _ = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await?;

    let error = serde_json::json!({
        "is_transient": false,
        "code": "VALIDATION_ERROR",
        "message": "Invalid input",
    });
    workflow.fail_step(step_name, error, Utc::now()).await?;

    // Should immediately fail with RetriesExhausted
    let step_res = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(attempts, 0);
        }
        _ => panic!("Expected RetriesExhausted"),
    }

    Ok(())
}

#[tokio::test]
async fn test_workflow_stays_running_during_retry() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_integration_tests").await;

    pgqrs::workflow()
        .name("running_state_wf")
        .store(&store)
        .create()
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name("running_state_wf")
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

    let step_name = "running_step";

    let _ = pgqrs::step()
        .run(&*workflow)
        .name(step_name)
        .execute()
        .await?;

    let error = serde_json::json!({
        "is_transient": true,
        "code": "TIMEOUT",
        "message": "Timeout",
    });
    workflow.fail_step(step_name, error, Utc::now()).await?;

    // Workflow should still be in RUNNING state
    let other_step_name = "other_step";
    let step_rec = pgqrs::step()
        .run(&*workflow)
        .name(other_step_name)
        .execute()
        .await?;

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_step_retries() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_integration_tests").await;

    let mut handles = vec![];

    for i in 0..3 {
        let store = store.clone();

        let handle = tokio::spawn(async move {
            let input = TestData {
                msg: format!("test_{}", i),
            };
            let name = format!("concurrent_wf_{}", i);
            pgqrs::workflow()
                .name(&name)
                .store(&store)
                .create()
                .await
                .unwrap();

            let run_msg = pgqrs::workflow()
                .name(&name)
                .store(&store)
                .trigger(&input)
                .unwrap()
                .execute()
                .await
                .unwrap();

            let mut workflow = pgqrs::run()
                .message(run_msg)
                .store(&store)
                .execute()
                .await
                .unwrap();
            workflow.start().await.unwrap();

            let step_name = "concurrent_step";

            let _ = pgqrs::step()
                .run(&*workflow)
                .name(step_name)
                .execute()
                .await
                .unwrap();

            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": format!("Timeout {}", i),
            });
            workflow
                .fail_step(step_name, error, Utc::now())
                .await
                .unwrap();

            let retry_at = match pgqrs::step()
                .run(&*workflow)
                .name(step_name)
                .execute()
                .await
            {
                Err(Error::StepNotReady { retry_at, .. }) => retry_at,
                _ => panic!("Expected StepNotReady"),
            };

            let simulated_time = retry_at + chrono::Duration::milliseconds(100);

            let step_rec = pgqrs::step()
                .run(&*workflow)
                .name(step_name)
                .with_time(simulated_time)
                .execute()
                .await
                .unwrap();

            assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Running);
            workflow
                .complete_step(step_name, serde_json::json!({"msg": format!("done_{}", i)}))
                .await
                .unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}
