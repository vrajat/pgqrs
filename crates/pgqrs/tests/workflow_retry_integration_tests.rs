use chrono::Utc;
use pgqrs::error::Error;
use pgqrs::pgqrs_workflow;
use pgqrs::store::AnyStore;
use pgqrs::store::Store;
use pgqrs::Run;
use serde::{Deserialize, Serialize};

mod common;

#[pgqrs_workflow(name = "not_ready_test_wf")]
async fn not_ready_test_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "exhaust_retry_wf")]
async fn exhaust_retry_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "non_transient_wf")]
async fn non_transient_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "concurrent_step_retries_wf")]
async fn concurrent_step_retries_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestData {
    msg: String,
}

async fn create_store() -> AnyStore {
    common::create_store("workflow_retry_integration_tests").await
}

async fn steps_for_run(store: &AnyStore, run_id: i64) -> anyhow::Result<Vec<pgqrs::StepRecord>> {
    Ok(store
        .workflow_steps()
        .list()
        .await?
        .into_iter()
        .filter(|step| step.run_id == run_id)
        .collect())
}

/// Test that a step with transient error returns StepNotReady
#[tokio::test]
async fn test_step_returns_not_ready_on_transient_error() -> anyhow::Result<()> {
    let store = create_store().await;

    // Create definition
    pgqrs::workflow()
        .name(not_ready_test_wf)
        .create(&store)
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name(not_ready_test_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let mut run = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;
    run = run.start().await?;

    // Step 1: Fail with transient error
    let step_name = "transient_step";
    let step_rec = pgqrs::step().run(&run).name(step_name).execute().await?;

    assert_eq!(step_rec.status(), pgqrs::WorkflowStatus::Running);

    let steps = steps_for_run(&store, run.id()).await?;
    assert_eq!(steps.len(), 1);
    let step = steps
        .iter()
        .find(|step| step.step_name == step_name)
        .expect("transient step should be persisted");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Running);
    assert_eq!(step.retry_count, 0);
    assert!(step.error.is_none());
    assert!(step.retry_at.is_none());

    // Fail with transient error
    let error = serde_json::json!({
        "is_transient": true,
        "code": "NETWORK_TIMEOUT",
        "message": "Connection timeout",
    });
    run.fail_step(step_name, error, Utc::now()).await?;

    let steps = steps_for_run(&store, run.id()).await?;
    let step = steps
        .iter()
        .find(|step| step.step_name == step_name)
        .expect("transient step should still be persisted");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Error);
    assert_eq!(step.retry_count, 1);
    assert!(step.error.is_some());
    assert!(step.retry_at.is_some());

    // Try to acquire again - should return StepNotReady immediately
    let step_res = pgqrs::step().run(&run).name(step_name).execute().await;

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

    // Workflow should still be in RUNNING state
    let other_step_name = "other_step";
    let step_rec = pgqrs::step()
        .run(&run)
        .name(other_step_name)
        .execute()
        .await?;

    assert_eq!(step_rec.status(), pgqrs::WorkflowStatus::Running);

    // Get the retry_at timestamp
    let step_res = pgqrs::step().run(&run).name(step_name).execute().await;

    let retry_at = match step_res {
        Err(Error::StepNotReady { retry_at, .. }) => retry_at,
        _ => panic!("Expected StepNotReady"),
    };

    // Simulate time advancing past retry_at by using with_time()
    let simulated_time = retry_at + chrono::Duration::milliseconds(100);

    // Now should be able to acquire for execution
    let step_rec = pgqrs::step()
        .run(&run)
        .name(step_name)
        .with_time(simulated_time)
        .execute()
        .await?;

    assert_eq!(step_rec.status(), pgqrs::WorkflowStatus::Running);

    let steps = steps_for_run(&store, run.id()).await?;
    let step = steps
        .iter()
        .find(|step| step.step_name == step_name)
        .expect("transient step should be runnable again");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Running);
    assert_eq!(step.retry_count, 1);
    assert!(step.error.is_none());
    assert!(step.retry_at.is_none());

    // This time succeed
    let output = TestData {
        msg: "success_after_retry".to_string(),
    };
    run.complete_step(step_name, serde_json::to_value(&output)?)
        .await?;

    // Verify step now succeeds on next acquire
    let step_rec = pgqrs::step().run(&run).name(step_name).execute().await?;

    assert_eq!(step_rec.status(), pgqrs::WorkflowStatus::Success);
    let data: TestData = serde_json::from_value(step_rec.output().unwrap().clone())?;
    assert_eq!(data.msg, "success_after_retry");

    let steps = steps_for_run(&store, run.id()).await?;
    let step = steps
        .iter()
        .find(|step| step.step_name == step_name)
        .expect("transient step should be completed");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Success);
    assert_eq!(step.retry_count, 1);
    assert!(step.retry_at.is_none());
    assert_eq!(step.output, Some(serde_json::to_value(&output)?));

    Ok(())
}

/// Test that a step exhausts retries and fails permanently
#[tokio::test]
async fn test_step_exhausts_retries() -> anyhow::Result<()> {
    let store = create_store().await;

    // Create definition
    pgqrs::workflow()
        .name(exhaust_retry_wf)
        .create(&store)
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name(exhaust_retry_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let mut workflow = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;
    workflow = workflow.start().await?;

    let step_name = "exhausting_step";

    // Initial execution (attempt 0)
    let _ = pgqrs::step()
        .run(&workflow)
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
        let step_res = pgqrs::step().run(&workflow).name(step_name).execute().await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 1"),
        };

        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let _ = pgqrs::step()
            .run(&workflow)
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
        let step_res = pgqrs::step().run(&workflow).name(step_name).execute().await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 2"),
        };

        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let _ = pgqrs::step()
            .run(&workflow)
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
        let step_res = pgqrs::step().run(&workflow).name(step_name).execute().await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 3"),
        };

        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let _ = pgqrs::step()
            .run(&workflow)
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
    let step_res = pgqrs::step().run(&workflow).name(step_name).execute().await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(attempts, 3);
        }
        _ => panic!("Expected RetriesExhausted"),
    }

    let steps = steps_for_run(&store, workflow.id()).await?;
    let step = steps
        .iter()
        .find(|step| step.step_name == step_name)
        .expect("exhausting step should be persisted");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Error);
    assert_eq!(step.retry_count, 3);
    assert!(step.retry_at.is_none());
    assert!(step.error.is_some());

    Ok(())
}

/// Test that non-transient errors fail immediately without retry
#[tokio::test]
async fn test_non_transient_error_no_retry() -> anyhow::Result<()> {
    let store = create_store().await;

    pgqrs::workflow()
        .name(non_transient_wf)
        .create(&store)
        .await?;

    let input = TestData {
        msg: "test".to_string(),
    };
    let run_msg = pgqrs::workflow()
        .name(non_transient_wf)
        .trigger(&input)?
        .execute(&store)
        .await?;

    let mut workflow = pgqrs::run()
        .message(run_msg)
        .store(&store)
        .execute()
        .await?;
    workflow = workflow.start().await?;

    let step_name = "non_transient_step";

    let _ = pgqrs::step()
        .run(&workflow)
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
    let step_res = pgqrs::step().run(&workflow).name(step_name).execute().await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(attempts, 0);
        }
        _ => panic!("Expected RetriesExhausted"),
    }

    let steps = steps_for_run(&store, workflow.id()).await?;
    let step = steps
        .iter()
        .find(|step| step.step_name == step_name)
        .expect("non transient step should be persisted");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Error);
    assert_eq!(step.retry_count, 0);
    assert!(step.retry_at.is_none());
    assert!(step.error.is_some());

    Ok(())
}

#[tokio::test]
async fn test_concurrent_step_retries() -> anyhow::Result<()> {
    let store = create_store().await;

    // This test used to generate unique workflow names dynamically.
    // With the new API, workflows are definition-driven (macro-generated WorkflowDef),
    // so we create a single workflow definition and run multiple runs concurrently.
    pgqrs::workflow()
        .name(concurrent_step_retries_wf)
        .create(&store)
        .await?;

    let mut handles = vec![];

    for i in 0..3 {
        let store = store.clone();

        let handle = tokio::spawn(async move {
            let input = TestData {
                msg: format!("test_{}", i),
            };

            let run_msg = pgqrs::workflow()
                .name(concurrent_step_retries_wf)
                .trigger(&input)
                .unwrap()
                .execute(&store)
                .await
                .unwrap();

            let mut workflow = pgqrs::run()
                .message(run_msg)
                .store(&store)
                .execute()
                .await
                .unwrap();
            workflow = workflow.start().await.unwrap();

            let step_name = "concurrent_step";

            let _ = pgqrs::step()
                .run(&workflow)
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

            let retry_at = match pgqrs::step().run(&workflow).name(step_name).execute().await {
                Err(Error::StepNotReady { retry_at, .. }) => retry_at,
                _ => panic!("Expected StepNotReady"),
            };

            let simulated_time = retry_at + chrono::Duration::milliseconds(100);

            let step_rec = pgqrs::step()
                .run(&workflow)
                .name(step_name)
                .with_time(simulated_time)
                .execute()
                .await
                .unwrap();

            assert_eq!(step_rec.status(), pgqrs::WorkflowStatus::Running);
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

    let steps = store.workflow_steps().list().await?;
    assert_eq!(steps.len(), 3);
    for step in steps {
        assert_eq!(step.step_name, "concurrent_step");
        assert_eq!(step.status, pgqrs::WorkflowStatus::Success);
        assert_eq!(step.retry_count, 1);
        assert!(step.retry_at.is_none());
        assert!(step.output.is_some());
    }

    Ok(())
}
