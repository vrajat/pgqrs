use chrono::Utc;
use pgqrs::error::Error;
use pgqrs::store::AnyStore;
use pgqrs::{StepGuardExt, StepResult};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

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
    let store = create_store("workflow_retry_not_ready").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("not_ready_test_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    // Step 1: Fail with transient error
    let step_id = "transient_step";
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            // Fail with transient error
            let error = serde_json::json!({
                "is_transient": true,
                "code": "NETWORK_TIMEOUT",
                "message": "Connection timeout",
            });
            guard.fail(&error).await?;
        }
        StepResult::Skipped(_) => panic!("Step should execute first time"),
    }

    // Try to acquire again - should return StepNotReady immediately
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
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
    let store = create_store("workflow_retry_becomes_ready").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("becomes_ready_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "delayed_step";

    // Fail with transient error
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Initial failure",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on initial attempt"),
    }

    // Get the retry_at timestamp
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    let retry_at = match step_res {
        Err(Error::StepNotReady { retry_at, .. }) => retry_at,
        _ => panic!("Expected StepNotReady"),
    };

    // Simulate time advancing past retry_at by using with_time()
    let simulated_time = retry_at + chrono::Duration::milliseconds(100);

    // Now should be able to acquire for execution
    let step_res = pgqrs::step(workflow_id, step_id)
        .with_time(simulated_time)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            // This time succeed
            let output = TestData {
                msg: "success_after_retry".to_string(),
            };
            guard.success(&serde_json::to_value(&output)?).await?;
        }
        StepResult::Skipped(_) => panic!("Step should execute after retry_at"),
    }

    // Verify step now succeeds on next acquire
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Skipped(data) => {
            assert_eq!(data.msg, "success_after_retry");
        }
        StepResult::Execute(_) => panic!("Step should be skipped after success"),
    }

    Ok(())
}

/// Test that a step exhausts retries and fails permanently
#[tokio::test]
async fn test_step_exhausts_retries() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_exhaust").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("exhaust_retry_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "exhausting_step";

    // Default policy: max_attempts = 3
    // Fail 4 times to exhaust retries

    // Initial execution (attempt 0)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Initial failure",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on initial attempt"),
    }

    // Retry 1
    {
        let step_res = pgqrs::step(workflow_id, step_id)
            .acquire::<TestData, _>(&store)
            .await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 1"),
        };

        // Simulate time passing beyond retry_at
        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let step_res = pgqrs::step(workflow_id, step_id)
            .with_time(simulated_time)
            .acquire::<TestData, _>(&store)
            .await?;
        match step_res {
            StepResult::Execute(mut guard) => {
                let error = serde_json::json!({
                    "is_transient": true,
                    "code": "TIMEOUT",
                    "message": "Retry 1",
                });
                guard.fail(&error).await?;
            }
            _ => panic!("Should execute at Retry 1"),
        }
    }

    // Retry 2
    {
        let step_res = pgqrs::step(workflow_id, step_id)
            .acquire::<TestData, _>(&store)
            .await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 2"),
        };

        // Simulate time passing beyond retry_at
        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let step_res = pgqrs::step(workflow_id, step_id)
            .with_time(simulated_time)
            .acquire::<TestData, _>(&store)
            .await?;
        match step_res {
            StepResult::Execute(mut guard) => {
                let error = serde_json::json!({
                    "is_transient": true,
                    "code": "TIMEOUT",
                    "message": "Retry 2",
                });
                guard.fail(&error).await?;
            }
            _ => panic!("Should execute at Retry 2"),
        }
    }

    // Retry 3
    {
        let step_res = pgqrs::step(workflow_id, step_id)
            .acquire::<TestData, _>(&store)
            .await;
        let retry_at = match step_res {
            Err(Error::StepNotReady { retry_at, .. }) => retry_at,
            _ => panic!("Expected StepNotReady at Retry 3"),
        };

        // Simulate time passing beyond retry_at
        let simulated_time = retry_at + chrono::Duration::milliseconds(100);
        let step_res = pgqrs::step(workflow_id, step_id)
            .with_time(simulated_time)
            .acquire::<TestData, _>(&store)
            .await?;
        match step_res {
            StepResult::Execute(mut guard) => {
                let error = serde_json::json!({
                    "is_transient": true,
                    "code": "TIMEOUT",
                    "message": "Retry 3",
                });
                guard.fail(&error).await?;
            }
            _ => panic!("Should execute at Retry 3"),
        }
    }

    // Now should fail with RetriesExhausted (retry_count = 3, should_retry(3) = false)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(
                attempts, 3,
                "Should have exhausted 3 retries, got attempts={}",
                attempts
            );
        }
        Ok(_) => panic!("Step should fail with RetriesExhausted after max retries"),
        Err(e) => panic!("Expected RetriesExhausted, got: {:?}", e),
    }

    Ok(())
}

/// Test that non-transient errors fail immediately without retry
#[tokio::test]
async fn test_non_transient_error_no_retry() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_non_transient").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("non_transient_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "non_transient_step";

    // Fail with non-transient error
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": false,
                "code": "VALIDATION_ERROR",
                "message": "Invalid input",
            });
            guard.fail(&error).await?;
        }
        StepResult::Skipped(_) => panic!("Step should execute first time"),
    }

    // Should immediately fail with RetriesExhausted (0 retries for non-transient)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, error }) => {
            assert_eq!(attempts, 0, "Non-transient errors should not retry");
            assert_eq!(
                error.get("is_transient").and_then(|v| v.as_bool()),
                Some(false)
            );
            assert_eq!(
                error.get("code").and_then(|v| v.as_str()),
                Some("VALIDATION_ERROR")
            );
        }
        Ok(_) => panic!("Step should fail immediately for non-transient error"),
        Err(e) => panic!("Expected RetriesExhausted, got: {:?}", e),
    }

    Ok(())
}

/// Test retry_at timestamp is scheduled correctly in the future
#[tokio::test]
async fn test_retry_at_scheduled_in_future() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_at_future").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("retry_at_future_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "future_step";

    // Record current time before failure
    let before_fail = Utc::now();

    // Fail with transient error
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Test timeout",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute"),
    }

    // Get StepNotReady error
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::StepNotReady {
            retry_at,
            retry_count,
        }) => {
            // retry_at should be in the future
            let now = Utc::now();
            assert!(
                retry_at > now,
                "retry_at ({}) should be after now ({})",
                retry_at,
                now
            );

            // retry_at should be approximately 1 second after failure (with jitter)
            // Default policy: base delay = 1s with ±25% jitter = 0.75-1.25s
            // Allow up to 2s to account for jitter + rounding
            let expected_max = before_fail + chrono::Duration::seconds(2);
            assert!(
                retry_at <= expected_max,
                "retry_at ({}) should be within 2s of failure (max: {})",
                retry_at,
                expected_max
            );

            assert_eq!(retry_count, 1, "First retry should have retry_count=1");
        }
        _ => panic!("Expected StepNotReady"),
    }

    Ok(())
}

/// Test that retry_count is persisted correctly
#[tokio::test]
async fn test_retry_count_persisted() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_count").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("retry_count_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "count_step";

    // Initial execution (retry_count = 0)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "RETRY_TEST",
                "message": "Attempt 0",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute initially"),
    }

    // Check retry_count = 1
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::StepNotReady { retry_count, .. }) => {
            assert_eq!(
                retry_count, 1,
                "Expected retry_count=1, got {}",
                retry_count
            );
        }
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(attempts, 1, "Expected attempts=1, got {}", attempts);
        }
        _ => panic!("Expected error with retry_count=1"),
    }

    Ok(())
}

/// Test custom retry_after delay from error
#[tokio::test]
async fn test_custom_retry_after_delay() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_custom_delay").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("custom_delay_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "custom_delay_step";

    let before_fail = Utc::now();

    // Fail with custom retry_after (e.g., from Retry-After header)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "RATE_LIMITED",
                "message": "Rate limit exceeded",
                "retry_after": 2,  // Custom 2 second delay
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute"),
    }

    // Should get StepNotReady with retry_at ~2 seconds in future
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::StepNotReady { retry_at, .. }) => {
            // Custom retry_after = 2 seconds
            // Allow up to 3s to account for rounding/precision
            let expected_max = before_fail + chrono::Duration::seconds(3);
            assert!(
                retry_at <= expected_max,
                "retry_at ({}) should be within 3s of failure (custom 2s delay, max: {})",
                retry_at,
                expected_max
            );
        }
        _ => panic!("Expected StepNotReady with custom delay"),
    }

    Ok(())
}

/// Test that workflow stays RUNNING during step retry
#[tokio::test]
async fn test_workflow_stays_running_during_retry() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_running_state").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("running_state_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "running_step";

    // Fail step with transient error
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Timeout",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute"),
    }

    // Workflow should still be in RUNNING state (not ERROR)
    // Verify by executing another step - should work if workflow is RUNNING

    let other_step_id = "other_step";
    let step_res = pgqrs::step(workflow_id, other_step_id)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            guard
                .success(&serde_json::json!({"msg": "other_step_done"}))
                .await?;
        }
        _ => panic!("Other step should execute if workflow is RUNNING"),
    }

    Ok(())
}

/// Test error without is_transient field gets wrapped as non-transient
#[tokio::test]
async fn test_error_wrapping_non_transient() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_wrapping").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("wrapping_test_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "wrap_step";

    // Fail with error that doesn't have is_transient field
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;

    match step_res {
        StepResult::Execute(mut guard) => {
            // Error without is_transient field
            let error = serde_json::json!({
                "error": "Something went wrong",
                "details": "No is_transient field",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute"),
    }

    // Should fail immediately (wrapped as non-transient)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, error }) => {
            assert_eq!(
                attempts, 0,
                "Wrapped errors should be non-transient (0 retries)"
            );
            // Check that it was wrapped
            assert_eq!(
                error.get("is_transient").and_then(|v| v.as_bool()),
                Some(false)
            );
            assert_eq!(
                error.get("code").and_then(|v| v.as_str()),
                Some("NON_RETRYABLE")
            );
        }
        _ => panic!("Should fail with RetriesExhausted for wrapped error"),
    }

    Ok(())
}

/// Test concurrent step retries don't interfere
#[tokio::test]
async fn test_concurrent_step_retries() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_concurrent").await;

    // Create multiple workflows
    let workflows = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];

    for i in 0..3 {
        let store = store.clone();
        let workflows = workflows.clone();

        let handle = tokio::spawn(async move {
            let input = TestData {
                msg: format!("test_{}", i),
            };
            let mut workflow = pgqrs::workflow()
                .name(&format!("concurrent_wf_{}", i))
                .arg(&input)
                .unwrap()
                .create(&store)
                .await
                .unwrap();
            workflow.start().await.unwrap();
            let workflow_id = workflow.id();

            workflows.lock().unwrap().push(workflow_id);

            let step_id = "concurrent_step";

            // Fail with transient error
            let step_res = pgqrs::step(workflow_id, step_id)
                .acquire::<TestData, _>(&store)
                .await
                .unwrap();
            match step_res {
                StepResult::Execute(mut guard) => {
                    let error = serde_json::json!({
                        "is_transient": true,
                        "code": "TIMEOUT",
                        "message": format!("Timeout {}", i),
                    });
                    guard.fail(&error).await.unwrap();
                }
                _ => panic!("Should execute"),
            }

            // Get retry_at and simulate time advancing
            let retry_at = match pgqrs::step(workflow_id, step_id)
                .acquire::<TestData, _>(&store)
                .await
            {
                Err(Error::StepNotReady { retry_at, .. }) => retry_at,
                _ => panic!("Expected StepNotReady"),
            };

            // Simulate time passing beyond retry_at
            let simulated_time = retry_at + chrono::Duration::milliseconds(100);

            // Retry and succeed
            let step_res = pgqrs::step(workflow_id, step_id)
                .with_time(simulated_time)
                .acquire::<TestData, _>(&store)
                .await
                .unwrap();
            match step_res {
                StepResult::Execute(mut guard) => {
                    guard
                        .success(&serde_json::json!({"msg": format!("done_{}", i)}))
                        .await
                        .unwrap();
                }
                _ => panic!("Should execute on retry"),
            }
        });

        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await?;
    }

    // Verify all workflows completed successfully
    let workflow_ids = workflows.lock().unwrap();
    assert_eq!(workflow_ids.len(), 3, "All workflows should complete");

    Ok(())
}
