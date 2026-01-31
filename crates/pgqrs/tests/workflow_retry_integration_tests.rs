use pgqrs::error::Error;
use pgqrs::store::AnyStore;
use pgqrs::{StepGuardExt, StepResult};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod common;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestData {
    msg: String,
}

async fn create_store(schema: &str) -> AnyStore {
    common::create_store(schema).await
}

/// Test that a step with transient error retries automatically
#[tokio::test]
async fn test_step_retries_on_transient_error() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_transient").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("retry_test_wf")
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

    // Give async operations time to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Step should retry (with backoff)
    let start = Instant::now();
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;

    // Verify backoff delay occurred (default: 1s for first retry)
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900), // Allow some tolerance
        "Expected backoff delay of ~1s, got {:?}",
        elapsed
    );

    // Should get Execute again (retry)
    match step_res {
        StepResult::Execute(mut guard) => {
            // This time succeed
            let output = TestData {
                msg: "success_after_retry".to_string(),
            };
            guard.success(&serde_json::to_value(&output)?).await?;
        }
        StepResult::Skipped(_) => panic!("Step should retry, not skip"),
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

    // Default policy: max_attempts = 3, exponential backoff
    // Sequence:
    // 1. Initial execution (fails) → retry_count = 0, should_retry(0)=true
    // 2. Backoff ~1s, retry (fails) → retry_count = 1, should_retry(1)=true
    // 3. Backoff ~2s, retry (fails) → retry_count = 2, should_retry(2)=true
    // 4. Backoff ~4s, retry (fails) → retry_count = 3, should_retry(3)=false → RetriesExhausted

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
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retry 1 (after ~1s backoff)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Retry 1 failure",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on retry 1"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retry 2 (after ~2s backoff)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Retry 2 failure",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on retry 2"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retry 3 (after ~4s backoff)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Retry 3 failure",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on retry 3"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now should fail with RetriesExhausted (retry_count = 3, should_retry(3) = false)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            // attempts is retry_count = 3
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

    // Give async operations time
    tokio::time::sleep(Duration::from_millis(100)).await;

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

/// Test exponential backoff timing
#[tokio::test]
async fn test_exponential_backoff_timing() -> anyhow::Result<()> {
    let store = create_store("workflow_retry_backoff").await;

    // Create workflow
    let input = TestData {
        msg: "test".to_string(),
    };
    let mut workflow = pgqrs::workflow()
        .name("backoff_test_wf")
        .arg(&input)?
        .create(&store)
        .await?;
    workflow.start().await?;
    let workflow_id = workflow.id();

    let step_id = "backoff_step";

    // Attempt 1: Initial execution
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Timeout 1",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Attempt 2: First retry (should wait ~1s with jitter)
    let start = Instant::now();
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    let elapsed1 = start.elapsed();

    // Default policy: base=1s, exponential with jitter
    // First retry (attempt 0): ~1s ±25% jitter = 0.75s - 1.25s
    assert!(
        elapsed1 >= Duration::from_millis(700) && elapsed1 <= Duration::from_millis(1500),
        "First retry should wait ~1s (±jitter), got {:?}",
        elapsed1
    );

    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "TIMEOUT",
                "message": "Timeout 2",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Attempt 3: Second retry (should wait ~2s with jitter)
    let start = Instant::now();
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    let elapsed2 = start.elapsed();

    // Second retry (attempt 1): ~2s ±25% jitter = 1.5s - 2.5s
    assert!(
        elapsed2 >= Duration::from_millis(1400) && elapsed2 <= Duration::from_millis(3000),
        "Second retry should wait ~2s (±jitter), got {:?}",
        elapsed2
    );

    match step_res {
        StepResult::Execute(mut guard) => {
            // Succeed this time
            guard.success(&serde_json::json!({"msg": "done"})).await?;
        }
        _ => panic!("Should execute"),
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

    // Fail 4 times total (initial + 3 retries) to exhaust max_attempts = 3
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
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retry 1
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "RETRY_TEST",
                "message": "Attempt 1",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on retry 1"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retry 2
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "RETRY_TEST",
                "message": "Attempt 2",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on retry 2"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Retry 3 (retry_count = 3 after this)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    match step_res {
        StepResult::Execute(mut guard) => {
            let error = serde_json::json!({
                "is_transient": true,
                "code": "RETRY_TEST",
                "message": "Attempt 3",
            });
            guard.fail(&error).await?;
        }
        _ => panic!("Should execute on retry 3"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should now exhaust retries (retry_count = 3, should_retry(3) = false)
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await;

    match step_res {
        Err(Error::RetriesExhausted { attempts, .. }) => {
            assert_eq!(
                attempts, 3,
                "retry_count should be persisted and reach 3, got attempts={}",
                attempts
            );
        }
        _ => panic!("Should fail with RetriesExhausted showing correct attempt count"),
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
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should respect custom retry_after delay
    let start = Instant::now();
    let step_res = pgqrs::step(workflow_id, step_id)
        .acquire::<TestData, _>(&store)
        .await?;
    let elapsed = start.elapsed();

    // Should wait for custom delay (2 seconds)
    assert!(
        elapsed >= Duration::from_millis(1900) && elapsed <= Duration::from_millis(2500),
        "Should respect custom retry_after=2s, got {:?}",
        elapsed
    );

    match step_res {
        StepResult::Execute(mut guard) => {
            guard.success(&serde_json::json!({"msg": "done"})).await?;
        }
        _ => panic!("Should execute after custom delay"),
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
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Workflow should still be in RUNNING state (not ERROR)
    // We can verify this by attempting to complete it (should succeed)
    // or by checking that other steps can still execute

    // Try to execute another step - should work if workflow is RUNNING
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
    tokio::time::sleep(Duration::from_millis(100)).await;

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

            tokio::time::sleep(Duration::from_millis(100)).await;

            // Retry and succeed
            let step_res = pgqrs::step(workflow_id, step_id)
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
