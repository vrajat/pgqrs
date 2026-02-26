use pgqrs::error::Result;
use pgqrs::pgqrs_workflow;
use pgqrs::store::Store;
use serde_json::json;
use serial_test::serial;
use tokio::time::{sleep, timeout, Duration};

mod common;

async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    common::create_store(schema).await
}

async fn wait_for_message(
    store: &pgqrs::store::AnyStore,
    msg_id: i64,
    max_wait: Duration,
    predicate: impl Fn(&pgqrs::types::QueueMessage) -> bool,
) -> pgqrs::types::QueueMessage {
    timeout(max_wait, async {
        loop {
            let msg = store.messages().get(msg_id).await.unwrap();
            if predicate(&msg) {
                return msg;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

async fn wait_for_archived_count(
    store: &pgqrs::store::AnyStore,
    queue_id: i64,
    expected: usize,
    max_wait: Duration,
) {
    timeout(max_wait, async {
        loop {
            let archived = store
                .messages()
                .list_archived_by_queue(queue_id)
                .await
                .unwrap();
            if archived.len() == expected {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_workflow_complete(
    store: &pgqrs::store::AnyStore,
    message_id: i64,
    max_wait: Duration,
) {
    timeout(max_wait, async {
        loop {
            let result = pgqrs::tables(store)
                .workflow_runs()
                .get_by_message_id(message_id)
                .await;

            match result {
                Ok(record) => {
                    if matches!(
                        record.status,
                        pgqrs::WorkflowStatus::Success | pgqrs::WorkflowStatus::Error
                    ) {
                        return;
                    }
                }
                Err(pgqrs::error::Error::NotFound { .. }) => {
                    // Run not created yet, continue waiting
                }
                Err(e) => {
                    panic!("Error getting workflow run: {:?}", e);
                }
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[serial]
async fn test_basic_queue_single_consumer_handler_poll() {
    // --8<-- [start:basic_queue_setup]
    let store = create_store("guide_basic_queue_single").await;

    let queue = "guide_basic_queue_single";
    pgqrs::admin(&store).create_queue(queue).await.unwrap();

    let producer = pgqrs::producer("guide-producer", 9001, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer = pgqrs::consumer("guide-consumer", 9101, queue)
        .create(&store)
        .await
        .unwrap();
    // --8<-- [end:basic_queue_setup]

    // --8<-- [start:basic_queue_worker_poll]
    // Assumes `store` and `consumer` already exist.
    let store_task = store.clone();
    let consumer_task_handle = consumer.clone();

    let consumer_task = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_task_handle)
            .batch(1)
            .handle(|_msg| Box::pin(async { Ok(()) }))
            .poll(&store_task)
            .await
    });
    // --8<-- [end:basic_queue_worker_poll]

    // --8<-- [start:basic_queue_enqueue_one]
    let payload = json!({"k": "v"});
    let ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    // --8<-- [end:basic_queue_enqueue_one]

    // --8<-- [start:basic_queue_assert_and_shutdown]
    let msg = wait_for_message(&store, ids[0], Duration::from_secs(5), |m| {
        m.archived_at.is_some() && m.consumer_worker_id.is_some()
    })
    .await;
    assert_eq!(msg.payload, payload);
    assert_eq!(msg.consumer_worker_id, Some(consumer.worker_id()));

    // --8<-- [start:basic_queue_interrupt_and_shutdown]
    consumer.interrupt().await.unwrap();

    let res = timeout(Duration::from_secs(15), consumer_task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res, Err(pgqrs::error::Error::Suspended { .. })));
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_interrupt_and_shutdown]
    // --8<-- [end:basic_queue_assert_and_shutdown]
}

#[tokio::test]
#[serial]
async fn test_basic_queue_two_consumers_poll_batch_handoff() {
    let store = create_store("guide_basic_queue_handoff").await;

    let queue = "guide_basic_queue_handoff";
    pgqrs::admin(&store).create_queue(queue).await.unwrap();

    let producer = pgqrs::producer("guide-producer", 9002, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer_a = pgqrs::consumer("guide-consumer-a", 9102, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer_b = pgqrs::consumer("guide-consumer-b", 9103, queue)
        .create(&store)
        .await
        .unwrap();

    // --8<-- [start:basic_queue_handoff_start_consumer_a]
    // Assumes `store` and `consumer_a` already exist.
    let store_a = store.clone();
    let consumer_a_task_handle = consumer_a.clone();
    let task_a = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_a_task_handle)
            .batch(5)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_a)
            .await
    });
    // --8<-- [end:basic_queue_handoff_start_consumer_a]

    let payload1 = json!({"n": 1});
    let ids1 = pgqrs::enqueue()
        .message(&payload1)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg1 = wait_for_message(&store, ids1[0], Duration::from_secs(5), |m| {
        m.archived_at.is_some() && m.consumer_worker_id == Some(consumer_a.worker_id())
    })
    .await;
    assert_eq!(msg1.payload, payload1);

    // --8<-- [start:basic_queue_handoff_interrupt_consumer_a]
    // Assumes `consumer_a` and `task_a` exist.
    consumer_a.interrupt().await.unwrap();
    let res_a = timeout(Duration::from_secs(5), task_a)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res_a, Err(pgqrs::error::Error::Suspended { .. })));
    assert_eq!(
        consumer_a.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_handoff_interrupt_consumer_a]

    // --8<-- [start:basic_queue_handoff_start_consumer_b]
    // Assumes `store` and `consumer_b` already exist.
    let store_b = store.clone();
    let consumer_b_task_handle = consumer_b.clone();
    let task_b = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_b_task_handle)
            .batch(5)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_b)
            .await
    });
    // --8<-- [end:basic_queue_handoff_start_consumer_b]

    let payload2 = json!({"n": 2});
    let ids2 = pgqrs::enqueue()
        .message(&payload2)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg2 = wait_for_message(&store, ids2[0], Duration::from_secs(5), |m| {
        m.archived_at.is_some() && m.consumer_worker_id == Some(consumer_b.worker_id())
    })
    .await;
    assert_eq!(msg2.payload, payload2);

    // --8<-- [start:basic_queue_handoff_interrupt_consumer_b]
    // Assumes `consumer_b` and `task_b` exist.
    consumer_b.interrupt().await.unwrap();
    let res_b = timeout(Duration::from_secs(5), task_b)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res_b, Err(pgqrs::error::Error::Suspended { .. })));
    assert_eq!(
        consumer_b.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_handoff_interrupt_consumer_b]
}

#[tokio::test]
#[serial]
async fn test_basic_queue_two_consumers_continuous_handler_poll_interrupt() {
    let store = create_store("guide_basic_queue_continuous").await;

    let queue = "guide_basic_queue_continuous";
    pgqrs::admin(&store).create_queue(queue).await.unwrap();

    let queue_id = store.queues().get_by_name(queue).await.unwrap().id;

    let producer = pgqrs::producer("guide-producer", 9003, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer_a = pgqrs::consumer("guide-consumer-a", 9104, queue)
        .create(&store)
        .await
        .unwrap();
    let consumer_b = pgqrs::consumer("guide-consumer-b", 9105, queue)
        .create(&store)
        .await
        .unwrap();

    // --8<-- [start:basic_queue_continuous_start_two_consumers]
    // Assumes `store`, `consumer_a`, and `consumer_b` already exist.
    let store_a = store.clone();
    let consumer_a_task_handle = consumer_a.clone();
    let task_a = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_a_task_handle)
            .batch(10)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_a)
            .await
    });

    let store_b = store.clone();
    let consumer_b_task_handle = consumer_b.clone();
    let task_b = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_b_task_handle)
            .batch(10)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_b)
            .await
    });
    // --8<-- [end:basic_queue_continuous_start_two_consumers]

    let payloads: Vec<serde_json::Value> = (0..40).map(|i| json!({"i": i})).collect();

    // Keep this small: we just want to demonstrate multi-consumer polling.
    let ids_a = pgqrs::enqueue()
        .messages(&payloads[..20])
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let ids_b = pgqrs::enqueue()
        .messages(&payloads[20..])
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    assert_eq!(ids_a.len() + ids_b.len(), 40);

    wait_for_archived_count(&store, queue_id, 40, Duration::from_secs(10)).await;

    // --8<-- [start:basic_queue_continuous_interrupt_two_consumers]
    // Assumes `consumer_a`, `consumer_b`, `task_a`, and `task_b` exist.
    consumer_a.interrupt().await.unwrap();
    consumer_b.interrupt().await.unwrap();

    let res_a = timeout(Duration::from_secs(5), task_a)
        .await
        .unwrap()
        .unwrap();
    let res_b = timeout(Duration::from_secs(5), task_b)
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(res_a, Err(pgqrs::error::Error::Suspended { .. })));
    assert!(matches!(res_b, Err(pgqrs::error::Error::Suspended { .. })));

    assert_eq!(
        consumer_a.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    assert_eq!(
        consumer_b.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_continuous_interrupt_two_consumers]
}
// --8<-- [end:basic_queue_two_consumers_continuous]

// ============================================================================
// Workflow Guide Tests - snippets for basic-workflow.md & durable-workflows.md
// ============================================================================

use pgqrs::Run;

/// Test: Basic workflow end-to-end using the new macro-driven API.
/// This test provides snippets for docs/user-guide/guides/basic-workflow.md
#[tokio::test]
#[serial]
async fn test_basic_workflow_ephemeral_trigger() {
    // --8<-- [start:basic_workflow_setup]
    let store = create_store("guide_basic_workflow").await;

    // Create workflow definition using macro (idempotent)
    // --8<-- [start:basic_workflow_define]
    #[pgqrs_workflow(name = "process_task")]
    async fn process_task_wf(
        ctx: &Run,
        _input: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        // Use workflow_step for durability - this caches results across crashes
        // --8<-- [start:basic_workflow_step_call]
        let result: serde_json::Value = pgqrs::workflow_step(ctx, "process_item", || async {
            // Simulate processing
            Ok(serde_json::json!({
                "processed": "task_data",
                "status": "done"
            }))
        })
        .await?;
        // --8<-- [end:basic_workflow_step_call]
        Ok(result)
    }
    // --8<-- [end:basic_workflow_define]

    pgqrs::workflow()
        .name(process_task_wf)
        .create(&store)
        .await
        .unwrap();
    // --8<-- [end:basic_workflow_setup]

    // --8<-- [start:basic_workflow_trigger_ephemeral]
    // Trigger workflow (ephemeral producer - no explicit producer needed)
    let input = serde_json::json!({"id": 1, "payload": "Hello pgqrs"});
    let msg = pgqrs::workflow()
        .name(process_task_wf)
        .trigger(&input)
        .unwrap()
        .execute(&store)
        .await
        .unwrap();
    // --8<-- [end:basic_workflow_trigger_ephemeral]

    // Verify message was enqueued
    assert!(msg.id > 0);
    assert_eq!(msg.payload.get("input").unwrap(), &input);

    // --8<-- [start:basic_workflow_consumer_start]
    // Create a consumer to process the workflow
    let consumer = pgqrs::consumer("workflow-worker", 9501, "process_task")
        .create(&store)
        .await
        .unwrap();

    // Spawn the consumer polling task using official workflow API
    let store_for_poll = store.clone();
    let consumer_handle = consumer.clone();
    let consumer_task = tokio::spawn(async move {
        pgqrs::workflow()
            .name(process_task_wf)
            .consumer(&consumer_handle)
            .poll(&store_for_poll)
            .await
    });
    // --8<-- [end:basic_workflow_consumer_start]

    // --8<-- [start:basic_workflow_get_result]
    // Get the workflow result using run().result() - handles polling automatically
    let result: serde_json::Value = pgqrs::run()
        .message(msg.clone())
        .store(&store)
        .result()
        .await
        .unwrap();

    println!("Workflow Result: {:?}", result);
    // --8<-- [end:basic_workflow_get_result]

    // Verify the result
    assert_eq!(result.get("processed").unwrap(), "task_data");

    // Cleanup: interrupt consumer
    consumer.interrupt().await.unwrap();
    let _ = timeout(Duration::from_secs(5), consumer_task).await;
}

/// Test: Crash recovery - workflow resumes from cached step results after worker crash.
/// This test provides snippets for docs/user-guide/guides/durable-workflows.md
#[tokio::test]
#[serial]
async fn test_durable_workflow_crash_recovery() {
    let store = create_store("guide_durable_workflow").await;

    static CRASH_RECOVERY_HAS_CRASHED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    // Define a workflow with steps - first step completes, then crashes
    // --8<-- [start:durable_workflow_crash_define]

    #[pgqrs_workflow(name = "crash_recovery_wf")]
    async fn crash_recovery_wf(run: &Run, _input: serde_json::Value) -> Result<serde_json::Value> {
        // Step 1: This will complete and be cached
        pgqrs::workflow_step(run, "step1", || async {
            Ok(serde_json::json!({"data": "from step 1"}))
        })
        .await?;

        if !CRASH_RECOVERY_HAS_CRASHED.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(pgqrs::Error::TestCrash);
        }

        // Step 2
        pgqrs::workflow_step(run, "step2", || async {
            Ok::<_, pgqrs::Error>("step2_done".to_string())
        })
        .await?;

        Ok(json!({"done": true}))
    }
    // --8<-- [end:durable_workflow_crash_define]

    // Create workflow definition
    pgqrs::workflow()
        .name(crash_recovery_wf)
        .create(&store)
        .await
        .unwrap();

    // Create consumer
    let consumer = pgqrs::consumer("crash-worker", 9601, "crash_recovery_wf")
        .create(&store)
        .await
        .unwrap();

    // --8<-- [start:durable_workflow_crash_first_run]
    // Start consumer with workflow().poll() - this will process until crash
    let store_for_consumer = store.clone();
    let consumer_handle = consumer.clone();
    let _consumer_task = tokio::spawn(async move {
        pgqrs::workflow()
            .name(crash_recovery_wf)
            .consumer(&consumer_handle)
            .poll(&store_for_consumer)
            .await
    });
    // --8<-- [end:durable_workflow_crash_first_run]

    // --8<-- [start:durable_workflow_crash_trigger]
    // Trigger workflow - consumer will pick it up and crash after step1
    let input = true;
    let msg = pgqrs::workflow()
        .name(crash_recovery_wf)
        .trigger(&input)
        .unwrap()
        .execute(&store)
        .await
        .unwrap();
    // --8<-- [end:durable_workflow_crash_trigger]

    // Wait for step 1 to be created in the database
    timeout(Duration::from_secs(5), async {
        loop {
            let steps = store.workflow_steps().list().await.unwrap();
            if steps.iter().any(|s| s.step_name == "step1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // Record step 1 timestamps before crash
    let steps_before = store.workflow_steps().list().await.unwrap();
    let step_before = steps_before.iter().find(|s| s.step_name == "step1");
    let step1_created_at_before = step_before.map(|s| s.created_at);
    let step1_updated_at_before = step_before.map(|s| s.updated_at);

    // --8<-- [start:durable_workflow_crash_simulate]
    // Wait for consumer to process and crash (TestCrash)
    // Then interrupt the consumer to simulate real crash
    timeout(Duration::from_secs(10), async {
        loop {
            let steps = store.workflow_steps().list().await.unwrap();
            let step = steps.iter().find(|s| s.step_name == "step1");
            if let Some(s) = step {
                eprintln!("Step 1 status: {:?}", s.status);
                if s.status == pgqrs::WorkflowStatus::Success {
                    consumer.interrupt().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    break;
                }
            } else {
                eprintln!("Step 1 not found yet");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap();

    // Now the consumer is interrupted, release the message
    pgqrs::admin(&store)
        .release_worker_messages(consumer.worker_id())
        .await
        .unwrap();
    // --8<-- [end:durable_workflow_crash_simulate]

    // Wait for message to be available again and process it
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --8<-- [start:durable_workflow_crash_recovery]
    // Create new consumer to resume and complete the workflow
    let consumer2 = pgqrs::consumer("crash-worker-2", 9602, "crash_recovery_wf")
        .create(&store)
        .await
        .unwrap();

    let store_for_consumer2 = store.clone();
    let consumer2_handle = consumer2.clone();
    let _consumer_task2 = tokio::spawn(async move {
        pgqrs::workflow()
            .name(crash_recovery_wf)
            .consumer(&consumer2_handle)
            .poll(&store_for_consumer2)
            .await
    });

    // Wait for workflow to complete
    wait_for_workflow_complete(&store, msg.id, Duration::from_secs(10)).await;
    // --8<-- [end:durable_workflow_crash_recovery]

    // Verify step 1 timestamps are unchanged (step was cached, not re-executed)
    let steps_after = store.workflow_steps().list().await.unwrap();
    let step_after = steps_after.iter().find(|s| s.step_name == "step1");

    assert!(step_after.is_some(), "Step 1 should exist");
    let step = step_after.unwrap();

    // Step should be completed
    assert_eq!(step.status, pgqrs::WorkflowStatus::Success);

    // Timestamps should be unchanged (step was cached, not re-executed)
    assert_eq!(step.created_at, step1_created_at_before.unwrap());
    assert_eq!(step.updated_at, step1_updated_at_before.unwrap());

    // Verify workflow completed successfully
    let record = pgqrs::tables(&store)
        .workflow_runs()
        .get_by_message_id(msg.id)
        .await
        .unwrap();
    assert_eq!(record.status, pgqrs::WorkflowStatus::Success);
}

/// Test: Transient errors - automatic retry with backoff.
/// This test provides snippets for docs/user-guide/guides/durable-workflows.md
#[tokio::test]
#[serial]
async fn test_durable_workflow_transient_error() {
    let store = create_store("guide_crash_recovery").await;

    // --8<-- [start:durable_workflow_transient_define]
    #[pgqrs_workflow(name = "transient_error_wf")]
    async fn transient_error_wf(run: &Run, _input: serde_json::Value) -> Result<serde_json::Value> {
        // This step will artificially fail with a transient error
        pgqrs::workflow_step(run, "api_call", || async {
            if true {
                return Err(pgqrs::Error::Transient {
                    code: "TIMEOUT".to_string(),
                    message: "Connection timed out".to_string(),
                    retry_after: Some(std::time::Duration::from_secs(30)),
                });
            }
            Ok::<_, pgqrs::Error>(())
        })
        .await?;

        Ok(json!({"done": true}))
    }
    // --8<-- [end:durable_workflow_transient_define]

    // Create workflow definition
    pgqrs::workflow()
        .name(transient_error_wf)
        .create(&store)
        .await
        .unwrap();

    // Create consumer
    let consumer = pgqrs::consumer("transient-worker", 9603, "transient_error_wf")
        .create(&store)
        .await
        .unwrap();

    // Trigger workflow
    let input = true;
    let msg = pgqrs::workflow()
        .name(transient_error_wf)
        .trigger(&input)
        .unwrap()
        .execute(&store)
        .await
        .unwrap();

    // --8<-- [start:durable_workflow_transient_run]
    // Start consumer with workflow().poll() - it will hit the transient error
    let store_for_consumer = store.clone();
    let consumer_handle = consumer.clone();
    let consumer_task = tokio::spawn(async move {
        pgqrs::workflow()
            .name(transient_error_wf)
            .consumer(&consumer_handle)
            .poll(&store_for_consumer)
            .await
    });
    // --8<-- [end:durable_workflow_transient_run]

    // Wait for step to be recorded with ERROR
    timeout(Duration::from_secs(15), async {
        loop {
            let steps = store.workflow_steps().list().await.unwrap();
            let step = steps.iter().find(|s| s.step_name == "api_call");
            if let Some(s) = step {
                if s.status == pgqrs::WorkflowStatus::Error {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();

    // Interrupt the consumer so we can inspect
    consumer.interrupt().await.unwrap();
    let _ = timeout(Duration::from_secs(5), consumer_task).await;

    // --8<-- [start:durable_workflow_transient_inspect]
    // Verify that the run is still RUNNING (not ERROR) because the error was transient
    let workflow = store
        .workflows()
        .get_by_name("transient_error_wf")
        .await
        .unwrap();
    let runs = pgqrs::tables(&store).workflow_runs().list().await.unwrap();
    let run_rec = runs
        .into_iter()
        .find(|r| r.workflow_id == workflow.id)
        .unwrap();
    assert_eq!(run_rec.status, pgqrs::WorkflowStatus::Running);

    // Verify that the step is ERROR, recorded as transient, and has a retry_at scheduled
    let steps = store.workflow_steps().list().await.unwrap();
    let step_rec = steps.into_iter().find(|s| s.run_id == run_rec.id).unwrap();

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Error);
    assert!(
        step_rec.retry_at.is_some(),
        "Step should be scheduled for retry"
    );
    // --8<-- [end:durable_workflow_transient_inspect]

    // Message shouldn't be archived yet
    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(msg.queue_id)
        .await
        .unwrap();
    assert_eq!(archived.len(), 0);
}

/// Test: Pausing workflows.
/// This test provides snippets for docs/user-guide/guides/durable-workflows.md
#[tokio::test]
#[serial]
async fn test_durable_workflow_pause() {
    let store = create_store("guide_pause").await;

    // --8<-- [start:durable_workflow_pause_define]
    #[pgqrs_workflow(name = "pause_wf")]
    async fn pause_wf(run: &Run, _input: serde_json::Value) -> Result<serde_json::Value> {
        // Step 1: Pause execution
        pgqrs::workflow_step(run, "step1", || async {
            if true {
                return Err(pgqrs::Error::Paused {
                    message: "Waiting for approval".to_string(),
                    resume_after: std::time::Duration::from_secs(60),
                });
            }
            Ok::<_, pgqrs::Error>(())
        })
        .await?;

        Ok(json!({"done": true}))
    }
    // --8<-- [end:durable_workflow_pause_define]

    // Create workflow definition
    pgqrs::workflow()
        .name(pause_wf)
        .create(&store)
        .await
        .unwrap();

    // Create consumer
    let consumer = pgqrs::consumer("pause-worker", 9604, "pause_wf")
        .create(&store)
        .await
        .unwrap();

    // Trigger workflow
    let input = true;
    let _msg = pgqrs::workflow()
        .name(pause_wf)
        .trigger(&input)
        .unwrap()
        .execute(&store)
        .await
        .unwrap();

    // --8<-- [start:durable_workflow_pause_run]
    // Start consumer with workflow().poll() - it will hit the paused error
    let store_for_consumer = store.clone();
    let consumer_handle = consumer.clone();
    let consumer_task = tokio::spawn(async move {
        pgqrs::workflow()
            .name(pause_wf)
            .consumer(&consumer_handle)
            .poll(&store_for_consumer)
            .await
    });
    // --8<-- [end:durable_workflow_pause_run]

    // Wait for step to be recorded with ERROR
    timeout(Duration::from_secs(15), async {
        loop {
            let steps = store.workflow_steps().list().await.unwrap();
            let step = steps.iter().find(|s| s.step_name == "step1");
            if let Some(s) = step {
                if s.status == pgqrs::WorkflowStatus::Error {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();

    consumer.interrupt().await.unwrap();
    let _ = timeout(Duration::from_secs(5), consumer_task).await;

    // --8<-- [start:durable_workflow_pause_inspect]
    // Verify that the run is PAUSED waiting for external event
    let workflow = store.workflows().get_by_name("pause_wf").await.unwrap();
    let runs = pgqrs::tables(&store).workflow_runs().list().await.unwrap();
    let run_rec = runs
        .into_iter()
        .find(|r| r.workflow_id == workflow.id)
        .unwrap();
    assert_eq!(run_rec.status, pgqrs::WorkflowStatus::Paused);

    // Verify that the step is ERROR, code PAUSED, and has a retry_at scheduled
    let steps = store.workflow_steps().list().await.unwrap();
    let step_rec = steps.into_iter().find(|s| s.run_id == run_rec.id).unwrap();

    assert_eq!(step_rec.status, pgqrs::WorkflowStatus::Error);
    assert!(
        step_rec.retry_at.is_some(),
        "Step should have resume_after scheduled"
    );
    // --8<-- [end:durable_workflow_pause_inspect]
}
