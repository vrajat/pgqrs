use pgqrs::error::Result;
use pgqrs::pgqrs_workflow;
use pgqrs::store::AnyStore;
use pgqrs::Run;
use pgqrs::Store;
use serde_json::json;
use serial_test::serial;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

mod common;

#[pgqrs_workflow(name = "scenario_success")]
async fn scenario_success_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_permanent_error")]
async fn scenario_permanent_error_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_crash_recovery")]
async fn scenario_crash_recovery_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_step_crash_recovery")]
async fn scenario_step_crash_recovery_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_transient_error")]
async fn scenario_transient_error_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_pause")]
async fn scenario_pause_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_cancel_boundary")]
async fn scenario_cancel_boundary_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_cancel_before_materialize")]
async fn scenario_cancel_before_materialize_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_cancel_before_start")]
async fn scenario_cancel_before_start_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_cancel_before_first_step")]
async fn scenario_cancel_before_first_step_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

#[pgqrs_workflow(name = "scenario_cancel_redelivery")]
async fn scenario_cancel_redelivery_wf(
    _run: &Run,
    input: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(input)
}

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_concurrent_test").await
}

async fn create_workflow_test_rig(
    workflow: pgqrs::WorkflowDef<serde_json::Value, serde_json::Value>,
    consumer_name: &str,
    payload: serde_json::Value,
) -> anyhow::Result<(pgqrs::WorkflowTestRig<AnyStore>, pgqrs::QueueMessage)> {
    let store = create_store().await;
    pgqrs::workflow()
        .name(workflow.clone())
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer(consumer_name, workflow.name())
        .create(&store)
        .await?;
    let rig = pgqrs::WorkflowTestRig::new(store.clone(), consumer);

    let message = pgqrs::workflow()
        .name(workflow)
        .trigger(&payload)?
        .execute(&store)
        .await?;

    Ok((rig, message))
}

async fn assert_run_status(
    store: &AnyStore,
    run_id: i64,
    expected: pgqrs::WorkflowStatus,
) -> anyhow::Result<()> {
    let run = pgqrs::tables(store).workflow_runs().get(run_id).await?;
    assert_eq!(run.status, expected);
    Ok(())
}

async fn assert_message_archived(
    store: &AnyStore,
    message: &pgqrs::QueueMessage,
) -> anyhow::Result<()> {
    let archived = pgqrs::tables(store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].id, message.id);
    Ok(())
}

async fn steps_for_run(store: &AnyStore, run_id: i64) -> anyhow::Result<Vec<pgqrs::StepRecord>> {
    Ok(store
        .workflow_steps()
        .list()
        .await?
        .into_iter()
        .filter(|entry| entry.run_id == run_id)
        .collect())
}

async fn dispatch_attempt_message<F, Fut>(
    consumer: &pgqrs::Consumer,
    attempt: &pgqrs::WorkflowAttempt,
    handler: F,
) -> anyhow::Result<()>
where
    F: Fn(pgqrs::QueueMessage) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = pgqrs::Result<()>> + Send,
{
    match handler(attempt.message.clone()).await {
        Ok(()) => {
            attempt.archive(consumer).await?;
            Ok(())
        }
        Err(err) => {
            attempt.release(consumer).await?;
            Err(err.into())
        }
    }
}

async fn app_step_success(step_state: &Arc<Mutex<Option<String>>>) -> Result<String> {
    let mut state = step_state.lock().expect("state lock poisoned");
    if state.is_none() {
        *state = Some("step_success".to_string());
    }
    Ok(state.clone().unwrap())
}

async fn app_step_permanent_error() -> Result<String> {
    Err(pgqrs::Error::Internal {
        message: "File not found".to_string(),
    })
}

async fn app_workflow_success(
    run: Run,
    step_state: Arc<Mutex<Option<String>>>,
) -> Result<serde_json::Value> {
    let _ = pgqrs::workflow_step(&run, "step1", || {
        let step_state = step_state.clone();
        async move { app_step_success(&step_state).await }
    })
    .await?;

    Ok(json!({"done": true}))
}

async fn app_workflow_permanent_error(run: Run) -> Result<serde_json::Value> {
    let result = pgqrs::workflow_step(&run, "download", || async {
        app_step_permanent_error().await
    })
    .await;

    match result {
        Ok(_) => Ok(json!({"done": true})),
        Err(err) => Err(err),
    }
}

async fn app_workflow_crash_recovery(
    run: Run,
    should_crash: bool,
    step1_calls: Arc<Mutex<u32>>,
) -> Result<serde_json::Value> {
    // Step 1
    pgqrs::workflow_step(&run, "step1", || {
        let step1_calls = step1_calls.clone();
        async move {
            let mut calls = step1_calls.lock().expect("step1_calls lock poisoned");
            *calls += 1;
            Ok::<_, pgqrs::Error>("step1_done".to_string())
        }
    })
    .await?;

    if should_crash {
        return Err(pgqrs::Error::TestCrash);
    }

    // Step 2
    pgqrs::workflow_step(&run, "step2", || async {
        Ok::<_, pgqrs::Error>("step2_done".to_string())
    })
    .await?;

    Ok(json!({"done": true}))
}

async fn app_workflow_transient_error(run: Run) -> Result<serde_json::Value> {
    let _: () = pgqrs::workflow_step(&run, "api_call", || async {
        Err(pgqrs::Error::Transient {
            code: "TIMEOUT".to_string(),
            message: "Connection timed out".to_string(),
            retry_after: Some(std::time::Duration::from_secs(30)),
        })
    })
    .await?;

    Ok(json!({"done": true}))
}

async fn app_workflow_step_crash(
    run: Run,
    step1_calls: Arc<Mutex<u32>>,
) -> Result<serde_json::Value> {
    let result: Result<()> = pgqrs::workflow_step(&run, "step1", || {
        let step1_calls = step1_calls.clone();
        async move {
            let mut calls = step1_calls.lock().expect("step1_calls lock poisoned");
            *calls += 1;
            Err(pgqrs::Error::TestCrash)
        }
    })
    .await;

    match result {
        Err(pgqrs::Error::TestCrash) => Err(pgqrs::Error::TestCrash),
        Err(err) => Err(err),
        Ok(_) => Err(pgqrs::Error::Internal {
            message: "Expected TestCrash".to_string(),
        }),
    }
}

async fn app_workflow_pause(run: Run, should_pause: bool) -> Result<serde_json::Value> {
    pgqrs::workflow_step(&run, "step1", || async {
        if should_pause {
            return Err(pgqrs::Error::Paused {
                message: "Waiting for approval".to_string(),
                resume_after: std::time::Duration::from_secs(60),
            });
        }
        Ok::<_, pgqrs::Error>("step1_done".to_string())
    })
    .await?;

    Ok(json!({"done": true}))
}

async fn app_workflow_cancelled_by_other_actor(
    run: Run,
    step1_started: Arc<Notify>,
    allow_step1_finish: Arc<Notify>,
    step2_calls: Arc<Mutex<u32>>,
) -> Result<serde_json::Value> {
    let _: String = pgqrs::workflow_step(&run, "step1", || {
        let step1_started = step1_started.clone();
        let allow_step1_finish = allow_step1_finish.clone();
        async move {
            step1_started.notify_one();
            allow_step1_finish.notified().await;
            Ok::<_, pgqrs::Error>("step1_done".to_string())
        }
    })
    .await?;

    let _: String = pgqrs::workflow_step(&run, "step2", || {
        let step2_calls = step2_calls.clone();
        async move {
            let mut calls = step2_calls.lock().expect("step2_calls lock poisoned");
            *calls += 1;
            Ok::<_, pgqrs::Error>("step2_done".to_string())
        }
    })
    .await?;

    Ok(json!({"done": true}))
}

async fn app_workflow_cancel_replay(
    _run: Run,
    execution_calls: Arc<Mutex<u32>>,
) -> Result<serde_json::Value> {
    let mut calls = execution_calls
        .lock()
        .expect("execution_calls lock poisoned");
    *calls += 1;
    Ok(json!({"done": true}))
}

async fn app_workflow_cancel_before_first_step(
    run: Run,
    step1_calls: Arc<Mutex<u32>>,
) -> Result<serde_json::Value> {
    let _: String = pgqrs::workflow_step(&run, "step1", || {
        let step1_calls = step1_calls.clone();
        async move {
            let mut calls = step1_calls.lock().expect("step1_calls lock poisoned");
            *calls += 1;
            Ok::<_, pgqrs::Error>("step1_done".to_string())
        }
    })
    .await?;

    Ok(json!({"done": true}))
}

#[tokio::test]
#[serial]
async fn test_zombie_consumer_race_condition() {
    let store = create_store().await;

    let queue_name = "race_condition_queue";
    let _queue_info = store
        .queue(queue_name)
        .await
        .expect("Failed to create queue");

    // 2. Setup Producer and Consumer A & B
    let producer = pgqrs::producer("producer_host-1000", queue_name)
        .create(&store)
        .await
        .expect("Failed to register producer");

    let consumer_a = pgqrs::consumer("consumer_a-2000", queue_name)
        .create(&store)
        .await
        .expect("Failed to register consumer A");

    let consumer_b = pgqrs::consumer("consumer_b-2001", queue_name)
        .create(&store)
        .await
        .expect("Failed to register consumer B");

    // 3. Enqueue Message
    let payload = json!({"task": "slow_process"});
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .expect("Enqueue failed");
    let msg_id = msg_ids[0];
    println!("Enqueued message ID: {}", msg_id);

    // 4. Consumer A dequeues with SHORT visibility (e.g., 1 second)
    // We use dequeue_many_with_delay to set explicit short timeout
    let msgs_a = consumer_a
        .dequeue_many_with_delay(1, 1)
        .await
        .expect("Dequeue A failed");
    assert_eq!(msgs_a.len(), 1);
    let msg_a = &msgs_a[0];
    assert_eq!(msg_a.id, msg_id);
    println!("Consumer A dequeued message. Holding lock for 1s...");

    // 5. Simulate Consumer A losing the lease (e.g. system reclamation or crash recovery)
    // We explicitly release the messages from A so B can pick them up.
    // In a real system, a "reaper" process would do this for expired messages.
    println!("Simulating lease reclamation for Consumer A...");
    let released = pgqrs::admin(&store)
        .release_worker_messages(consumer_a.worker_id())
        .await
        .expect("Release failed");
    assert_eq!(released, 1, "Should have released 1 message");

    // 6. Consumer B dequeues the SAME message (stealing the lock)
    let msgs_b = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let msgs = consumer_b.dequeue().await.expect("Dequeue B failed");
            if !msgs.is_empty() {
                break msgs;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Consumer B should be able to pick up released message within deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    assert_eq!(
        msgs_b.len(),
        1,
        "Consumer B should be able to pick up expired message"
    );
    let msg_b = &msgs_b[0];
    assert_eq!(msg_b.id, msg_id);
    println!("Consumer B dequeued message (stole lock).");

    // 7. Consumer A tries to DELETE -> Should FAIL (return false)
    let deleted_a = consumer_a.delete(msg_id).await.expect("Delete A op failed");
    assert!(
        !deleted_a,
        "Consumer A should NOT be able to delete message owned by B"
    );
    println!("Consumer A delete correctly failed.");

    // 8. Consumer A tries to ARCHIVE -> Should FAIL (return None)
    let archived_a = consumer_a
        .archive(msg_id)
        .await
        .expect("Archive A op failed");
    assert!(
        archived_a.is_none(),
        "Consumer A should NOT be able to archive message owned by B"
    );
    println!("Consumer A archive correctly failed.");

    // 9. Consumer B completes work and DELETES -> Should SUCCEED
    let deleted_b = consumer_b.delete(msg_id).await.expect("Delete B op failed");
    assert!(
        deleted_b,
        "Consumer B should be able to delete its own message"
    );
    println!("Consumer B delete succeeded.");
}

#[tokio::test]
#[serial]
#[cfg(any(feature = "sqlite", feature = "turso"))]
async fn test_single_process_producer_consumer_contention() {
    let store = create_store().await;
    let backend_name = store.backend_name();
    if backend_name != "sqlite" && backend_name != "turso" {
        eprintln!("Skipping test: requires sqlite or turso backend");
        return;
    }
    let config = store.config().clone();
    let queue_name = match backend_name {
        "sqlite" => "sqlite_serialized_lock_queue".to_string(),
        "turso" => format!("turso_serialized_lock_{}", uuid::Uuid::new_v4()),
        _ => unreachable!(),
    };

    let _queue = store
        .queue(&queue_name)
        .await
        .expect("Failed to create queue");

    let producer = store
        .producer(&queue_name, "serialized-prod-3101", &config)
        .await
        .expect("Failed to create producer");
    let consumer = store
        .consumer(&queue_name, "serialized-cons-3102")
        .await
        .expect("Failed to create consumer");

    const MESSAGE_COUNT: usize = 50;

    let producer_task = async {
        for i in 0..MESSAGE_COUNT {
            pgqrs::enqueue()
                .message(&json!({ "idx": i }))
                .worker(&producer)
                .execute(&store)
                .await
                .unwrap_or_else(|e| panic!("enqueue failed during contention test: {e}"));
            tokio::task::yield_now().await;
        }
    };

    let consumer_task = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut consumed = 0;

        while consumed < MESSAGE_COUNT {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after consuming {consumed} of {MESSAGE_COUNT} messages"
            );

            match pgqrs::dequeue()
                .worker(&consumer)
                .fetch_one(&store)
                .await
                .unwrap_or_else(|e| panic!("dequeue failed during contention test: {e}"))
            {
                Some(msg) => {
                    consumer
                        .delete(msg.id)
                        .await
                        .unwrap_or_else(|e| panic!("delete failed during contention test: {e}"));
                    consumed += 1;
                }
                None => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    };

    tokio::join!(producer_task, consumer_task);

    let queue = store
        .queues()
        .get_by_name(&queue_name)
        .await
        .expect("Failed to refetch queue");
    let pending = store
        .messages()
        .count_pending_for_queue(queue.id)
        .await
        .expect("Failed to count pending messages");
    assert_eq!(pending, 0);
}

#[tokio::test]
#[serial]
async fn test_zombie_consumer_batch_ops() {
    let store = create_store().await;

    let queue_name = "batch_race_queue";
    let _queue_info = store
        .queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("prod-1", queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer_a = pgqrs::consumer("con_a-2", queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer_b = pgqrs::consumer("con_b-3", queue_name)
        .create(&store)
        .await
        .unwrap();

    // Enqueue 2 messages
    let msg1_ids = pgqrs::enqueue()
        .message(&json!(1))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg1_id = msg1_ids[0];

    let msg2_ids = pgqrs::enqueue()
        .message(&json!(2))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg2_id = msg2_ids[0];

    // A dequeues both with short timeout
    // Use slightly futuristic time to ensure visibility
    let future_time = chrono::Utc::now() + chrono::Duration::seconds(1);
    let msgs_a = consumer_a.dequeue_at(2, 1, future_time).await.unwrap();
    assert_eq!(msgs_a.len(), 2);

    // Simulate reclamation of messages from A
    let released = pgqrs::admin(&store)
        .release_worker_messages(consumer_a.worker_id())
        .await
        .unwrap();
    assert_eq!(released, 2);

    // B dequeues both
    let msgs_b = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let msgs = consumer_b.dequeue_many(2).await.unwrap();
            if msgs.len() == 2 {
                break msgs;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Consumer B should be able to pick up both released messages within deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    assert_eq!(msgs_b.len(), 2);

    // A tries delete_many -> Should return [false, false]
    let results_a = consumer_a
        .delete_many(vec![msg1_id, msg2_id])
        .await
        .unwrap();
    assert_eq!(
        results_a,
        vec![false, false],
        "Batch delete by A should fail for all"
    );

    // A tries archive_many -> return [false, false]
    let arch_results_a = consumer_a
        .archive_many(vec![msg1_id, msg2_id])
        .await
        .unwrap();
    assert_eq!(
        arch_results_a,
        vec![false, false],
        "Batch archive by A should fail for all"
    );

    // B deletes -> [true, true]
    let results_b = consumer_b
        .delete_many(vec![msg1_id, msg2_id])
        .await
        .unwrap();
    assert_eq!(
        results_b,
        vec![true, true],
        "Batch delete by B should succeed"
    );
}

#[tokio::test]
#[serial]
async fn test_concurrent_visibility_extension() {
    let store = create_store().await;

    let queue_name = "concurrent_vis_queue";
    let _queue_info = store.queue(queue_name).await.unwrap();

    let consumer_a = pgqrs::consumer("consumer_a-1001", queue_name)
        .create(&store)
        .await
        .unwrap();

    let consumer_b = pgqrs::consumer("consumer_b-1002", queue_name)
        .create(&store)
        .await
        .unwrap();

    let producer = pgqrs::producer("producer-2001", queue_name)
        .create(&store)
        .await
        .unwrap();

    // 3. Enqueue Message
    let msg_ids = pgqrs::enqueue()
        .message(&json!({"foo": "bar"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg_id = msg_ids[0];

    // 4. Consumer A dequeues
    let msgs_a = consumer_a.dequeue().await.unwrap();
    assert_eq!(msgs_a.len(), 1);
    assert_eq!(msgs_a[0].id, msg_id);

    // 5. Consumer B tries to extend visibility -> SHOULD FAIL
    let extended_by_b = consumer_b.extend_vt(msg_id, 10).await.unwrap();
    assert!(
        !extended_by_b,
        "Consumer B should not be able to extend visibility of message owned by A"
    );

    // 6. Consumer A tries to extend visibility -> SHOULD SUCCEED
    let extended_by_a = consumer_a.extend_vt(msg_id, 10).await.unwrap();
    assert!(
        extended_by_a,
        "Consumer A should be able to extend visibility of its own message"
    );
}

#[tokio::test]
#[serial]
async fn test_workflow_scenario_success() -> anyhow::Result<()> {
    let store = create_store().await;

    let workflow_name = "scenario_success";
    pgqrs::workflow()
        .name(scenario_success_wf)
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer("scenario_success_cons-3200", workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(scenario_success_wf)
        .trigger(&json!({"msg": "success"}))?
        .execute(&store)
        .await?;

    let step_state: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let handler_state = step_state.clone();

    let handler = pgqrs::workflow_handler(store.clone(), move |run, input: serde_json::Value| {
        let handler_state = handler_state.clone();
        async move {
            let _input = input;
            app_workflow_success(run, handler_state).await
        }
    });

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await?;

    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 1);

    let workflow = store.workflows().get_by_name(workflow_name).await?;
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Success);

    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Success);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_scenario_permanent_error() -> anyhow::Result<()> {
    let store = create_store().await;

    let workflow_name = "scenario_permanent_error";
    pgqrs::workflow()
        .name(scenario_permanent_error_wf)
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer("scenario_perm_err_cons-3210", workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(scenario_permanent_error_wf)
        .trigger(&json!({"msg": "perm_error"}))?
        .execute(&store)
        .await?;

    let handler = pgqrs::workflow_handler(
        store.clone(),
        move |run, input: serde_json::Value| async move {
            let _input = input;
            app_workflow_permanent_error(run).await
        },
    );
    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await?;

    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 1);

    let workflow = store.workflows().get_by_name(workflow_name).await?;
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Error);
    let error_val = run.error.clone().expect("run error missing");
    let error_msg = match error_val {
        serde_json::Value::String(value) => value,
        value => value
            .get("message")
            .and_then(|entry| entry.as_str())
            .unwrap_or_default()
            .to_string(),
    };
    assert!(error_msg.contains("File not found"));

    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Error);
    let step_error = step.error.clone().expect("step error missing");
    let step_msg = match step_error {
        serde_json::Value::String(value) => value,
        value => value
            .get("message")
            .and_then(|entry| entry.as_str())
            .unwrap_or_default()
            .to_string(),
    };
    assert!(step_msg.contains("File not found"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_scenario_crash_recovery() -> anyhow::Result<()> {
    let store = create_store().await;

    let workflow_name = "scenario_crash_recovery";
    pgqrs::workflow()
        .name(scenario_crash_recovery_wf)
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer("scenario_crash_cons-3220", workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(scenario_crash_recovery_wf)
        .trigger(&json!({"msg": "crash_recovery"}))?
        .execute(&store)
        .await?;

    let step1_calls = Arc::new(Mutex::new(0u32));

    // 1. First attempt: "Crash" after step 1 using handler
    let handler = pgqrs::workflow_handler(store.clone(), {
        let step1_calls = step1_calls.clone();
        move |run, _input: serde_json::Value| {
            let step1_calls = step1_calls.clone();
            async move { app_workflow_crash_recovery(run, true, step1_calls).await }
        }
    });

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    let first_attempt = pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await;

    assert!(matches!(first_attempt, Err(pgqrs::Error::TestCrash)));

    // Verify state: step1 SUCCESS, run RUNNING, message NOT archived
    let workflow = store.workflows().get_by_name(workflow_name).await?;
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run_rec = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run record missing");
    assert_eq!(run_rec.status, pgqrs::WorkflowStatus::Running);

    let steps = store.workflow_steps().list().await?;
    let steps: Vec<_> = steps
        .into_iter()
        .filter(|entry| entry.run_id == run_rec.id)
        .collect();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "step1");
    assert_eq!(steps[0].status, pgqrs::WorkflowStatus::Success);

    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 0);

    // 2. Release message (simulate timeout or admin release)
    pgqrs::admin(&store)
        .release_worker_messages(consumer.worker_id())
        .await?;

    // 3. Second attempt: Recovery using workflow_handler
    let handler = pgqrs::workflow_handler(store.clone(), {
        let step1_calls = step1_calls.clone();
        move |run, _input: serde_json::Value| {
            let step1_calls = step1_calls.clone();
            async move { app_workflow_crash_recovery(run, false, step1_calls).await }
        }
    });

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await?;

    // 4. Final Verification
    // Message should be archived
    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 1);

    let workflow = store.workflows().get_by_name(workflow_name).await?;
    // Run should be SUCCESS
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Success);

    // Steps: step1 SUCCESS (cached), step2 SUCCESS
    let steps = store.workflow_steps().list().await?;
    let step_count = steps
        .into_iter()
        .filter(|entry| entry.run_id == run.id)
        .count();
    assert_eq!(step_count, 2);

    let step1_calls = step1_calls.lock().expect("step1_calls lock poisoned");
    assert_eq!(*step1_calls, 1, "step1 should only run once");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_step_crash_recovery() -> anyhow::Result<()> {
    let store = create_store().await;

    let workflow_name = "scenario_step_crash_recovery";
    pgqrs::workflow()
        .name(scenario_step_crash_recovery_wf)
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer("scenario_step_crash_cons-3250", workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(scenario_step_crash_recovery_wf)
        .trigger(&json!({"msg": "step_crash"}))?
        .execute(&store)
        .await?;

    let step1_calls = Arc::new(Mutex::new(0u32));

    let handler = pgqrs::workflow_handler(store.clone(), {
        let step1_calls = step1_calls.clone();
        move |run, _input: serde_json::Value| {
            let step1_calls = step1_calls.clone();
            async move { app_workflow_step_crash(run, step1_calls).await }
        }
    });

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    let first_attempt = pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await;

    assert!(matches!(first_attempt, Err(pgqrs::Error::TestCrash)));

    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 0);

    let workflow = store.workflows().get_by_name(workflow_name).await?;
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Running);

    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Running);

    pgqrs::admin(&store)
        .release_worker_messages(consumer.worker_id())
        .await?;

    let handler = pgqrs::workflow_handler(store.clone(), {
        let step1_calls = step1_calls.clone();
        move |run, _input: serde_json::Value| {
            let step1_calls = step1_calls.clone();
            async move { app_workflow_step_crash(run, step1_calls).await }
        }
    });

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    let second_attempt = pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await;

    assert!(matches!(second_attempt, Err(pgqrs::Error::TestCrash)));

    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 0);

    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Running);

    let step1_calls = step1_calls.lock().expect("step1_calls lock poisoned");
    assert_eq!(*step1_calls, 2, "step1 should run once per attempt");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_scenario_transient_error() -> anyhow::Result<()> {
    let store = create_store().await;

    let workflow_name = "scenario_transient_error";
    pgqrs::workflow()
        .name(scenario_transient_error_wf)
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer("scenario_transient_cons-3230", workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(scenario_transient_error_wf)
        .trigger(&json!({"msg": "transient_error"}))?
        .execute(&store)
        .await?;

    let handler = pgqrs::workflow_handler(
        store.clone(),
        move |run, _input: serde_json::Value| async move { app_workflow_transient_error(run).await },
    );

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    let result = pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await;

    // Expect Err(Transient)
    assert!(matches!(result, Err(pgqrs::Error::Transient { .. })));

    // Verify state:
    // 1. Message NOT archived
    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 0);

    let workflow = store.workflows().get_by_name(workflow_name).await?;
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Running);

    // 3. Step status ERROR with is_transient true and retry_at set
    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Error);

    let error_val = step.error.clone().expect("step error missing");
    assert_eq!(error_val.get("is_transient"), Some(&json!(true)));
    assert_eq!(error_val.get("code"), Some(&json!("TIMEOUT")));

    // 4. retry_at should be set
    assert!(step.retry_at.is_some());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_scenario_pause() -> anyhow::Result<()> {
    let store = create_store().await;

    let workflow_name = "scenario_pause";
    pgqrs::workflow()
        .name(scenario_pause_wf)
        .create(&store)
        .await?;

    let consumer = pgqrs::consumer("scenario_pause_cons-3240", workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(scenario_pause_wf)
        .trigger(&json!({"msg": "pause"}))?
        .execute(&store)
        .await?;

    // 1. First attempt: Pause in step 1
    let handler = pgqrs::workflow_handler(
        store.clone(),
        move |run, _input: serde_json::Value| async move { app_workflow_pause(run, true).await },
    );

    let handler = {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    };

    let result = pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler)
        .execute(&store)
        .await;

    // Expect Err(Paused)
    assert!(matches!(result, Err(pgqrs::Error::Paused { .. })));

    // Verify state:
    // 1. Message NOT archived
    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 0);

    let workflow = store.workflows().get_by_name(workflow_name).await?;
    // 2. Run status PAUSED
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Paused);
    let error_val = run.error.clone().expect("run error missing");
    let error_msg = match error_val {
        serde_json::Value::String(value) => value,
        value => value
            .get("message")
            .and_then(|entry| entry.as_str())
            .unwrap_or_default()
            .to_string(),
    };
    assert_eq!(error_msg, "Waiting for approval");

    // 3. Step status ERROR with code PAUSED (or is_transient false)
    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Error);

    let error_val = step.error.clone().expect("step error missing");
    assert_eq!(error_val.get("code"), Some(&json!("PAUSED")));
    assert_eq!(error_val.get("is_transient"), Some(&json!(true)));

    // 4. retry_at/resume info if present
    assert!(step.retry_at.is_some());

    // 5. Release message (simulate timeout or admin release)
    pgqrs::admin(&store)
        .release_worker_messages(consumer.worker_id())
        .await?;

    // 6. Second attempt: Try immediately - should return StepNotReady
    let handler_resume = pgqrs::workflow_handler(
        store.clone(),
        move |run, _input: serde_json::Value| async move { app_workflow_pause(run, false).await },
    );

    let result = pgqrs::dequeue()
        .worker(&consumer)
        .handle(handler_resume)
        .execute(&store)
        .await;

    assert!(
        matches!(result, Err(pgqrs::Error::StepNotReady { .. })),
        "Expected StepNotReady, got {:?}",
        result
    );

    // 7. Third attempt: Resume with advanced time and complete
    let resume_after = chrono::Duration::seconds(61); // pause was 60s
    let future_time = chrono::Utc::now() + resume_after;

    let handler_resume_with_time = pgqrs::workflow_handler_with_time(
        store.clone(),
        move |run, _input: serde_json::Value| async move { app_workflow_pause(run, false).await },
        future_time,
    );

    pgqrs::dequeue()
        .worker(&consumer)
        .at(future_time)
        .handle(handler_resume_with_time)
        .execute(&store)
        .await?;

    // 8. Final Verification
    // Message should be archived
    let archived = pgqrs::tables(&store)
        .messages()
        .list_archived_by_queue(message.queue_id)
        .await?;
    assert_eq!(archived.len(), 1);

    // Run should be SUCCESS
    let runs = pgqrs::tables(&store).workflow_runs().list().await?;
    let run = runs
        .into_iter()
        .find(|entry| entry.workflow_id == workflow.id)
        .expect("run not found");
    assert_eq!(run.status, pgqrs::WorkflowStatus::Success);

    // Steps: step1 SUCCESS
    let steps = store.workflow_steps().list().await?;
    let step = steps
        .into_iter()
        .find(|entry| entry.run_id == run.id)
        .expect("step not found");
    assert_eq!(step.status, pgqrs::WorkflowStatus::Success);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_cancellation_before_consumer_materializes_run() -> anyhow::Result<()> {
    let (rig, message) = create_workflow_test_rig(
        scenario_cancel_before_materialize_wf,
        "scenario_cancel_before_start_cons",
        json!({"msg": "cancel_before_start"}),
    )
    .await?;

    // External actor resolves the run directly from the trigger message and requests
    // cancellation before the consumer even dequeues the workflow trigger.
    let run = rig.as_external_actor_get_run(&message).await?;
    let _ = run.cancel().await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelling).await?;

    let execution_calls = Arc::new(Mutex::new(0u32));
    let handler = pgqrs::workflow_handler(rig.store().clone(), {
        let execution_calls = execution_calls.clone();
        move |run, _input: serde_json::Value| {
            let execution_calls = execution_calls.clone();
            async move { app_workflow_cancel_replay(run, execution_calls).await }
        }
    });

    // Consumer later dequeues the message and opens an attempt. Start is already no longer
    // allowed, and a later queue dispatch should finalize cancellation without user code running.
    let dequeued = rig
        .as_consumer_dequeue()
        .await?
        .expect("expected one message");
    let attempt = rig.as_consumer_open_attempt(dequeued).await?;

    let start_result = attempt.clone().run.start().await;
    assert!(
        matches!(start_result, Err(pgqrs::Error::ValidationFailed { .. })),
        "expected cancellation to block start, got {start_result:?}"
    );

    dispatch_attempt_message(rig.consumer(), &attempt, {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    })
    .await?;

    assert_message_archived(rig.store(), &message).await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelled).await?;

    let execution_calls = execution_calls
        .lock()
        .expect("execution_calls lock poisoned");
    assert_eq!(*execution_calls, 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_cancellation_after_run_exists_before_start() -> anyhow::Result<()> {
    let (rig, message) = create_workflow_test_rig(
        scenario_cancel_before_start_wf,
        "scenario_cancel_between_open_and_start_cons",
        json!({"msg": "cancel_between_open_and_start"}),
    )
    .await?;

    // Consumer acquires the trigger and materializes the run, but has not started it yet.
    let dequeued = rig
        .as_consumer_dequeue()
        .await?
        .expect("expected one message");
    let mut attempt = rig.as_consumer_open_attempt(dequeued).await?;

    // External actor sends the cancellation request against the same run.
    let run = rig.as_external_actor_get_run(&attempt.message).await?;
    let _ = run.cancel().await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelling).await?;

    // The consumer's later start() call should be rejected before user workflow code runs.
    let start_result = attempt.start().await;
    assert!(
        matches!(start_result, Err(pgqrs::Error::ValidationFailed { .. })),
        "expected cancellation to block start, got {start_result:?}"
    );
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelling).await?;

    let _ = message;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_cancellation_after_start_before_first_step() -> anyhow::Result<()> {
    let (rig, message) = create_workflow_test_rig(
        scenario_cancel_before_first_step_wf,
        "scenario_cancel_before_first_step_cons",
        json!({"msg": "cancel_before_first_step"}),
    )
    .await?;

    // Consumer acquires the trigger, materializes the run, and moves it to RUNNING.
    let dequeued = rig
        .as_consumer_dequeue()
        .await?
        .expect("expected one message");
    let mut attempt = rig.as_consumer_open_attempt(dequeued).await?;
    attempt.start().await?;

    // External actor requests cancellation after the run is already RUNNING.
    let run = rig.as_external_actor_get_run(&attempt.message).await?;
    let _ = run.cancel().await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelling).await?;

    let step1_calls = Arc::new(Mutex::new(0u32));

    // When the consumer invokes workflow logic, the first step boundary should observe the
    // cancellation request, finalize the run, and avoid entering the step body.
    let invoke_result = attempt
        .invoke({
            let step1_calls = step1_calls.clone();
            move |run| async move { app_workflow_cancel_before_first_step(run, step1_calls).await }
        })
        .await;
    assert!(matches!(invoke_result, Err(pgqrs::Error::Cancelled { .. })));

    attempt.archive(rig.consumer()).await?;
    assert_message_archived(rig.store(), &message).await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelled).await?;

    let step1_calls = step1_calls.lock().expect("step1_calls lock poisoned");
    assert_eq!(*step1_calls, 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_cancellation_during_step_execution() -> anyhow::Result<()> {
    let (rig, message) = create_workflow_test_rig(
        scenario_cancel_boundary_wf,
        "scenario_cancel_boundary_cons",
        json!({"msg": "cancel"}),
    )
    .await?;

    let step1_started = Arc::new(Notify::new());
    let allow_step1_finish = Arc::new(Notify::new());
    let step2_calls = Arc::new(Mutex::new(0u32));

    // Consumer acquires the trigger, materializes the run, and starts it.
    let dequeued = rig
        .as_consumer_dequeue()
        .await?
        .expect("expected one message");
    let mut attempt = rig.as_consumer_open_attempt(dequeued).await?;
    attempt.start().await?;

    let worker_attempt = attempt.clone();
    let worker_consumer = rig.consumer().clone();
    let step1_started_for_worker = step1_started.clone();
    let allow_step1_finish_for_worker = allow_step1_finish.clone();
    let step2_calls_for_worker = step2_calls.clone();
    let worker_task = tokio::spawn(async move {
        let mut worker_attempt = worker_attempt;
        let result = worker_attempt
            .invoke({
                let step1_started = step1_started_for_worker.clone();
                let allow_step1_finish = allow_step1_finish_for_worker.clone();
                let step2_calls = step2_calls_for_worker.clone();
                move |run| async move {
                    app_workflow_cancelled_by_other_actor(
                        run,
                        step1_started,
                        allow_step1_finish,
                        step2_calls,
                    )
                    .await
                }
            })
            .await;

        match result {
            Ok(_) => worker_attempt.archive(&worker_consumer).await?,
            Err(pgqrs::Error::Cancelled { .. }) => worker_attempt.archive(&worker_consumer).await?,
            Err(err) => {
                worker_attempt.release(&worker_consumer).await?;
                return Err(anyhow::Error::from(err));
            }
        }

        Ok::<_, anyhow::Error>(worker_attempt)
    });

    // Step1 is in flight. An external actor sends the cancellation request while user code is
    // still blocked inside that step body.
    step1_started.notified().await;
    let run = rig.as_external_actor_get_run(&attempt.message).await?;
    let _ = run.cancel().await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelling).await?;

    // Once step1 completes, the next workflow boundary should observe CANCELLING and prevent
    // step2 from starting.
    allow_step1_finish.notify_one();
    let worker_attempt = worker_task.await??;
    let _ = worker_attempt;

    assert_message_archived(rig.store(), &message).await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelled).await?;

    let steps = steps_for_run(rig.store(), run.id()).await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_name, "step1");
    assert_eq!(steps[0].status, pgqrs::WorkflowStatus::Success);

    let step2_calls = step2_calls.lock().expect("step2_calls lock poisoned");
    assert_eq!(*step2_calls, 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_workflow_redelivery_while_cancelling_archives_without_running_handler(
) -> anyhow::Result<()> {
    let (rig, message) = create_workflow_test_rig(
        scenario_cancel_redelivery_wf,
        "scenario_cancel_after_start_cons",
        json!({"msg": "cancel_replay"}),
    )
    .await?;

    // First consumer attempt materializes and starts the run.
    let dequeued = rig
        .as_consumer_dequeue()
        .await?
        .expect("expected one message");
    let attempt = rig.as_consumer_open_attempt(dequeued).await?;
    let mut started_attempt = attempt.clone();
    started_attempt.start().await?;

    // External actor requests cancellation while the run is already RUNNING.
    let run = rig
        .as_external_actor_get_run(&started_attempt.message)
        .await?;
    let _ = run.cancel().await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelling).await?;

    // Simulate redelivery by releasing the in-flight trigger and letting the consumer receive it
    // again later.
    started_attempt.release(rig.consumer()).await?;

    let execution_calls = Arc::new(Mutex::new(0u32));
    let handler = pgqrs::workflow_handler(rig.store().clone(), {
        let execution_calls = execution_calls.clone();
        move |run, _input: serde_json::Value| {
            let execution_calls = execution_calls.clone();
            async move { app_workflow_cancel_replay(run, execution_calls).await }
        }
    });

    let dequeued = rig
        .as_consumer_dequeue()
        .await?
        .expect("expected one message");
    let redelivery_attempt = rig.as_consumer_open_attempt(dequeued).await?;
    dispatch_attempt_message(rig.consumer(), &redelivery_attempt, {
        let handler = handler.clone();
        move |msg| (handler)(msg)
    })
    .await?;

    assert_message_archived(rig.store(), &message).await?;
    assert_run_status(rig.store(), run.id(), pgqrs::WorkflowStatus::Cancelled).await?;

    let execution_calls = execution_calls
        .lock()
        .expect("execution_calls lock poisoned");
    assert_eq!(*execution_calls, 0);

    Ok(())
}
