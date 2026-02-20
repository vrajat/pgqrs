use pgqrs::error::Result;
use pgqrs::store::AnyStore;
use pgqrs::Run;
use pgqrs::Store;
use serde_json::json;
use serial_test::serial;
use std::sync::{Arc, Mutex};

mod common;

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_concurrent_test").await
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
    let producer = pgqrs::producer("producer_host", 1000, queue_name)
        .create(&store)
        .await
        .expect("Failed to register producer");

    let consumer_a = pgqrs::consumer("consumer_a", 2000, queue_name)
        .create(&store)
        .await
        .expect("Failed to register consumer A");

    let consumer_b = pgqrs::consumer("consumer_b", 2001, queue_name)
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
    let msgs_b = consumer_b.dequeue().await.expect("Dequeue B failed");
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
async fn test_zombie_consumer_batch_ops() {
    let store = create_store().await;

    let queue_name = "batch_race_queue";
    let _queue_info = store
        .queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("prod", 1, queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer_a = pgqrs::consumer("con_a", 2, queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer_b = pgqrs::consumer("con_b", 3, queue_name)
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
    let msgs_b = consumer_b.dequeue_many(2).await.unwrap();
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

    let consumer_a = pgqrs::consumer("consumer_a", 1001, queue_name)
        .create(&store)
        .await
        .unwrap();

    let consumer_b = pgqrs::consumer("consumer_b", 1002, queue_name)
        .create(&store)
        .await
        .unwrap();

    let producer = pgqrs::producer("producer", 2001, queue_name)
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
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let consumer = pgqrs::consumer("scenario_success_cons", 3200, workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&json!({"msg": "success"}))?
        .execute()
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
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let consumer = pgqrs::consumer("scenario_perm_err_cons", 3210, workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&json!({"msg": "perm_error"}))?
        .execute()
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
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let consumer = pgqrs::consumer("scenario_crash_cons", 3220, workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&json!({"msg": "crash_recovery"}))?
        .execute()
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
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let consumer = pgqrs::consumer("scenario_step_crash_cons", 3250, workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&json!({"msg": "step_crash"}))?
        .execute()
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
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let consumer = pgqrs::consumer("scenario_transient_cons", 3230, workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&json!({"msg": "transient_error"}))?
        .execute()
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
        .name(workflow_name)
        .store(&store)
        .create()
        .await?;

    let consumer = pgqrs::consumer("scenario_pause_cons", 3240, workflow_name)
        .create(&store)
        .await?;

    let message = pgqrs::workflow()
        .name(workflow_name)
        .store(&store)
        .trigger(&json!({"msg": "pause"}))?
        .execute()
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
