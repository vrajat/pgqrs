use pgqrs::Store;
use serde_json::json;
use std::time::Duration;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    common::create_store("pgqrs_builder_test").await
}

async fn create_ergonomics_store() -> pgqrs::store::AnyStore {
    common::create_store("pgqrs_builder_ergonomics_test").await
}

async fn wait_for_worker_status(
    store: &pgqrs::store::AnyStore,
    worker_id: i64,
    expected: pgqrs::types::WorkerStatus,
) -> pgqrs::types::WorkerRecord {
    for _ in 0..50 {
        let worker = pgqrs::tables(store).workers().get(worker_id).await.unwrap();
        if worker.status == expected {
            return worker;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker {} did not reach status {:?}", worker_id, expected);
}

async fn wait_for_worker_heartbeat_advance(
    store: &pgqrs::store::AnyStore,
    worker_id: i64,
    after: chrono::DateTime<chrono::Utc>,
) -> pgqrs::types::WorkerRecord {
    for _ in 0..80 {
        let worker = pgqrs::tables(store).workers().get(worker_id).await.unwrap();
        if worker.heartbeat_at > after {
            return worker;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("worker {} heartbeat did not advance", worker_id);
}

#[tokio::test]
async fn test_enqueue_all_options() {
    let store = create_store().await;
    let queue_name = "test_enqueue_all_options";

    // Create queue and worker
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9000", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test enqueue with all options set
    let payload = json!({"test": "all_options"});
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .delay(5) // 5 second delay
        .execute(&store)
        .await
        .expect("Failed to enqueue with all options");

    assert_eq!(msg_ids.len(), 1);
    let msg_id = msg_ids[0];
    assert!(msg_id > 0);

    // Verify the message exists
    let message = pgqrs::tables(&store)
        .messages()
        .get(msg_id)
        .await
        .expect("Failed to get message");

    assert_eq!(message.payload, payload);
    assert!(message.vt > chrono::Utc::now()); // Should have future vt due to delay

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_enqueue_edge_cases() {
    let store = create_store().await;
    let queue_name = "test_enqueue_edge_cases";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9001", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test with empty object payload
    let empty_payload = json!({});
    let msg_ids = pgqrs::enqueue()
        .message(&empty_payload)
        .worker(&producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue empty payload");

    assert_eq!(msg_ids.len(), 1);
    assert!(msg_ids[0] > 0);

    // Test with max delay (large number)
    let max_delay_payload = json!({"delayed": true});
    let msg_ids = pgqrs::enqueue()
        .message(&max_delay_payload)
        .worker(&producer)
        .delay(3600) // 1 hour delay
        .execute(&store)
        .await
        .expect("Failed to enqueue with max delay");

    assert_eq!(msg_ids.len(), 1);
    assert!(msg_ids[0] > 0);

    // Test with zero delay (should be immediate)
    let zero_delay_payload = json!({"immediate": true});
    let msg_ids = pgqrs::enqueue()
        .message(&zero_delay_payload)
        .worker(&producer)
        .delay(0)
        .execute(&store)
        .await
        .expect("Failed to enqueue with zero delay");

    assert_eq!(msg_ids.len(), 1);
    assert!(msg_ids[0] > 0);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_enqueue_with_delay_duration() {
    let store = create_ergonomics_store().await;
    let queue_name = "test_with_delay";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9100", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test with_delay using Duration
    let payload = json!({"test": "with_delay"});
    let before = chrono::Utc::now();

    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .with_delay(Duration::from_secs(300)) // 5 minutes
        .execute(&store)
        .await
        .expect("Failed to enqueue with Duration delay");
    let msg_id = msg_ids[0];

    assert!(msg_id > 0);

    // Verify the message has correct visibility timeout
    let message = pgqrs::tables(&store)
        .messages()
        .get(msg_id)
        .await
        .expect("Failed to get message");

    assert_eq!(message.payload, payload);

    // VT should be approximately 300 seconds (5 mins) in the future
    let vt_diff = (message.vt - before).num_seconds();
    assert!((295..=305).contains(&vt_diff), "VT diff was {}", vt_diff);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_with_vt_duration() {
    let store = create_ergonomics_store().await;
    let queue_name = "test_with_vt";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9101", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host-9102", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Enqueue a message
    pgqrs::enqueue()
        .message(&json!({"test": "vt"}))
        .worker(&producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue");

    // Dequeue with Duration-based VT
    let before = chrono::Utc::now();
    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .with_vt(Duration::from_secs(600)) // 10 minutes
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue with Duration VT");

    assert_eq!(messages.len(), 1);

    // VT should be approximately 600 seconds in the future
    let vt_diff = (messages[0].vt - before).num_seconds();
    assert!((595..=605).contains(&vt_diff), "VT diff was {}", vt_diff);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    // Release messages before shutting down consumer
    let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    consumer.release_messages(&msg_ids).await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_batch() {
    let store = create_ergonomics_store().await;
    let queue_name = "test_dequeue_batch";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9103", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host-9104", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Enqueue 10 messages
    for i in 0..10 {
        pgqrs::enqueue()
            .message(&json!({"index": i}))
            .worker(&producer)
            .execute(&store)
            .await
            .expect("Failed to enqueue");
    }

    // Test batch() method
    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .batch(5)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue with batch");

    assert_eq!(messages.len(), 5);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    // Release messages before shutting down consumer
    let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    consumer.release_messages(&msg_ids).await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_builder_method_chaining() {
    let store = create_ergonomics_store().await;
    let queue_name = "test_chaining";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9105", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host-9106", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Test chaining multiple ergonomic methods with time mocking
    let payload = json!({"test": "chaining"});

    // Use a custom time in the past for the enqueue operation
    let custom_time = chrono::Utc::now() - chrono::Duration::seconds(20);

    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .with_delay(Duration::from_secs(10))
        .at(custom_time) // Mock time: message was enqueued 20s ago with 10s delay
        .execute(&store)
        .await
        .expect("Failed to enqueue");
    let msg_id = msg_ids[0];

    assert!(msg_id > 0);

    // Message should already be visible (enqueued 20s ago + 10s delay = -10s from now)
    // Dequeue with custom time that matches the message visibility
    let dequeue_time = custom_time + chrono::Duration::seconds(15); // 15s after enqueue, 5s after vt

    // Dequeue with chained ergonomic methods and custom time
    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .with_vt(Duration::from_secs(30))
        .at(dequeue_time) // Use time that ensures message is visible
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload, payload);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    // Release messages before shutting down consumer
    let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    consumer.release_messages(&msg_ids).await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_enqueue_batch_builder() {
    let store = create_store().await;
    let queue_name = "test_enqueue_batch_builder";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host-9004", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test batch enqueue
    let payloads = vec![
        json!({"batch": 0}),
        json!({"batch": 1}),
        json!({"batch": 2}),
    ];

    // UPDATED: Use .messages() instead of removed enqueue_batch function
    let msg_ids = pgqrs::enqueue()
        .messages(&payloads)
        .worker(&producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue batch");

    assert_eq!(msg_ids.len(), 3);
    for id in &msg_ids {
        assert!(*id > 0);
    }

    // Test empty batch
    let empty_batch: Vec<serde_json::Value> = vec![];
    let result = pgqrs::enqueue()
        .messages(&empty_batch)
        .worker(&producer)
        .execute(&store)
        .await;

    assert!(
        result.is_err(),
        "Empty batch should return validation error"
    );

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_producer_consumer_builders() {
    let store = create_store().await;
    let queue_name = "test_worker_builders";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Test producer builder
    let producer = pgqrs::producer("builder-host-9005", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    assert!(producer.worker_id() > 0);

    // Test consumer builder
    let consumer = pgqrs::consumer("builder-host-9006", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    assert!(consumer.worker_id() > 0);

    // Verify workers were created
    let workers = pgqrs::tables(&store)
        .workers()
        .list()
        .await
        .expect("Failed to list workers");

    assert!(workers.iter().any(|w| w.id == producer.worker_id()));
    assert!(workers.iter().any(|w| w.id == consumer.worker_id()));

    // Cleanup - delete workers before queue
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();

    pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await
        .unwrap();

    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_admin_builder() {
    let store = create_store().await;

    // Test admin builder - create new instance for each operation
    let verify_result = pgqrs::admin(&store).verify().await;
    assert!(verify_result.is_ok());

    // Create queue
    let queue_name = "test_admin_builder";
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    assert!(queue_info.id > 0);

    // Get metrics - create new admin instance
    let metrics = pgqrs::admin(&store)
        .all_queues_metrics()
        .await
        .expect("Failed to get metrics");

    assert!(metrics.iter().any(|m| m.name == queue_name));

    // Delete queue - create new admin instance
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_tables_builder() {
    let store = create_store().await;

    // Test tables builder - create new instance for each table access
    let queue_count = pgqrs::tables(&store)
        .queues()
        .count()
        .await
        .expect("Failed to count queues");
    assert!(queue_count >= 0);

    let message_count = pgqrs::tables(&store)
        .messages()
        .count()
        .await
        .expect("Failed to count messages");
    assert!(message_count >= 0);

    let worker_count = pgqrs::tables(&store)
        .workers()
        .count()
        .await
        .expect("Failed to count workers");
    assert!(worker_count >= 0);

    let workflow_count = pgqrs::tables(&store)
        .workflows()
        .count()
        .await
        .expect("Failed to count workflows");
    assert!(workflow_count >= 0);
}

#[tokio::test]
async fn test_dequeue_with_handlers() {
    let store = create_store().await;
    let queue_name = "test_dequeue_handlers";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Add 2 messages
    let payloads = vec![json!({"h": 1}), json!({"h": 2})];
    pgqrs::enqueue()
        .messages(&payloads)
        .to(queue_name)
        .execute(&store)
        .await
        .expect("Failed to enqueue messages");

    // Test single handler (managed via ephemeral worker logic in handle())
    pgqrs::dequeue()
        .from(queue_name)
        .handle(|msg| async move {
            assert_eq!(msg.payload["h"], 1);
            Ok(())
        })
        .execute(&store)
        .await
        .expect("Failed to handle single message");

    // Test batch handler
    pgqrs::dequeue()
        .from(queue_name)
        .batch(10) // should get remaining 1
        .handle_batch(|msgs| async move {
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload["h"], 2);
            Ok(())
        })
        .execute(&store)
        .await
        .expect("Failed to handle batch");

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_enqueue_empty_messages_error() {
    let store = create_store().await;
    let queue_name = "test_empty_messages";
    let queue_info = store.queue(queue_name).await.unwrap();
    let producer = pgqrs::producer("host-9999", queue_name)
        .create(&store)
        .await
        .unwrap();

    // Should fail because no .message() or .messages() called
    let result = pgqrs::enqueue().worker(&producer).execute(&store).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(
                reason.contains("No messages to enqueue"),
                "Expected 'No messages to enqueue' error, got: {}",
                reason
            );
        }
        _ => panic!("Expected ValidationFailed error, got: {:?}", err),
    }

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_builder_delay_behavior() {
    let store = create_store().await;
    let queue_name = "test_builder_delay_behavior";
    let queue_info = store.queue(queue_name).await.unwrap();

    let payload = json!({"delayed": true});

    // Enqueue with 2 second delay
    pgqrs::enqueue()
        .message(&payload)
        .to(queue_name)
        .delay(2)
        .execute(&store)
        .await
        .expect("Failed to enqueue delayed message");

    // Dequeue immediately - should be None
    let msg = pgqrs::dequeue()
        .from(queue_name)
        .fetch_one(&store)
        .await
        .expect("Failed to fetch");
    assert!(msg.is_none(), "Message should not be visible yet");

    // Use time travel instead of sleep
    // Dequeue at (now + 3s) - should be Some because 3s > 2s delay
    let future_time = chrono::Utc::now() + chrono::Duration::seconds(3);

    // Dequeue at future time (now + 3s) - should be Some
    let msg = pgqrs::dequeue()
        .from(queue_name)
        .at(future_time)
        .fetch_one(&store)
        .await
        .expect("Failed to fetch");
    assert!(msg.is_some(), "Message should be visible now");
    assert_eq!(msg.unwrap().payload, payload);

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_single_handler_rejects_batch_size_gt_one() {
    let store = create_store().await;
    let queue_name = "test_dequeue_single_handler_batch_validation";
    let queue_info = store.queue(queue_name).await.unwrap();

    let execute_err = pgqrs::dequeue()
        .from(queue_name)
        .batch(2)
        .handle(|_msg| async move { Ok(()) })
        .execute(&store)
        .await
        .unwrap_err();

    match execute_err {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(
                reason.contains("single-message handlers require batch size = 1"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("Expected ValidationFailed, got {:?}", other),
    }

    let poll_err = pgqrs::dequeue()
        .from(queue_name)
        .batch(2)
        .handle(|_msg| async move { Ok(()) })
        .poll(&store)
        .await
        .unwrap_err();

    match poll_err {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(
                reason.contains("single-message handlers require batch size = 1"),
                "unexpected reason: {}",
                reason
            );
        }
        other => panic!("Expected ValidationFailed, got {:?}", other),
    }

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_single_handler_poll_exits_cleanly_when_consumer_suspended() {
    let store = create_store().await;
    let queue_name = "test_dequeue_single_handler_poll_suspended";
    let queue_info = store.queue(queue_name).await.unwrap();
    let consumer = pgqrs::consumer("host-9999", queue_name)
        .create(&store)
        .await
        .unwrap();

    consumer.suspend().await.unwrap();

    let err = pgqrs::dequeue()
        .worker(&consumer)
        .handle(|_msg| async move { Ok(()) })
        .poll(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::Suspended { ref reason } if reason == "worker suspended"
    ));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_handler_poll_rejects_until() {
    let store = create_store().await;
    let queue_name = "test_dequeue_handler_poll_rejects_until";
    let queue_info = store.queue(queue_name).await.unwrap();

    let err = pgqrs::dequeue()
        .from(queue_name)
        .until(Duration::from_secs(1))
        .handle(|_msg| async move { Ok(()) })
        .poll(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::ValidationFailed { ref reason }
            if reason == "until() is not supported with handler poll loops"
    ));

    let err = pgqrs::dequeue()
        .from(queue_name)
        .until(Duration::from_secs(1))
        .batch(5)
        .handle_batch(|_msgs| async move { Ok(()) })
        .poll(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::ValidationFailed { ref reason }
            if reason == "until() is not supported with handler poll loops"
    ));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_fetch_all_exits_cleanly_when_consumer_suspended() {
    let store = create_store().await;
    let queue_name = "test_dequeue_fetch_all_suspended";
    let queue_info = store.queue(queue_name).await.unwrap();
    let consumer = pgqrs::consumer("host-9998", queue_name)
        .create(&store)
        .await
        .unwrap();

    consumer.suspend().await.unwrap();

    let err = pgqrs::dequeue()
        .worker(&consumer)
        .fetch_all(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::Suspended { ref reason } if reason == "worker suspended"
    ));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_fetch_one_exits_cleanly_when_consumer_suspended() {
    let store = create_store().await;
    let queue_name = "test_dequeue_fetch_one_suspended";
    let queue_info = store.queue(queue_name).await.unwrap();
    let consumer = pgqrs::consumer("host-9997", queue_name)
        .create(&store)
        .await
        .unwrap();

    consumer.suspend().await.unwrap();

    let err = pgqrs::dequeue()
        .worker(&consumer)
        .fetch_one(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::Suspended { ref reason } if reason == "worker suspended"
    ));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_batch_handler_poll_exits_cleanly_when_consumer_suspended() {
    let store = create_store().await;
    let queue_name = "test_dequeue_batch_handler_poll_suspended";
    let queue_info = store.queue(queue_name).await.unwrap();
    let consumer = pgqrs::consumer("host-9996", queue_name)
        .create(&store)
        .await
        .unwrap();

    consumer.suspend().await.unwrap();

    let err = pgqrs::dequeue()
        .worker(&consumer)
        .batch(5)
        .handle_batch(|_msgs| async move { Ok(()) })
        .poll(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::Suspended { ref reason } if reason == "worker suspended"
    ));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_single_handler_execute_propagates_handler_error() {
    let store = create_store().await;
    let queue_name = "test_dequeue_single_handler_execute_error";
    let queue_info = store.queue(queue_name).await.unwrap();

    pgqrs::enqueue()
        .message(&json!({"err": true}))
        .to(queue_name)
        .execute(&store)
        .await
        .unwrap();

    let err = pgqrs::dequeue()
        .from(queue_name)
        .handle(|_msg| async move {
            Err(pgqrs::error::Error::ValidationFailed {
                reason: "handler failed".to_string(),
            })
        })
        .execute(&store)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        pgqrs::error::Error::ValidationFailed { ref reason } if reason == "handler failed"
    ));

    let visible = pgqrs::dequeue()
        .from(queue_name)
        .fetch_one(&store)
        .await
        .unwrap();
    assert!(
        visible.is_some(),
        "message should be released back to the queue"
    );

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_poll_updates_worker_heartbeat_while_idle() {
    let store = common::create_store_with_config("pgqrs_builder_test", |config| {
        config.heartbeat_interval = 1;
        config.poll_interval_ms = 2_000;
    })
    .await;
    let queue_name = "test_dequeue_poll_heartbeat";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let consumer = pgqrs::consumer("heartbeat-host-9910", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");
    let fixed_now = chrono::Utc::now();

    let store_task = store.clone();
    let consumer_task_handle = consumer.clone();
    let task = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_task_handle)
            .batch(1)
            .at(fixed_now)
            .handle(|_msg| Box::pin(async { Ok(()) }))
            .poll(&store_task)
            .await
    });

    let worker_before = wait_for_worker_status(
        &store,
        consumer.worker_id(),
        pgqrs::types::WorkerStatus::Polling,
    )
    .await;
    let worker_after =
        wait_for_worker_heartbeat_advance(&store, consumer.worker_id(), worker_before.heartbeat_at)
            .await;

    assert!(
        worker_after.heartbeat_at > worker_before.heartbeat_at,
        "expected heartbeat_at to advance while polling idle"
    );

    consumer.interrupt().await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res, Err(pgqrs::error::Error::Suspended { .. })));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_poll_until_returns_empty_on_timeout() {
    let store = common::create_store_with_config("pgqrs_builder_test", |config| {
        config.poll_interval_ms = 10;
    })
    .await;
    let queue_name = "test_dequeue_poll_until_timeout";
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");
    let consumer = pgqrs::consumer("poll-for-timeout-host", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .poll_interval(Duration::from_millis(10))
        .until(Duration::from_millis(50))
        .poll(&store)
        .await
        .expect("bounded poll should not fail on timeout");

    assert!(messages.is_empty(), "empty queue should time out empty");
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Ready,
        "bounded poll should return the consumer to ready"
    );

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_poll_until_returns_available_messages_before_timeout() {
    let store = create_store().await;
    let queue_name = "test_dequeue_poll_until_messages";
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::producer("poll-for-producer-host", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");
    let consumer = pgqrs::consumer("poll-for-consumer-host", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    producer
        .enqueue(&json!({ "kind": "poll_until" }))
        .await
        .expect("message should enqueue");

    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .poll_interval(Duration::from_millis(10))
        .until(Duration::from_secs(5))
        .poll(&store)
        .await
        .expect("bounded poll should return available messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload, json!({ "kind": "poll_until" }));
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Ready,
        "bounded poll should return the consumer to ready"
    );

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_poll_returns_consumer_to_ready_after_message() {
    let store = create_store().await;
    let queue_name = "test_dequeue_poll_returns_ready";
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::producer("poll-ready-producer-host", queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");
    let consumer = pgqrs::consumer("poll-ready-consumer-host", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    producer
        .enqueue(&json!({ "kind": "poll" }))
        .await
        .expect("message should enqueue");

    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .batch(1)
        .poll_interval(Duration::from_millis(10))
        .poll(&store)
        .await
        .expect("poll should return available messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload, json!({ "kind": "poll" }));
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Ready,
        "one-shot poll should return the consumer to ready"
    );

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_poll_until_updates_worker_heartbeat_while_idle() {
    let store = common::create_store_with_config("pgqrs_builder_test", |config| {
        config.heartbeat_interval = 1;
        config.poll_interval_ms = 2_000;
    })
    .await;
    let queue_name = "test_dequeue_poll_until_heartbeat";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let consumer = pgqrs::consumer("heartbeat-until-host-9911", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    let store_task = store.clone();
    let consumer_task_handle = consumer.clone();
    let task = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_task_handle)
            .batch(1)
            .poll_interval(Duration::from_secs(2))
            .until(Duration::from_secs(5))
            .poll(&store_task)
            .await
    });

    let worker_before = wait_for_worker_status(
        &store,
        consumer.worker_id(),
        pgqrs::types::WorkerStatus::Polling,
    )
    .await;
    let worker_after =
        wait_for_worker_heartbeat_advance(&store, consumer.worker_id(), worker_before.heartbeat_at)
            .await;

    assert!(
        worker_after.heartbeat_at > worker_before.heartbeat_at,
        "expected heartbeat_at to advance while bounded polling idle"
    );

    consumer.interrupt().await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res, Err(pgqrs::error::Error::Suspended { .. })));

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_poll_until_returns_suspended_when_interrupted() {
    let store = common::create_store_with_config("pgqrs_builder_test", |config| {
        config.heartbeat_interval = 60;
        config.poll_interval_ms = 50;
    })
    .await;
    let queue_name = "test_dequeue_poll_until_interrupted";
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");
    let consumer = pgqrs::consumer("poll-until-interrupt-host", queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    let store_task = store.clone();
    let consumer_task_handle = consumer.clone();
    let task = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_task_handle)
            .batch(1)
            .poll_interval(Duration::from_millis(10))
            .until(Duration::from_secs(5))
            .poll(&store_task)
            .await
    });

    wait_for_worker_status(
        &store,
        consumer.worker_id(),
        pgqrs::types::WorkerStatus::Polling,
    )
    .await;

    consumer.interrupt().await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        res,
        Err(pgqrs::error::Error::Suspended { ref reason }) if reason == "worker interrupted"
    ));
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended,
        "interrupt should win over bounded poll completion"
    );

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_builder_vt_offset_behavior() {
    let store = create_store().await;
    let queue_name = "test_builder_vt_offset_behavior";
    let queue_info = store.queue(queue_name).await.unwrap();

    let payload = json!({"reappearing": true});
    pgqrs::enqueue()
        .message(&payload)
        .to(queue_name)
        .execute(&store)
        .await
        .unwrap();

    // Dequeue with 5 second vt_offset
    let msg = pgqrs::dequeue()
        .from(queue_name)
        .with_vt(Duration::from_secs(5))
        .fetch_one(&store)
        .await
        .expect("Failed to fetch")
        .expect("Should have message");

    assert_eq!(msg.payload, payload);

    // Verify VT duration
    let now = chrono::Utc::now();
    let diff = (msg.vt - now).num_seconds();
    assert!(
        (2..=8).contains(&diff),
        "VT should be ~5s in future, got {}s",
        diff
    );

    // Try to dequeue immediately - should be None
    let msg2 = pgqrs::dequeue()
        .from(queue_name)
        .fetch_one(&store)
        .await
        .expect("Failed to fetch");
    assert!(msg2.is_none(), "Message should be locked");

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_builder_batch_enqueue_advanced() {
    let store = create_store().await;
    let queue_name = "test_batch_advanced";
    let queue_info = store.queue(queue_name).await.unwrap();

    let payloads: Vec<_> = (0..10).map(|i| json!({"batch_idx": i})).collect();

    // Batch enqueue with 1 hour delay (using with_delay)
    let msg_ids = pgqrs::enqueue()
        .messages(&payloads)
        .to(queue_name)
        .with_delay(std::time::Duration::from_secs(3600))
        .execute(&store)
        .await
        .expect("Failed to enqueue batch");

    assert_eq!(msg_ids.len(), 10);

    // Verify all messages have future VT
    for id in msg_ids {
        let msg = pgqrs::tables(&store)
            .messages()
            .get(id)
            .await
            .expect("Message missing");

        assert!(msg.vt > chrono::Utc::now() + chrono::Duration::minutes(59));
    }

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}
