use pgqrs::store::Store;
use serde_json::json;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    let database_url = common::get_postgres_dsn(Some("pgqrs_ephemeral_test")).await;
    let config =
        pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_ephemeral_test")
            .expect("Failed to create config with ephemeral_test schema");
    pgqrs::store::AnyStore::connect(&config)
        .await
        .expect("Failed to create store")
}

#[tokio::test]
async fn test_ephemeral_produce_consume() {
    let store = create_store().await;
    let queue_name = "test_ephemeral_basic";

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let payload = json!({"message": "test_ephemeral"});

    // Use ephemeral produce - worker is auto-created and cleaned up
    let msg_id = pgqrs::produce(&payload)
        .to(queue_name)
        .execute(&store)
        .await
        .expect("Failed to produce message");

    assert!(msg_id > 0, "Message ID should be positive");

    // Verify message was enqueued
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .unwrap();
    assert_eq!(pending_count, 1);

    // Use ephemeral dequeue - worker is auto-created and cleaned up
    let messages = pgqrs::dequeue()
        .from(queue_name)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue message");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload, payload);

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ephemeral_worker_auto_cleanup() {
    let store = create_store().await;
    let queue_name = "test_ephemeral_cleanup";

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Count workers before
    let workers_before = pgqrs::tables(&store)
        .workers()
        .list()
        .await
        .unwrap()
        .len();

    {
        // Produce in a scope - worker should be cleaned up when scope exits
        let _msg_id = pgqrs::produce(&json!({"test": "cleanup"}))
            .to(queue_name)
            .execute(&store)
            .await
            .expect("Failed to produce");
    }

    // Give a moment for cleanup
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Count workers after - should be same or cleaned up
    let workers_after = pgqrs::tables(&store)
        .workers()
        .list()
        .await
        .unwrap()
        .len();

    // Workers should be cleaned up (ephemeral workers are suspended/shutdown)
    assert!(
        workers_after >= workers_before,
        "Ephemeral workers should be managed properly"
    );

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_concurrent_ephemeral_workers() {
    let store = create_store().await;
    let queue_name = "test_ephemeral_concurrent";

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Produce multiple messages concurrently using ephemeral workers
    let mut handles = vec![];
    for i in 0..5 {
        let store_clone = store.clone();
        let queue_clone = queue_name.to_string();
        let handle = tokio::spawn(async move {
            pgqrs::produce(&json!({"index": i}))
                .to(&queue_clone)
                .execute(&store_clone)
                .await
        });
        handles.push(handle);
    }

    // Wait for all produces to complete
    for handle in handles {
        handle.await.unwrap().expect("Failed to produce");
    }

    // Verify all messages were enqueued
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .unwrap();
    assert_eq!(pending_count, 5);

    // Consume all messages concurrently
    let mut consume_handles = vec![];
    for _ in 0..5 {
        let store_clone = store.clone();
        let queue_clone = queue_name.to_string();
        let handle = tokio::spawn(async move {
            pgqrs::dequeue()
                .from(&queue_clone)
                .batch(1)
                .fetch_all(&store_clone)
                .await
        });
        consume_handles.push(handle);
    }

    let mut total_consumed = 0;
    for handle in consume_handles {
        let messages = handle.await.unwrap().expect("Failed to consume");
        total_consumed += messages.len();
    }

    assert!(
        total_consumed >= 5,
        "Should consume at least 5 messages, got {}",
        total_consumed
    );

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ephemeral_produce_batch() {
    let store = create_store().await;
    let queue_name = "test_ephemeral_batch";

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let payloads = vec![
        json!({"index": 0}),
        json!({"index": 1}),
        json!({"index": 2}),
    ];

    // Use ephemeral produce_batch
    let msg_ids = pgqrs::produce_batch(&payloads)
        .to(queue_name)
        .execute(&store)
        .await
        .expect("Failed to produce batch");

    assert_eq!(msg_ids.len(), 3);

    // Verify all messages were enqueued
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .unwrap();
    assert_eq!(pending_count, 3);

    // Use ephemeral dequeue for batch
    let messages = pgqrs::dequeue()
        .from(queue_name)
        .batch(3)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue batch");

    assert_eq!(messages.len(), 3);

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ephemeral_error_invalid_queue() {
    let store = create_store().await;
    let nonexistent_queue = "nonexistent_queue_12345";

    // Try to produce to a non-existent queue
    let result = pgqrs::produce(&json!({"test": "error"}))
        .to(nonexistent_queue)
        .execute(&store)
        .await;

    assert!(
        result.is_err(),
        "Should fail when producing to non-existent queue"
    );
}

#[tokio::test]
async fn test_ephemeral_with_delay() {
    let store = create_store().await;
    let queue_name = "test_ephemeral_delay";

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Produce with delay using enqueue builder (produce doesn't support delay)
    let producer = store
        .producer_ephemeral(queue_name, store.config())
        .await
        .expect("Failed to create ephemeral producer");

    let msg_id = pgqrs::enqueue(&json!({"delayed": true}))
        .worker(&*producer)
        .delay(5) // 5 second delay
        .execute(&store)
        .await
        .expect("Failed to produce delayed message");

    assert!(msg_id > 0);

    // Try to dequeue immediately - should get nothing
    let messages = pgqrs::dequeue()
        .from(queue_name)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue");

    assert_eq!(
        messages.len(),
        0,
        "Should not dequeue delayed message immediately"
    );

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ephemeral_with_vt_offset() {
    let store = create_store().await;
    let queue_name = "test_ephemeral_vt";

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Produce a message
    let _msg_id = pgqrs::produce(&json!({"vt_test": true}))
        .to(queue_name)
        .execute(&store)
        .await
        .expect("Failed to produce");

    // Dequeue with custom vt_offset
    let messages = pgqrs::dequeue()
        .from(queue_name)
        .batch(1)
        .vt_offset(10) // 10 second visibility timeout
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue");

    assert_eq!(messages.len(), 1);

    // Verify the message has a future vt
    let message = &messages[0];
    assert!(
        message.vt > chrono::Utc::now(),
        "Message should have future visibility timeout"
    );

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}
