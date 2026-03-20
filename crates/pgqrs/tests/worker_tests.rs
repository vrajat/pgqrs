//! Integration tests for worker management functionality

use chrono::Duration;
use pgqrs::store::AnyStore;
use pgqrs::types::WorkerStatus;
use pgqrs::{Store, Worker};
use serde_json::json;

mod common;

async fn create_store() -> AnyStore {
    common::create_store("pgqrs_worker_test").await
}

#[tokio::test]
async fn test_worker_registration() {
    let store = create_store().await;

    // Create a test queue
    let queue = pgqrs::admin(&store)
        .create_queue("test_queue")
        .await
        .unwrap();

    // Create a producer (which registers a worker)
    let producer = pgqrs::producer("test-host", 8080, &queue.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Verify worker appears in queue workers list
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, producer.worker_id());
    assert_eq!(workers[0].hostname, "test-host");
    assert_eq!(workers[0].port, 8080);
    assert_eq!(workers[0].queue_id, Some(queue.id));
    assert_eq!(workers[0].status, WorkerStatus::Ready);
}

#[tokio::test]
async fn test_worker_lifecycle() {
    let store = create_store().await;

    // Create a test queue
    let queue = pgqrs::admin(&store)
        .create_queue("lifecycle_queue")
        .await
        .unwrap();

    // Create a producer (which registers a worker)
    let producer = pgqrs::producer("lifecycle-host", 9090, &queue.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test heartbeat
    producer.heartbeat().await.unwrap();

    // Test shutdown process (must suspend first)
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();

    // Verify worker is in stopped state
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, WorkerStatus::Stopped);
}

#[tokio::test]
async fn test_worker_message_assignment() {
    let store = create_store().await;

    // Create a test queue
    let queue_info = pgqrs::admin(&store)
        .create_queue("message_queue")
        .await
        .unwrap();

    // Create a producer and consumer with unique hostname+port combinations
    let producer = pgqrs::producer("message-producer-host", 7070, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("message-consumer-host", 7071, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Add some messages to the queue
    pgqrs::enqueue()
        .message(&json!({"task": "test1"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    pgqrs::enqueue()
        .message(&json!({"task": "test2"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    // Read messages normally
    let messages_list = pgqrs::dequeue()
        .worker(&consumer)
        .batch(2)
        .fetch_all(&store)
        .await
        .unwrap();
    assert_eq!(messages_list.len(), 2);

    // Verify worker can read messages from queue
    assert!(!messages_list.is_empty());
    for msg in &messages_list {
        assert!(msg.id > 0);
    }

    // Verify worker can process and delete messages
    let message_ids: Vec<i64> = messages_list.iter().map(|m| m.id).collect();
    consumer.delete_many(message_ids).await.unwrap();

    // Verify messages were deleted
    assert_eq!(
        pgqrs::tables(&store)
            .messages()
            .count_pending_for_queue(queue_info.id)
            .await
            .unwrap(),
        0
    );
    assert!(pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await
        .is_ok());
    assert!(pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await
        .is_ok());
    assert!(pgqrs::admin(&store).delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_producer_shutdown() {
    const TEST_QUEUE_PRODUCER_SHUTDOWN: &str = "test_producer_shutdown";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_PRODUCER_SHUTDOWN)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test_producer_shutdown", 3015, TEST_QUEUE_PRODUCER_SHUTDOWN)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Verify worker starts in Ready state
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, pgqrs::types::WorkerStatus::Ready);

    // First suspend the producer
    let suspend_result = producer.suspend().await;
    assert!(suspend_result.is_ok(), "Producer suspend should succeed");

    // Verify worker is now suspended
    let status = producer.status().await.unwrap();
    assert_eq!(status, pgqrs::types::WorkerStatus::Suspended);

    // Shutdown the producer (must be suspended first)
    let shutdown_result = producer.shutdown().await;
    assert!(shutdown_result.is_ok(), "Producer shutdown should succeed");

    // Verify worker transitioned through states correctly
    let workers_after = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers_after.len(), 1);
    assert_eq!(workers_after[0].status, pgqrs::types::WorkerStatus::Stopped);

    // Verify shutdown timestamp was set
    assert!(
        workers_after[0].shutdown_at.is_some(),
        "Shutdown timestamp should be set"
    );

    // Cleanup
    pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_worker_health_and_heartbeat() {
    let store = create_store().await;
    let queue_name = "test_worker_health";
    let _queue_info = store.queue(queue_name).await.unwrap();

    let consumer = pgqrs::consumer("host", 5000, queue_name)
        .create(&store)
        .await
        .unwrap();

    // Check initial health (max age 10s)
    let is_healthy = consumer
        .is_healthy(chrono::Duration::seconds(10))
        .await
        .unwrap();
    assert!(is_healthy, "Newly created worker should be healthy");

    // Check status
    let status = consumer.status().await.unwrap();
    assert_eq!(status, pgqrs::types::WorkerStatus::Ready);

    // Heartbeat
    consumer.heartbeat().await.expect("Heartbeat failed");

    // Test health with a very short window (should still be healthy)
    let is_healthy_now = consumer
        .is_healthy(chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(is_healthy_now);

    // Test suspend/resume
    consumer.suspend().await.unwrap();
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );

    consumer.resume().await.unwrap();
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Ready
    );

    // Cleanup
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
}

#[tokio::test]
async fn test_consumer_shutdown_no_messages() {
    const TEST_QUEUE_CONSUMER_SHUTDOWN_EMPTY: &str = "test_consumer_shutdown_empty";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_EMPTY)
        .await
        .expect("Failed to create queue");

    let consumer = pgqrs::consumer(
        "test_consumer_shutdown_no_messages",
        3108,
        TEST_QUEUE_CONSUMER_SHUTDOWN_EMPTY,
    )
    .create(&store)
    .await
    .expect("Failed to create consumer");

    // First suspend the consumer (new lifecycle requirement)
    // Consumer can suspend when it has no pending messages
    let suspend_result = consumer.suspend().await;
    assert!(
        suspend_result.is_ok(),
        "Consumer suspend with no messages should succeed"
    );

    // Shutdown consumer (must be suspended first)
    let shutdown_result = consumer.shutdown().await;
    assert!(shutdown_result.is_ok(), "Consumer shutdown should succeed");

    // Verify worker status
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, pgqrs::types::WorkerStatus::Stopped);

    // Cleanup
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
async fn test_consumer_shutdown_with_held_messages() {
    const TEST_QUEUE_CONSUMER_SHUTDOWN_HELD: &str = "test_consumer_shutdown_held";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_HELD)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer(
        "test_consumer_shutdown_with_held_messages",
        3016,
        TEST_QUEUE_CONSUMER_SHUTDOWN_HELD,
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    let consumer = pgqrs::consumer(
        "test_consumer_shutdown_with_held_messages",
        3109,
        TEST_QUEUE_CONSUMER_SHUTDOWN_HELD,
    )
    .create(&store)
    .await
    .expect("Failed to create consumer");

    // Send multiple messages
    let msg1_ids = pgqrs::enqueue()
        .message(&json!({"task": "held"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg1 = msg1_ids[0];
    let msg2_ids = pgqrs::enqueue()
        .message(&json!({"task": "in_progress"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg2 = msg2_ids[0];
    let msg3_ids = pgqrs::enqueue()
        .message(&json!({"task": "held"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg3 = msg3_ids[0];

    // Dequeue messages (they become held by the worker)
    let dequeued = consumer.dequeue_many(3).await.unwrap();
    assert_eq!(dequeued.len(), 3);

    // Verify all messages are held by worker
    let worker_messages = pgqrs::admin(&store)
        .get_worker_messages(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(worker_messages.len(), 3);

    consumer.suspend().await.unwrap();
    // Consumer cannot shutdown while holding messages
    let shutdown_result = consumer.shutdown().await;
    assert!(
        shutdown_result.is_err(),
        "Consumer shutdown should fail when holding messages"
    );

    // Check the error is WorkerHasPendingMessages
    match shutdown_result {
        Err(pgqrs::error::Error::WorkerHasPendingMessages { count, .. }) => {
            assert_eq!(count, 3, "Should report 3 pending messages");
        }
        _ => panic!("Expected WorkerHasPendingMessages error"),
    }

    // Release messages using the new release_messages method
    let released = consumer.release_messages(&[msg1, msg3]).await.unwrap();
    assert_eq!(released, 2, "Should release 2 messages");

    // Still can't shutdown - msg2 is still held
    let shutdown_result = consumer.shutdown().await;
    assert!(
        shutdown_result.is_err(),
        "Consumer shutdown should fail when still holding messages"
    );

    // Release the last message
    let released = consumer.release_messages(&[msg2]).await.unwrap();
    assert_eq!(released, 1, "Should release 1 message");

    // Now shutdown should succeed
    let shutdown_result = consumer.shutdown().await;
    assert!(
        shutdown_result.is_ok(),
        "Consumer shutdown should succeed after releasing all messages"
    );

    // Verify worker status
    let consumer_worker = pgqrs::tables(&store)
        .workers()
        .get(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(consumer_worker.status, pgqrs::types::WorkerStatus::Stopped);

    // Verify all messages are back in pending state
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending_for_queue(queue_info.id)
        .await
        .unwrap();
    assert_eq!(
        pending_count, 3,
        "All messages should be back in pending state"
    );

    // Cleanup - purge queue to remove messages since consumer doesn't own them anymore
    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_HELD)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_consumer_shutdown_all_messages_released() {
    const TEST_QUEUE_CONSUMER_SHUTDOWN_ALL: &str = "test_consumer_shutdown_all";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_ALL)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer(
        "test_consumer_shutdown_all_messages_released",
        3017,
        TEST_QUEUE_CONSUMER_SHUTDOWN_ALL,
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    let consumer = pgqrs::consumer(
        "test_consumer_shutdown_all_messages_released",
        3110,
        TEST_QUEUE_CONSUMER_SHUTDOWN_ALL,
    )
    .create(&store)
    .await
    .expect("Failed to create consumer");

    // Send and dequeue messages
    let msg1_ids = pgqrs::enqueue()
        .message(&json!({"task": "release_me"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg1 = msg1_ids[0];
    let msg2_ids = pgqrs::enqueue()
        .message(&json!({"task": "release_me_too"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let msg2 = msg2_ids[0];

    let dequeued = consumer.dequeue_many(2).await.unwrap();
    assert_eq!(dequeued.len(), 2);

    // Release all messages before suspend
    let released = consumer.release_messages(&[msg1, msg2]).await.unwrap();
    assert_eq!(released, 2, "Should release 2 messages");

    // Now we can suspend
    let suspend_result = consumer.suspend().await;
    assert!(suspend_result.is_ok(), "Consumer suspend should succeed");

    // Shutdown consumer
    let shutdown_result = consumer.shutdown().await;
    assert!(shutdown_result.is_ok(), "Consumer shutdown should succeed");

    // Verify all messages are released back to pending
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending_for_queue(queue_info.id)
        .await
        .unwrap();
    assert_eq!(
        pending_count, 2,
        "All messages should be back in pending state"
    );

    // Verify no messages are held by worker
    let worker_messages = pgqrs::admin(&store)
        .get_worker_messages(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(
        worker_messages.len(),
        0,
        "No messages should be held by worker"
    );

    // Verify worker status
    let consumer_worker = pgqrs::tables(&store)
        .workers()
        .get(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(consumer_worker.status, pgqrs::types::WorkerStatus::Stopped);

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_ALL)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_delete_consumer() {
    let store = create_store().await;
    let queue_name = "test_admin_mgmt";
    let _queue_info = store.queue(queue_name).await.unwrap();

    // Initial workers
    // let initial_workers = store.workers().list().await.unwrap();

    // Create 2 temporary workers
    let c1 = pgqrs::consumer("host1", 6000, queue_name)
        .create(&store)
        .await
        .unwrap();
    let c2 = pgqrs::consumer("host2", 6001, queue_name)
        .create(&store)
        .await
        .unwrap();

    let workers = store.workers().list().await.unwrap();
    // assert!(workers.len() >= initial_workers.len() + 2); // Flaky due to parallel tests
    assert!(workers.iter().any(|w| w.id == c1.worker_id()));
    assert!(workers.iter().any(|w| w.id == c2.worker_id()));

    // Shutdown and delete one
    let w1_id = c1.worker_id();
    c1.suspend().await.unwrap();
    c1.shutdown().await.unwrap();

    let deleted = pgqrs::admin(&store).delete_worker(w1_id).await.unwrap();
    assert_eq!(deleted, 1);

    let workers_final = store.workers().list().await.unwrap();
    assert!(!workers_final.iter().any(|w| w.id == w1_id));

    // Cleanup
    c2.suspend().await.unwrap();
    c2.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .delete_worker(c2.worker_id())
        .await
        .unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
}

#[tokio::test]
async fn test_admin_worker_management() {
    let store = create_store().await;

    // Create test queues
    let queue1 = pgqrs::admin(&store)
        .create_queue("admin_queue1")
        .await
        .unwrap();
    let queue2 = pgqrs::admin(&store)
        .create_queue("admin_queue2")
        .await
        .unwrap();

    // Create producers on different queues (which registers workers)
    let producer1 = pgqrs::producer("admin-host1", 8001, &queue1.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer1");

    let producer2 = pgqrs::producer("admin-host2", 8002, &queue2.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer2");

    // Test listing all workers
    let all_workers = pgqrs::tables(&store).workers().list().await.unwrap();
    // Note: Cannot assert exact count when tests run in parallel
    // Other tests may create workers in the same schema
    assert!(
        all_workers.len() >= 2,
        "Should have at least our 2 workers, found {}",
        all_workers.len()
    );

    // Test listing workers by queue
    let queue1_workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue1.id)
        .await
        .unwrap();
    assert_eq!(queue1_workers.len(), 1);
    assert_eq!(queue1_workers[0].id, producer1.worker_id());

    let queue2_workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue2.id)
        .await
        .unwrap();
    assert_eq!(queue2_workers.len(), 1);
    assert_eq!(queue2_workers[0].id, producer2.worker_id());

    // Testing admin wrapper methods specifically
    // Note: worker_stats method on Admin trait might not be exposed or implemented yet in the interface
    // but the underlying implementation has it. Let's check via workers table if possible
    // or use count methods.

    // Assuming admin.worker_stats is not in the Admin trait (based on mod.rs view),
    // we should use equivalent logic or skip if specific to previous impl.
    // Use worker_stats method on Admin trait via AdminBuilder
    let stats = pgqrs::admin(&store)
        .worker_stats(&queue1.queue_name)
        .await
        .unwrap();

    assert_eq!(stats.total_workers, 1);
    assert_eq!(stats.ready_workers, 1);
    assert_eq!(stats.stopped_workers, 0);
}

#[tokio::test]
async fn test_admin_registration_persists_worker_record() {
    let store = create_store().await;

    let admin = store
        .admin("admin-host", 9100, &store.config().clone())
        .await
        .unwrap();
    let ephemeral_admin = store
        .admin_ephemeral(&store.config().clone())
        .await
        .unwrap();

    let admin_worker = store.workers().get(admin.worker_id()).await.unwrap();
    assert_eq!(admin_worker.id, admin.worker_id());
    assert_eq!(admin_worker.hostname, "admin-host");
    assert_eq!(admin_worker.port, 9100);
    assert_eq!(admin_worker.queue_id, None);
    assert_eq!(admin_worker.status, WorkerStatus::Ready);

    let ephemeral_worker = store
        .workers()
        .get(ephemeral_admin.worker_id())
        .await
        .unwrap();
    assert_eq!(ephemeral_worker.id, ephemeral_admin.worker_id());
    assert_eq!(ephemeral_worker.queue_id, None);
    assert_eq!(ephemeral_worker.status, WorkerStatus::Ready);

    admin.suspend().await.unwrap();
    admin.shutdown().await.unwrap();
    store.workers().delete(admin.worker_id()).await.unwrap();

    ephemeral_admin.suspend().await.unwrap();
    ephemeral_admin.shutdown().await.unwrap();
    store
        .workers()
        .delete(ephemeral_admin.worker_id())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_worker_health_check() {
    let store = create_store().await;

    // Create a test queue
    let queue = pgqrs::admin(&store)
        .create_queue("health_queue")
        .await
        .unwrap();

    // Create a producer (which registers a worker)
    let producer = pgqrs::producer("health-host", 6060, &queue.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Worker should be healthy initially
    let healthy = producer.is_healthy(Duration::seconds(300)).await.unwrap();
    assert!(healthy);

    // Worker should be unhealthy with very short timeout
    let unhealthy = producer.is_healthy(Duration::seconds(0)).await.unwrap();
    assert!(!unhealthy);

    // Update heartbeat and check health again
    producer.heartbeat().await.unwrap();
    let healthy_after = producer.is_healthy(Duration::seconds(300)).await.unwrap();
    assert!(healthy_after);
}

#[tokio::test]
async fn test_custom_schema_search_path() {
    // use common::TestBackend;
    #[cfg(feature = "sqlite")]
    skip_on_backend!(pgqrs::store::BackendType::Sqlite);
    #[cfg(feature = "s3")]
    skip_on_backend!(pgqrs::store::BackendType::S3);
    #[cfg(feature = "turso")]
    skip_on_backend!(pgqrs::store::BackendType::Turso);

    let store = create_store().await;

    // Check search_path via store helper
    let result: String = store.query_string("SHOW search_path").await.unwrap();

    // Should contain our custom schema
    assert!(
        result.contains("pgqrs_worker_test"),
        "search_path should contain 'pgqrs_worker_test', got: {}",
        result
    );

    // Create a queue to verify functionality works in custom schema
    let queue = pgqrs::admin(&store)
        .create_queue("schema_test_queue")
        .await
        .unwrap();

    assert_eq!(queue.queue_name, "schema_test_queue");

    // Create a producer to test worker functionality
    let producer = pgqrs::producer("schema-test-host", 5050, &queue.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Verify worker registration via admin listing
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].hostname, "schema-test-host");
    assert_eq!(workers[0].port, 5050);
    assert_eq!(workers[0].status, WorkerStatus::Ready);

    // Test worker operations work correctly using Worker trait methods
    producer.heartbeat().await.unwrap();
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();

    // Verify worker status updated
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, WorkerStatus::Stopped);
}

#[tokio::test]
async fn test_worker_deletion_with_references() {
    let store = create_store().await;

    // Create a test queue and register producer/consumer
    let queue_name = "test_worker_deletion_with_references";
    let queue_info = store.queue(queue_name).await.unwrap();

    let producer = pgqrs::producer("deletion-producer-host", 8080, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("deletion-consumer-host", 8081, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    let message_ids = pgqrs::enqueue()
        .message(&json!({"test": "data"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let message_id = message_ids[0];

    assert!(pgqrs::dequeue()
        .worker(&consumer)
        .fetch_one(&store)
        .await
        .is_ok());
    // Attempt to delete consumer worker with references - should fail
    let result = pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await;
    assert!(
        result.is_err(),
        "Worker deletion should fail when references exist"
    );

    let error_msg = format!("{}", result.unwrap_err());
    assert!(
        error_msg.contains("associated messages"),
        "Error should mention associated messages, got: {}",
        error_msg
    );

    // Verify workers still exist
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 2);
    consumer.delete(message_id).await.unwrap();
    assert!(pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await
        .is_ok());
    assert!(pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await
        .is_ok());
    assert!(pgqrs::admin(&store).delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_worker_deletion_without_references() {
    let store = create_store().await;

    // Create a test queue and register a producer
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    "test_deletion_clean".hash(&mut hasher);
    let queue_name = format!("test_delete_clean_{}", hasher.finish());
    let queue = pgqrs::admin(&store)
        .create_queue(&queue_name)
        .await
        .unwrap();

    let producer = pgqrs::producer("clean-delete-host", 8080, &queue.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Delete worker without references - should succeed
    let result = pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await;
    assert!(
        result.is_ok(),
        "Worker deletion should succeed when no references exist"
    );
    assert_eq!(result.unwrap(), 1, "Should delete exactly 1 worker");

    // Verify worker no longer exists
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 0);
}

#[tokio::test]
async fn test_worker_deletion_with_archived_references() {
    let store = create_store().await;

    // Create a test queue and register producer/consumer
    let queue_name = "test_worker_deletion_with_archived_references";
    let queue_info = store.queue(queue_name).await.unwrap();

    let producer = pgqrs::producer("archive-producer-host", 8080, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("archive-consumer-host", 8081, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Send and process a message (this should create archived reference)
    let message_ids = pgqrs::enqueue()
        .message(&json!({"test": "archive_data"}))
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let message_id = message_ids[0];

    // Read the message to assign it to worker
    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .fetch_all(&store)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);

    // Archive the message to create an archived reference
    consumer.archive(message_id).await.unwrap();

    // Attempt to delete consumer worker with archived references - should fail
    let result = pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await;
    assert!(
        result.is_err(),
        "Worker deletion should fail when archived references exist"
    );

    let error_msg = format!("{}", result.unwrap_err());
    assert!(
        error_msg.contains("associated messages"),
        "Error should mention associated messages, got: {}",
        error_msg
    );
}

/// Test that purge_old_workers removes stopped workers with old heartbeats.
///
/// Workers must be in Stopped state AND have old heartbeat to be purged.
/// Workers with recent heartbeats are not purged regardless of status.
#[tokio::test]
async fn test_purge_old_workers() {
    let store = create_store().await;

    // Create a test queue
    let queue_name = "test_purge_old_workers";
    let queue_info = store.queue(queue_name).await.unwrap();

    // Create two producers to be purged
    let producer1 = pgqrs::producer("purge-producer1", 8081, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer1");

    let producer2 = pgqrs::producer("purge-producer2", 8082, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer2");

    // Create a consumer that will NOT be purged (recent heartbeat)
    let consumer = pgqrs::consumer("purge-consumer", 8083, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Stop all workers (must suspend first per lifecycle)
    producer1.suspend().await.unwrap();
    producer1.shutdown().await.unwrap();
    producer2.suspend().await.unwrap();
    producer2.shutdown().await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();

    // Set old heartbeat ONLY for producer1 and producer2
    let old_seconds = Duration::days(30).num_seconds();

    let sql = if store.backend_name() == "sqlite"
        || store.backend_name() == "turso"
        || store.backend_name() == "s3"
    {
        "UPDATE pgqrs_workers SET heartbeat_at = datetime('now', '-' || ? || ' seconds') WHERE id = ?"
    } else {
        "UPDATE pgqrs_workers SET heartbeat_at = NOW() - $1 * INTERVAL '1 second' WHERE id = $2"
    };

    store
        .execute_raw_with_two_i64(sql, old_seconds, producer1.worker_id())
        .await
        .unwrap();
    store
        .execute_raw_with_two_i64(sql, old_seconds, producer2.worker_id())
        .await
        .unwrap();

    // Purge old workers - should only remove producer1 and producer2
    let purged_count = pgqrs::admin(&store)
        .purge_old_workers(chrono::Duration::seconds(60))
        .await
        .unwrap();

    assert_eq!(
        purged_count, 2,
        "Should purge exactly 2 workers with old heartbeats"
    );

    // Verify consumer remains (recent heartbeat)
    let workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1, "Only consumer should remain");
    assert_eq!(
        workers[0].id,
        consumer.worker_id(),
        "Remaining worker should be the consumer"
    );

    // Cleanup
    pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}
