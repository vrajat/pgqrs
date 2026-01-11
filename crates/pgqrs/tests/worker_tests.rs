//! Integration tests for worker management functionality

use chrono::Duration;
use pgqrs::store::AnyStore;
use pgqrs::types::WorkerStatus;
use pgqrs::Store;
use serde_json::json;
use serial_test::serial;

mod common;

async fn create_store() -> AnyStore {
    let store = common::create_store("pgqrs_worker_test").await;

    // Clean up any existing workers to ensure test isolation
    // SQLite doesn't support TRUNCATE, use DELETE
    let sql = if store.backend_name() == "sqlite" || store.backend_name() == "turso" {
        "DELETE FROM pgqrs_workers"
    } else {
        "TRUNCATE TABLE pgqrs_workers RESTART IDENTITY CASCADE"
    };

    if let Err(e) = store.execute_raw(sql).await {
        // Ignore error in case table doesn't exist yet
        eprintln!("Warning: Failed to clear workers table: {}", e);
    }
    store
}

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    pgqrs::enqueue()
        .message(&json!({"task": "test2"}))
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();

    // Read messages normally
    let messages_list = pgqrs::dequeue()
        .worker(&*consumer)
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
            .count_pending(queue_info.id)
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
#[serial]
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
    assert_eq!(all_workers.len(), 2);

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
#[serial]
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
#[serial]
async fn test_custom_schema_search_path() {
    use common::TestBackend;
    skip_on_backend!(TestBackend::Sqlite);
    skip_on_backend!(TestBackend::Turso);

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
#[serial]
async fn test_worker_deletion_with_references() {
    let store = create_store().await;

    // Create a test queue and register producer/consumer
    let queue_name = "test_worker_deletion_with_references";
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

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
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    let message_id = message_ids[0];

    assert!(pgqrs::dequeue()
        .worker(&*consumer)
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
        error_msg.contains("associated messages/archives"),
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
#[serial]
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
#[serial]
async fn test_worker_deletion_with_archived_references() {
    let store = create_store().await;

    // Create a test queue and register producer/consumer
    let queue_name = "test_worker_deletion_with_archived_references";
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

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
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    let message_id = message_ids[0];

    // Read the message to assign it to worker
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
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
        error_msg.contains("associated messages/archives"),
        "Error should mention associated archives, got: {}",
        error_msg
    );
}

/// Test that purge_old_workers removes stopped workers with old heartbeats.
///
/// Workers must be in Stopped state AND have old heartbeat to be purged.
/// Workers with recent heartbeats are not purged regardless of status.
#[tokio::test]
#[serial]
async fn test_purge_old_workers() {
    let store = create_store().await;

    // Create a test queue
    let queue_name = "test_purge_old_workers";
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

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

    let sql = if store.backend_name() == "sqlite" || store.backend_name() == "turso" {
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
