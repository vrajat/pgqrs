//! Integration tests for worker management functionality

use chrono::Duration;
use pgqrs::admin::Admin;
use pgqrs::config::Config;
use pgqrs::types::WorkerStatus;
use pgqrs::worker::Worker;
use pgqrs::{Consumer, Producer, Table, Workers};
use serde_json::json;
use serial_test::serial;

mod common;

async fn create_admin() -> pgqrs::admin::Admin {
    let database_url = common::get_postgres_dsn(Some("pgqrs_worker_test")).await;
    let admin =
        Admin::new(&Config::from_dsn_with_schema(database_url, "pgqrs_worker_test").unwrap())
            .await
            .expect("Failed to create Admin");

    // Clean up any existing workers to ensure test isolation
    if let Err(e) = sqlx::query("TRUNCATE TABLE pgqrs_workers RESTART IDENTITY CASCADE")
        .execute(&admin.pool)
        .await
    {
        // Ignore error in case table doesn't exist yet
        eprintln!("Warning: Failed to truncate workers table: {}", e);
    }

    admin
}

#[tokio::test]
#[serial]
async fn test_worker_registration() {
    let admin = create_admin().await;

    // Create a test queue
    let queue = admin.create_queue("test_queue").await.unwrap();

    // Create a producer (which registers a worker)
    let producer = Producer::new(admin.pool.clone(), &queue, "test-host", 8080, &admin.config)
        .await
        .expect("Failed to create producer");

    // Verify worker appears in queue workers list
    let workers = Workers::new(admin.pool.clone())
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
    let admin = create_admin().await;

    // Create a test queue
    let queue = admin.create_queue("lifecycle_queue").await.unwrap();

    // Create a producer (which registers a worker)
    let producer = Producer::new(
        admin.pool.clone(),
        &queue,
        "lifecycle-host",
        9090,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Test heartbeat
    producer.heartbeat().await.unwrap();

    // Test shutdown process (must suspend first)
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();

    // Verify worker is in stopped state
    let workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, WorkerStatus::Stopped);
}

#[tokio::test]
#[serial]
async fn test_worker_message_assignment() {
    let admin = create_admin().await;

    // Create a test queue
    let queue_info = admin.create_queue("message_queue").await.unwrap();

    // Create a producer and consumer with unique hostname+port combinations
    let producer = Producer::new(
        admin.pool.clone(),
        &queue_info,
        "message-producer-host",
        7070,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    let consumer = Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "message-consumer-host",
        7071,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");

    let messages = pgqrs::tables::Messages::new(admin.pool.clone());

    // Add some messages to the queue
    producer.enqueue(&json!({"task": "test1"})).await.unwrap();
    producer.enqueue(&json!({"task": "test2"})).await.unwrap();

    // Read messages normally
    let messages_list = consumer.dequeue_many(2).await.unwrap();
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
    assert_eq!(messages.count_pending(queue_info.id).await.unwrap(), 0);
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
#[serial]
async fn test_admin_worker_management() {
    let admin = create_admin().await;

    // Create test queues
    let queue1 = admin.create_queue("admin_queue1").await.unwrap();
    let queue2 = admin.create_queue("admin_queue2").await.unwrap();

    // Create producers on different queues (which registers workers)
    let producer1 = Producer::new(
        admin.pool.clone(),
        &queue1,
        "admin-host1",
        8001,
        &admin.config,
    )
    .await
    .expect("Failed to create producer1");

    let producer2 = Producer::new(
        admin.pool.clone(),
        &queue2,
        "admin-host2",
        8002,
        &admin.config,
    )
    .await
    .expect("Failed to create producer2");

    // Test listing all workers
    let all_workers = Workers::new(admin.pool.clone()).list().await.unwrap();
    assert_eq!(all_workers.len(), 2);

    // Test listing workers by queue
    let queue1_workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue1.id)
        .await
        .unwrap();
    assert_eq!(queue1_workers.len(), 1);
    assert_eq!(queue1_workers[0].id, producer1.worker_id());

    let queue2_workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue2.id)
        .await
        .unwrap();
    assert_eq!(queue2_workers.len(), 1);
    assert_eq!(queue2_workers[0].id, producer2.worker_id());

    // Test worker statistics
    let stats = admin.worker_stats("admin_queue1").await.unwrap();
    assert_eq!(stats.total_workers, 1);
    assert_eq!(stats.ready_workers, 1);
    assert_eq!(stats.stopped_workers, 0);
}

#[tokio::test]
#[serial]
async fn test_worker_health_check() {
    let admin = create_admin().await;

    // Create a test queue
    let queue = admin.create_queue("health_queue").await.unwrap();

    // Create a producer (which registers a worker)
    let producer = Producer::new(
        admin.pool.clone(),
        &queue,
        "health-host",
        6060,
        &admin.config,
    )
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
    let admin = create_admin().await;

    // Get a connection from the pool to check search_path
    let mut connection = admin.pool.acquire().await.unwrap();
    let result = sqlx::query_scalar::<_, String>("SHOW search_path")
        .fetch_one(&mut *connection)
        .await
        .unwrap();

    // Should contain our custom schema
    assert!(
        result.contains("pgqrs_worker_test"),
        "search_path should contain 'pgqrs_worker_test', got: {}",
        result
    );

    // Create a queue to verify functionality works in custom schema
    let queue = admin.create_queue("schema_test_queue").await.unwrap();

    assert_eq!(queue.queue_name, "schema_test_queue");

    // Create a producer to test worker functionality
    let producer = Producer::new(
        admin.pool.clone(),
        &queue,
        "schema-test-host",
        5050,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Verify worker registration via admin listing
    let workers = Workers::new(admin.pool.clone())
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
    let workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, WorkerStatus::Stopped);
}

#[tokio::test]
#[serial]
async fn test_worker_deletion_with_references() {
    let admin = create_admin().await;

    // Create a test queue and register producer/consumer
    let queue_name = "test_worker_deletion_with_references";
    let queue_info = admin.create_queue(queue_name).await.unwrap();

    let producer = Producer::new(
        admin.pool.clone(),
        &queue_info,
        "deletion-producer-host",
        8080,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    let consumer = Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "deletion-consumer-host",
        8081,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");

    let message = producer.enqueue(&json!({"test": "data"})).await.unwrap();

    assert!(consumer.dequeue().await.is_ok());
    // Attempt to delete consumer worker with references - should fail
    let result = admin.delete_worker(consumer.worker_id()).await;
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
    let workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 2);
    consumer.delete(message.id).await.unwrap();
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
#[serial]
async fn test_worker_deletion_without_references() {
    let admin = create_admin().await;

    // Create a test queue and register a producer
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    "test_deletion_clean".hash(&mut hasher);
    let queue_name = format!("test_delete_clean_{}", hasher.finish());
    let queue = admin.create_queue(&queue_name).await.unwrap();

    let producer = Producer::new(
        admin.pool.clone(),
        &queue,
        "clean-delete-host",
        8080,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Delete worker without references - should succeed
    let result = admin.delete_worker(producer.worker_id()).await;
    assert!(
        result.is_ok(),
        "Worker deletion should succeed when no references exist"
    );
    assert_eq!(result.unwrap(), 1, "Should delete exactly 1 worker");

    // Verify worker no longer exists
    let workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 0);
}

#[tokio::test]
#[serial]
async fn test_worker_deletion_with_archived_references() {
    let admin = create_admin().await;

    // Create a test queue and register producer/consumer
    let queue_name = "test_worker_deletion_with_archived_references";
    let queue_info = admin.create_queue(queue_name).await.unwrap();

    let producer = Producer::new(
        admin.pool.clone(),
        &queue_info,
        "archive-producer-host",
        8080,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    let consumer = Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "archive-consumer-host",
        8081,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");

    // Send and process a message (this should create archived reference)
    let message = producer
        .enqueue(&json!({"test": "archive_data"}))
        .await
        .unwrap();

    // Read the message to assign it to worker
    let messages = consumer.dequeue().await.unwrap();
    assert_eq!(messages.len(), 1);

    // Archive the message to create an archived reference
    consumer.archive(message.id).await.unwrap();

    // Attempt to delete consumer worker with archived references - should fail
    let result = admin.delete_worker(consumer.worker_id()).await;
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
    let admin = create_admin().await;

    // Create a test queue
    let queue_name = "test_purge_old_workers";
    let queue_info = admin.create_queue(queue_name).await.unwrap();

    // Create two producers to be purged
    let producer1 = Producer::new(
        admin.pool.clone(),
        &queue_info,
        "purge-producer1",
        8081,
        &admin.config,
    )
    .await
    .expect("Failed to create producer1");

    let producer2 = Producer::new(
        admin.pool.clone(),
        &queue_info,
        "purge-producer2",
        8082,
        &admin.config,
    )
    .await
    .expect("Failed to create producer2");

    // Create a consumer that will NOT be purged (recent heartbeat)
    let consumer = Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "purge-consumer",
        8083,
        &admin.config,
    )
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
    let old_time = chrono::Utc::now() - Duration::days(30);
    sqlx::query("UPDATE pgqrs_workers SET heartbeat_at = $1 WHERE id = ANY($2)")
        .bind(old_time)
        .bind(vec![producer1.worker_id(), producer2.worker_id()])
        .execute(&admin.pool)
        .await
        .unwrap();

    // Purge old workers - should only remove producer1 and producer2
    let purged_count = admin
        .purge_old_workers(std::time::Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(
        purged_count, 2,
        "Should purge exactly 2 workers with old heartbeats"
    );

    // Verify consumer remains (recent heartbeat)
    let workers = Workers::new(admin.pool.clone())
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
    admin.delete_worker(consumer.worker_id()).await.unwrap();
    admin.delete_queue(&queue_info).await.unwrap();
}
