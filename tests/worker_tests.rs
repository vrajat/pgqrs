//! Integration tests for worker management functionality

use chrono::Duration;
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::types::WorkerStatus;
use pgqrs::{Consumer, Producer};
use serde_json::json;
use serial_test::serial;

mod common;

async fn create_admin() -> pgqrs::admin::PgqrsAdmin {
    let database_url = common::get_postgres_dsn(Some("pgqrs_worker_test")).await;
    let admin =
        PgqrsAdmin::new(&Config::from_dsn_with_schema(database_url, "pgqrs_worker_test").unwrap())
            .await
            .expect("Failed to create PgqrsAdmin");

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

    // Register a worker
    let worker = admin
        .register(queue.queue_name.clone(), "test-host".to_string(), 8080)
        .await
        .unwrap();

    assert_eq!(worker.hostname, "test-host");
    assert_eq!(worker.port, 8080);
    assert_eq!(worker.queue_id, queue.id);
    assert_eq!(worker.status, WorkerStatus::Ready);

    // Verify worker appears in queue workers list
    let workers = admin.list_all_workers().await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, worker.id);
}

#[tokio::test]
#[serial]
async fn test_worker_lifecycle() {
    let admin = create_admin().await;

    // Create a test queue
    let queue = admin.create_queue("lifecycle_queue").await.unwrap();

    // Register a worker
    let worker = admin
        .register(queue.queue_name.clone(), "lifecycle-host".to_string(), 9090)
        .await
        .unwrap();

    // Test heartbeat
    admin.heartbeat(worker.id).await.unwrap();

    // Test shutdown process
    admin.begin_shutdown(worker.id).await.unwrap();
    admin.mark_stopped(worker.id).await.unwrap();

    // Verify worker is in stopped state
    let workers = admin.list_queue_workers("lifecycle_queue").await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, WorkerStatus::Stopped);
}

#[tokio::test]
#[serial]
async fn test_worker_message_assignment() {
    let admin = create_admin().await;

    // Create a test queue
    let queue_info = admin.create_queue("message_queue").await.unwrap();
    let producer = pgqrs::Producer::new(admin.pool.clone(), &queue_info, &admin.config);
    let consumer = Consumer::new(admin.pool.clone(), &queue_info, &admin.config);
    let messages = pgqrs::tables::PgqrsMessages::new(admin.pool.clone());

    // Register a worker to verify the worker registration process
    let worker = admin
        .register(
            queue_info.queue_name.clone(),
            "message-host".to_string(),
            7070,
        )
        .await
        .unwrap();

    // Add some messages to the queue
    producer.enqueue(&json!({"task": "test1"})).await.unwrap();
    producer.enqueue(&json!({"task": "test2"})).await.unwrap();

    // Read messages normally
    let messages_list = consumer.dequeue_many(&worker, 2).await.unwrap();
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
    assert!(admin.delete_worker(worker.id).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
#[serial]
async fn test_admin_worker_management() {
    let admin = create_admin().await;

    // Create test queues
    let queue1 = admin.create_queue("admin_queue1").await.unwrap();
    let queue2 = admin.create_queue("admin_queue2").await.unwrap();

    // Register workers on different queues
    let worker1 = admin
        .register(queue1.queue_name.clone(), "admin-host1".to_string(), 8001)
        .await
        .unwrap();
    let worker2 = admin
        .register(queue2.queue_name.clone(), "admin-host2".to_string(), 8002)
        .await
        .unwrap();

    // Test listing all workers
    let all_workers = admin.list_all_workers().await.unwrap();
    assert_eq!(all_workers.len(), 2);

    // Test listing workers by queue
    let queue1_workers = admin.list_queue_workers("admin_queue1").await.unwrap();
    assert_eq!(queue1_workers.len(), 1);
    assert_eq!(queue1_workers[0].id, worker1.id);

    let queue2_workers = admin.list_queue_workers("admin_queue2").await.unwrap();
    assert_eq!(queue2_workers.len(), 1);
    assert_eq!(queue2_workers[0].id, worker2.id);

    // Test worker statistics
    let stats = admin.worker_stats("admin_queue1").await.unwrap();
    assert_eq!(stats.total_workers, 1);
    assert_eq!(stats.ready_workers, 1);
    assert_eq!(stats.shutting_down_workers, 0);
    assert_eq!(stats.stopped_workers, 0);
}

#[tokio::test]
#[serial]
async fn test_worker_health_check() {
    let admin = create_admin().await;

    // Create a test queue
    let queue = admin.create_queue("health_queue").await.unwrap();

    // Register a worker
    let worker = admin
        .register(queue.queue_name.clone(), "health-host".to_string(), 6060)
        .await
        .unwrap();

    // Worker should be healthy initially
    let healthy = admin
        .is_healthy(worker.id, Duration::seconds(300))
        .await
        .unwrap();
    assert!(healthy);

    // Worker should be unhealthy with very short timeout
    let unhealthy = admin
        .is_healthy(worker.id, Duration::seconds(0))
        .await
        .unwrap();
    assert!(!unhealthy);

    // Update heartbeat and check health again
    admin.heartbeat(worker.id).await.unwrap();
    let healthy_after = admin
        .is_healthy(worker.id, Duration::seconds(300))
        .await
        .unwrap();
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

    // Register a worker to test worker functionality
    let worker = admin
        .register(
            queue.queue_name.clone(),
            "schema-test-host".to_string(),
            5050,
        )
        .await
        .unwrap();

    assert_eq!(worker.hostname, "schema-test-host");
    assert_eq!(worker.port, 5050);
    assert_eq!(worker.status, WorkerStatus::Ready);

    // Test worker operations work correctly
    admin.heartbeat(worker.id).await.unwrap();
    admin.begin_shutdown(worker.id).await.unwrap();
    admin.mark_stopped(worker.id).await.unwrap();

    // Verify worker status updated
    let workers = admin.list_queue_workers("schema_test_queue").await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, WorkerStatus::Stopped);
}

#[tokio::test]
#[serial]
async fn test_worker_deletion_with_references() {
    let admin = create_admin().await;

    // Create a test queue and register a worker
    let queue_name = "test_worker_deletion_with_references";
    let queue_info = admin.create_queue(queue_name).await.unwrap();
    let producer = Producer::new(admin.pool.clone(), &queue_info, &admin.config);
    let consumer = Consumer::new(admin.pool.clone(), &queue_info, &admin.config);

    let worker = admin
        .register(queue_info.queue_name.clone(), "test-host".to_string(), 8080)
        .await
        .unwrap();

    let message = producer.enqueue(&json!({"test": "data"})).await.unwrap();

    assert!(consumer.dequeue(&worker).await.is_ok());
    // Attempt to delete worker with references - should fail
    let result = admin.delete_worker(worker.id).await;
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

    // Verify worker still exists
    let workers = admin
        .list_queue_workers(&queue_info.queue_name)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    consumer.delete(message.id).await.unwrap();
    assert!(admin.delete_worker(worker.id).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
#[serial]
async fn test_worker_deletion_without_references() {
    let admin = create_admin().await;

    // Create a test queue and register a worker
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    "test_deletion_clean".hash(&mut hasher);
    let queue_name = format!("test_delete_clean_{}", hasher.finish());
    let queue = admin.create_queue(&queue_name).await.unwrap();
    let worker = admin
        .register(queue.queue_name.clone(), "test-host".to_string(), 8080)
        .await
        .unwrap();

    // Delete worker without references - should succeed
    let result = admin.delete_worker(worker.id).await;
    assert!(
        result.is_ok(),
        "Worker deletion should succeed when no references exist"
    );
    assert_eq!(result.unwrap(), 1, "Should delete exactly 1 worker");

    // Verify worker no longer exists
    let workers = admin.list_queue_workers(&queue.queue_name).await.unwrap();
    assert_eq!(workers.len(), 0);
}

#[tokio::test]
#[serial]
async fn test_worker_deletion_with_archived_references() {
    let admin = create_admin().await;

    // Create a test queue and register a worker
    let queue_name = "test_worker_deletion_with_archived_references";
    let queue_info = admin.create_queue(queue_name).await.unwrap();
    let producer = Producer::new(admin.pool.clone(), &queue_info, &admin.config);
    let consumer = Consumer::new(admin.pool.clone(), &queue_info, &admin.config);
    let worker = admin
        .register(queue_info.queue_name.clone(), "test-host".to_string(), 8080)
        .await
        .unwrap();

    // Send and process a message (this should create archived reference)
    let message = producer
        .enqueue(&json!({"test": "archive_data"}))
        .await
        .unwrap();

    // Read the message to assign it to worker
    let messages = consumer.dequeue(&worker).await.unwrap();
    assert_eq!(messages.len(), 1);

    // Archive the message to create an archived reference
    consumer.archive(message.id).await.unwrap();

    // Attempt to delete worker with archived references - should fail
    let result = admin.delete_worker(worker.id).await;
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

#[tokio::test]
#[serial]
async fn test_purge_old_workers_respects_references() {
    let admin = create_admin().await;

    // Create a test queue
    let queue_name = "test_purge_old_workers_with_references";
    let queue_info = admin.create_queue(queue_name).await.unwrap();
    let producer = Producer::new(admin.pool.clone(), &queue_info, &admin.config);
    let consumer = Consumer::new(admin.pool.clone(), &queue_info, &admin.config);

    // Register two workers
    let worker1 = admin
        .register(queue_info.queue_name.clone(), "worker1".to_string(), 8081)
        .await
        .unwrap();
    let worker2 = admin
        .register(queue_info.queue_name.clone(), "worker2".to_string(), 8082)
        .await
        .unwrap();

    // Stop both workers
    admin.mark_stopped(worker1.id).await.unwrap();
    admin.mark_stopped(worker2.id).await.unwrap();

    // Send a message that gets assigned to worker1
    producer
        .enqueue(&json!({"test": "purge_data"}))
        .await
        .unwrap();

    assert!(consumer.dequeue(&worker1).await.is_ok());

    // Manually update heartbeat to be old for both workers
    let old_time = chrono::Utc::now() - Duration::days(30);
    sqlx::query("UPDATE pgqrs_workers SET heartbeat_at = $1 WHERE id = ANY($2)")
        .bind(old_time)
        .bind(vec![worker1.id, worker2.id])
        .execute(&admin.pool)
        .await
        .unwrap();

    // Purge old workers (should only remove worker2, not worker1 with message)
    let purged_count = admin
        .purge_old_workers(std::time::Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(
        purged_count, 1,
        "Should only purge worker without references"
    );

    // Verify only worker1 (with references) remains
    let workers = admin
        .list_queue_workers(&queue_info.queue_name)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, worker1.id);
}
