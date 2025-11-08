//! Integration tests for worker management functionality

use chrono::Duration;
use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::types::WorkerStatus;
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
    if let Err(e) = sqlx::query("TRUNCATE TABLE worker_repository RESTART IDENTITY CASCADE")
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
    let queue = admin
        .create_queue(&"test_queue".to_string(), false)
        .await
        .unwrap();

    // Register a worker
    let worker = admin
        .register(queue.queue_name.clone(), "test-host".to_string(), 8080)
        .await
        .unwrap();

    assert_eq!(worker.hostname, "test-host");
    assert_eq!(worker.port, 8080);
    assert_eq!(worker.queue_name, "test_queue");
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
    let queue = admin
        .create_queue(&"lifecycle_queue".to_string(), false)
        .await
        .unwrap();

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
    let queue = admin
        .create_queue(&"message_queue".to_string(), false)
        .await
        .unwrap();

    // Register a worker to verify the worker registration process
    let _worker = admin
        .register(queue.queue_name.clone(), "message-host".to_string(), 7070)
        .await
        .unwrap();

    // Add some messages to the queue
    queue.enqueue(&json!({"task": "test1"})).await.unwrap();
    queue.enqueue(&json!({"task": "test2"})).await.unwrap();

    // Read messages normally
    let messages = queue.read(2).await.unwrap();
    assert_eq!(messages.len(), 2);

    // Verify worker can read messages from queue
    assert!(!messages.is_empty());
    for msg in &messages {
        assert!(msg.msg_id > 0);
    }

    // Verify worker can process and delete messages
    let message_ids: Vec<i64> = messages.iter().map(|m| m.msg_id).collect();
    queue.delete_batch(message_ids).await.unwrap();

    // Verify messages were deleted
    let remaining_messages = queue.read(10).await.unwrap();
    assert_eq!(remaining_messages.len(), 0);
}

#[tokio::test]
#[serial]
async fn test_admin_worker_management() {
    let admin = create_admin().await;

    // Create test queues
    let queue1 = admin
        .create_queue(&"admin_queue1".to_string(), false)
        .await
        .unwrap();
    let queue2 = admin
        .create_queue(&"admin_queue2".to_string(), false)
        .await
        .unwrap();

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
    let queue = admin
        .create_queue(&"health_queue".to_string(), false)
        .await
        .unwrap();

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
    let queue = admin
        .create_queue(&"schema_test_queue".to_string(), false)
        .await
        .unwrap();

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
