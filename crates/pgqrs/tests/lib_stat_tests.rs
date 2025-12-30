use pgqrs::store::AnyStore;
use serde_json::json;

mod common;

async fn create_store() -> AnyStore {
    let database_url = common::get_postgres_dsn(Some("pgqrs_lib_stat_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_lib_stat_test")
        .expect("Failed to create config with lib_stat_test schema");
    pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to connect store")
}

#[tokio::test]
async fn test_queue_metrics() {
    let store = create_store().await;
    let queue_name = "test_metrics_queue";

    // Create queue
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();
    let producer = pgqrs::producer("test_metrics_producer", 3200, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");
    let consumer = pgqrs::consumer("test_metrics_consumer", 3201, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Initial state: 0 messages
    let metrics = pgqrs::admin(&store)
        .queue_metrics(queue_name)
        .await
        .expect("Failed to get metrics");
    assert_eq!(metrics.total_messages, 0);
    assert_eq!(metrics.pending_messages, 0);
    assert_eq!(metrics.locked_messages, 0);
    assert_eq!(metrics.archived_messages, 0);

    // Enqueue 2 messages
    pgqrs::enqueue(&json!({"id": 1}))
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    pgqrs::enqueue(&json!({"id": 2}))
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();

    // Check metrics: 2 pending
    let metrics = pgqrs::admin(&store)
        .queue_metrics(queue_name)
        .await
        .expect("Failed to get metrics");
    assert_eq!(metrics.total_messages, 2);
    assert_eq!(metrics.pending_messages, 2);
    assert_eq!(metrics.locked_messages, 0);
    assert_eq!(metrics.archived_messages, 0);

    // Consume 1 message (locks it)
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .fetch_all(&store)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    let msg_id = messages[0].id;

    // Check metrics: 1 pending, 1 locked
    let metrics = pgqrs::admin(&store)
        .queue_metrics(queue_name)
        .await
        .expect("Failed to get metrics");
    assert_eq!(metrics.total_messages, 2);
    assert_eq!(metrics.pending_messages, 1);
    assert_eq!(metrics.locked_messages, 1);
    assert_eq!(metrics.archived_messages, 0);

    // Archive the locked message
    consumer.archive(msg_id).await.unwrap();

    // Check metrics: 1 pending, 0 locked, 1 archived
    // Note: total_messages counts only active messages, so it's 1 after archiving

    let metrics = pgqrs::admin(&store)
        .queue_metrics(queue_name)
        .await
        .expect("Failed to get metrics");
    assert_eq!(metrics.total_messages, 1); // Only 1 left in active table
    assert_eq!(metrics.pending_messages, 1);
    assert_eq!(metrics.locked_messages, 0);
    assert_eq!(metrics.archived_messages, 1);

    // Check all_queues_metrics
    let all_metrics = pgqrs::admin(&store)
        .all_queues_metrics()
        .await
        .expect("Failed to get all metrics");
    let initial_len = all_metrics.len();
    assert!(initial_len >= 1);
    let my_metric = all_metrics
        .iter()
        .find(|m| m.name == queue_name)
        .expect("Queue not found in all metrics");
    assert_eq!(my_metric.pending_messages, 1);

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
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
async fn test_system_stats() {
    let store = create_store().await;
    let queue_name = "test_system_stats_queue";

    // Setup: Create queue, producer, consumer, and messages
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();
    let producer = pgqrs::producer("test_system_stats_producer", 3202, queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer = pgqrs::consumer("test_system_stats_consumer", 3203, queue_name)
        .create(&store)
        .await
        .unwrap();

    pgqrs::enqueue(&json!({"id": 1}))
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    pgqrs::dequeue()
        .worker(&*consumer)
        .fetch_all(&store)
        .await
        .unwrap(); // Make one locked

    // Fetch system stats
    let stats = pgqrs::admin(&store)
        .system_stats()
        .await
        .expect("Failed to get system stats");

    // Verify system stats
    assert!(stats.total_queues >= 1);
    assert!(stats.total_workers >= 2); // producer + consumer
    assert!(stats.active_workers >= 2);
    assert!(stats.total_messages >= 1);
    assert!(stats.locked_messages >= 1);
    assert!(!stats.schema_version.is_empty());

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
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
async fn test_worker_health_stats() {
    let store = create_store().await;
    let queue_name = "test_worker_health_queue";

    // Setup: Create queue
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    // Insert a stale worker manually
    // Using pgqrs_lib_stat_test schema
    sqlx::query(
        "INSERT INTO pgqrs_lib_stat_test.pgqrs_workers (queue_id, hostname, port, status, heartbeat_at)
         VALUES ($1, 'stale_worker', 9999, 'ready', NOW() - INTERVAL '1 hour')"
    )
    .bind(queue_info.id)
    .execute(store.pool())
    .await
    .unwrap();

    // Test Global Health
    let global_stats = pgqrs::admin(&store)
        .worker_health_stats(chrono::Duration::seconds(60), false)
        .await
        .unwrap();

    let global = global_stats
        .iter()
        .find(|s| s.queue_name == "Global")
        .unwrap();
    assert!(global.stale_workers >= 1);

    // Test Per-Queue Health
    let queue_stats = pgqrs::admin(&store)
        .worker_health_stats(chrono::Duration::seconds(60), true)
        .await
        .unwrap();

    let q_stat = queue_stats
        .iter()
        .find(|s| s.queue_name == queue_name)
        .unwrap();
    assert_eq!(q_stat.stale_workers, 1);
    assert_eq!(q_stat.total_workers, 1);

    // Cleanup
    sqlx::query("DELETE FROM pgqrs_lib_stat_test.pgqrs_workers WHERE hostname = 'stale_worker'")
        .execute(store.pool())
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_worker_stats() {
    let store = create_store().await;
    let queue_name = "test_worker_stats_queue";

    // Setup: Create queue
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    // Create 1 producer and 1 consumer
    let producer = pgqrs::producer("test_worker_stats_producer", 3204, queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer = pgqrs::consumer("test_worker_stats_consumer", 3205, queue_name)
        .create(&store)
        .await
        .unwrap();

    // Verify initial stats
    // Total workers: 2
    // Ready workers: 2
    let stats = pgqrs::admin(&store)
        .worker_stats(queue_name)
        .await
        .expect("Failed to get worker stats");
    assert_eq!(stats.total_workers, 2);
    assert_eq!(stats.ready_workers, 2);
    assert_eq!(stats.suspended_workers, 0);
    assert!(stats.average_messages_per_worker == 0.0);

    // Enqueue 1 message and dequeue (lock) it by consumer
    pgqrs::enqueue(&json!({"id": 1}))
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .fetch_all(&store)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);

    // Verify message stats
    // Total messages locked: 1
    // Total workers: 2
    // Avg: 0.5
    let stats = pgqrs::admin(&store)
        .worker_stats(queue_name)
        .await
        .expect("Failed to get worker stats");
    assert_eq!(stats.average_messages_per_worker, 0.5);

    // Suspend producer
    producer.suspend().await.unwrap();

    // Verify suspended stats
    // Total: 2
    // Ready: 1 (consumer)
    // Suspended: 1 (producer)
    let stats = pgqrs::admin(&store)
        .worker_stats(queue_name)
        .await
        .expect("Failed to get worker stats");
    assert_eq!(stats.total_workers, 2);
    assert_eq!(stats.ready_workers, 1);
    assert_eq!(stats.suspended_workers, 1);

    // Cleanup
    // Resume producer to delete it cleanly (though delete handles it, good practice)
    producer.resume().await.unwrap();

    // Purge queue to release/delete messages
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();

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
