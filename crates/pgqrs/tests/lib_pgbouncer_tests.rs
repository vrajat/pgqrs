#![cfg(feature = "postgres")]
use pgqrs::store::{AnyStore, Store};
use serde_json::json;

// Test-specific constants
const TEST_QUEUE_PGBOUNCER_HAPPY: &str = "test_pgbouncer_happy_path";
const TEST_QUEUE_PGBOUNCER_LIST: &str = "test_pgbouncer_list_path";

mod common;

async fn create_test_setup() -> AnyStore {
    let database_url = common::get_pgbouncer_dsn(Some("pgqrs_pgbouncer_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_pgbouncer_test")
        .expect("Failed to create config with pgbouncer test schema");

    pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to connect to pgbouncer")
}

#[tokio::test]
async fn test_pgbouncer_happy_path() {
    let store = create_test_setup().await;

    // Verify the installation works through PgBouncer
    pgqrs::admin(&store)
        .verify()
        .await
        .expect("Verify should succeed through PgBouncer");

    // Create a queue through PgBouncer
    let queue_result = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_PGBOUNCER_HAPPY)
        .await;

    if let Err(ref e) = queue_result {
        panic!("Failed to create queue through PgBouncer: {:?}", e);
    }
    let queue_info = queue_result.unwrap();

    // Create producer/consumer using the builder API
    let producer = pgqrs::producer("pgbouncer_test_producer", 3000, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer through PgBouncer");

    let consumer = pgqrs::consumer("pgbouncer_test_consumer", 3001, &queue_info.queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer through PgBouncer");

    // Send a message through PgBouncer
    let test_message = json!({
        "test": "data",
        "pgbouncer": true,
        "timestamp": "2023-01-01T00:00:00Z"
    });

    pgqrs::enqueue()
        .message(&test_message)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue message through PgBouncer");

    // Verify we have a pending message
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .expect("Failed to get pending count through PgBouncer");

    assert_eq!(pending_count, 1, "Should have exactly one pending message");

    // Read the message through PgBouncer
    let messages_list = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to read messages through PgBouncer");

    assert_eq!(messages_list.len(), 1, "Should receive exactly one message");

    let received_message = &messages_list[0];
    assert_eq!(
        received_message.payload, test_message,
        "Message content should match"
    );

    // Dequeue the message through PgBouncer
    consumer
        .delete(received_message.id)
        .await
        .expect("Failed to dequeue message through PgBouncer");

    // Verify the message is gone from the main queue
    let pending_count_after = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .expect("Failed to get pending count after dequeue");

    assert_eq!(
        pending_count_after, 0,
        "Queue should be empty after dequeuing"
    );

    let _ = producer.suspend().await;
    let _ = producer.shutdown().await;
    pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await
        .expect("Failed to delete producer worker");

    let _ = consumer.suspend().await;
    let _ = consumer.shutdown().await;
    pgqrs::admin(&store)
        .delete_worker(consumer.worker_id())
        .await
        .expect("Failed to delete consumer worker");

    let queue_info = pgqrs::admin(&store)
        .get_queue(TEST_QUEUE_PGBOUNCER_HAPPY)
        .await
        .expect("Failed to get queue info through PgBouncer");
    // Cleanup: delete the queue through PgBouncer
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue through PgBouncer");

    println!("PgBouncer happy path test completed successfully!");
}

#[tokio::test]
async fn test_pgbouncer_queue_list() {
    let store = create_test_setup().await;

    // Create a test queue
    // Recreate admin builder for each call as it consumes self
    let _queue = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_PGBOUNCER_LIST)
        .await
        .expect("Failed to create queue through PgBouncer");

    // Use store.queues() instead of manual instantiation
    let queues = store
        .queues()
        .list()
        .await
        .expect("Failed to list queues through PgBouncer");

    let found_queue = queues
        .iter()
        .find(|q| q.queue_name == TEST_QUEUE_PGBOUNCER_LIST);
    assert!(
        found_queue.is_some(),
        "Created queue should appear in queue list"
    );

    let queue_info = found_queue.unwrap();
    assert_eq!(queue_info.queue_name, TEST_QUEUE_PGBOUNCER_LIST);

    // Cleanup
    pgqrs::admin(&store)
        .delete_queue(queue_info)
        .await
        .expect("Failed to delete queue through PgBouncer");
}
