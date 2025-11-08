use pgqrs::PgqrsAdmin;
use serde_json::json;

// Test-specific constants
const TEST_QUEUE_PGBOUNCER_HAPPY: &str = "test_pgbouncer_happy_path";
const TEST_QUEUE_PGBOUNCER_LIST: &str = "test_pgbouncer_list_path";

mod common;

async fn create_admin() -> pgqrs::admin::PgqrsAdmin {
    let database_url = common::get_pgbouncer_dsn(Some("pgqrs_pgbouncer_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_pgbouncer_test")
        .expect("Failed to create config with pgbouncer test schema");
    PgqrsAdmin::new(&config)
        .await
        .expect("Failed to create PgqrsAdmin")
}

#[tokio::test]
async fn test_pgbouncer_happy_path() {
    let admin = create_admin().await;

    // Verify the installation works through PgBouncer
    admin.verify().await.expect("Verify should succeed through PgBouncer");

    // Create a queue through PgBouncer
    let queue_result = admin
        .create_queue(TEST_QUEUE_PGBOUNCER_HAPPY, false)
        .await;

    if let Err(ref e) = queue_result {
        panic!("Failed to create queue through PgBouncer: {:?}", e);
    }
    let queue = queue_result.unwrap();

    // Send a message through PgBouncer
    let test_message = json!({
        "test": "data",
        "pgbouncer": true,
        "timestamp": "2023-01-01T00:00:00Z"
    });

    queue
        .enqueue(&test_message)
        .await
        .expect("Failed to enqueue message through PgBouncer");

    // Verify we have a pending message
    let pending_count = queue
        .pending_count()
        .await
        .expect("Failed to get pending count through PgBouncer");

    assert_eq!(pending_count, 1, "Should have exactly one pending message");

    // Read the message through PgBouncer
    let messages = queue
        .read(1)
        .await
        .expect("Failed to read messages through PgBouncer");

    assert_eq!(messages.len(), 1, "Should receive exactly one message");

    let received_message = &messages[0];
    assert_eq!(received_message.message, test_message, "Message content should match");

    // Dequeue the message through PgBouncer
    queue
        .dequeue(received_message.msg_id)
        .await
        .expect("Failed to dequeue message through PgBouncer");

    // Verify the message is gone from the main queue
    let pending_count_after = queue
        .pending_count()
        .await
        .expect("Failed to get pending count after dequeue");

    assert_eq!(pending_count_after, 0, "Queue should be empty after dequeuing");

    // Cleanup: delete the queue through PgBouncer
    admin
        .delete_queue(TEST_QUEUE_PGBOUNCER_HAPPY)
        .await
        .expect("Failed to delete queue through PgBouncer");

    println!("PgBouncer happy path test completed successfully!");
}

#[tokio::test]
async fn test_pgbouncer_queue_list() {
    let admin = create_admin().await;

    // Create a test queue
    let _queue = admin
        .create_queue(TEST_QUEUE_PGBOUNCER_LIST, false)
        .await
        .expect("Failed to create queue through PgBouncer");

    // List queues to verify it shows up
    let queues = admin
        .list_queues()
        .await
        .expect("Failed to list queues through PgBouncer");

    let found_queue = queues.iter().find(|q| q.queue_name == TEST_QUEUE_PGBOUNCER_LIST);
    assert!(found_queue.is_some(), "Created queue should appear in queue list");

    let queue_info = found_queue.unwrap();
    assert_eq!(queue_info.queue_name, TEST_QUEUE_PGBOUNCER_LIST);
    assert!(!queue_info.unlogged, "Queue should be logged");

    // Cleanup
    admin
        .delete_queue(TEST_QUEUE_PGBOUNCER_LIST)
        .await
        .expect("Failed to delete queue through PgBouncer");
}