use pgqrs::{
    tables::{PgqrsMessages, PgqrsQueues},
    Consumer, PgqrsAdmin, Producer, Table,
};
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
    admin
        .verify()
        .await
        .expect("Verify should succeed through PgBouncer");

    // Create a queue through PgBouncer
    let queue_result = admin.create_queue(TEST_QUEUE_PGBOUNCER_HAPPY).await;
    let worker = admin
        .register(
            TEST_QUEUE_PGBOUNCER_HAPPY.to_string(),
            "http://localhost".to_string(),
            3000,
        )
        .await
        .expect("Failed to register worker through PgBouncer");

    if let Err(ref e) = queue_result {
        panic!("Failed to create queue through PgBouncer: {:?}", e);
    }
    let queue_info = queue_result.unwrap();
    let producer = Producer::new(admin.pool.clone(), &queue_info, &worker, &admin.config).unwrap();
    let consumer = Consumer::new(admin.pool.clone(), &queue_info, &worker, &admin.config).unwrap();
    let messages = PgqrsMessages::new(admin.pool.clone());

    // Send a message through PgBouncer
    let test_message = json!({
        "test": "data",
        "pgbouncer": true,
        "timestamp": "2023-01-01T00:00:00Z"
    });

    producer
        .enqueue(&test_message)
        .await
        .expect("Failed to enqueue message through PgBouncer");

    // Verify we have a pending message
    let pending_count = messages
        .count_pending(queue_info.id)
        .await
        .expect("Failed to get pending count through PgBouncer");

    assert_eq!(pending_count, 1, "Should have exactly one pending message");

    // Read the message through PgBouncer
    let messages_list = consumer
        .dequeue()
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
    let pending_count_after = messages
        .count_pending(queue_info.id)
        .await
        .expect("Failed to get pending count after dequeue");

    assert_eq!(
        pending_count_after, 0,
        "Queue should be empty after dequeuing"
    );

    let _ = admin.begin_shutdown(worker.id).await;
    let _ = admin.mark_stopped(worker.id).await;
    admin
        .delete_worker(worker.id)
        .await
        .expect("Failed to delete worker");

    let queue_info = admin
        .get_queue(TEST_QUEUE_PGBOUNCER_HAPPY)
        .await
        .expect("Failed to get queue info through PgBouncer");
    // Cleanup: delete the queue through PgBouncer
    admin
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue through PgBouncer");

    println!("PgBouncer happy path test completed successfully!");
}

#[tokio::test]
async fn test_pgbouncer_queue_list() {
    let admin = create_admin().await;

    // Create a test queue
    let _queue = admin
        .create_queue(TEST_QUEUE_PGBOUNCER_LIST)
        .await
        .expect("Failed to create queue through PgBouncer");

    let queue_obj = PgqrsQueues::new(admin.pool.clone());
    // List queues to verify it shows up
    let queues = queue_obj
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
    admin
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue through PgBouncer");
}
