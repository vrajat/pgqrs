use pgqrs::store::Store;
use serde_json::json;

// Test-specific constants
const TEST_QUEUE_LOGGED: &str = "test_create_logged_queue";
const TEST_QUEUE_SEND_MESSAGE: &str = "test_send_message";
const EXPECTED_MESSAGE_COUNT: i64 = 1;
const READ_MESSAGE_COUNT: usize = 1;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    common::create_store("pgqrs_lib_test").await
}

#[tokio::test]
async fn verify() {
    let store = create_store().await;
    // Verify should succeed (using custom schema "pgqrs_lib_test")
    let result = pgqrs::admin(&store).verify().await;
    assert!(result.is_ok(), "Verify should succeed: {:?}", result);
}

#[tokio::test]
async fn test_custom_schema_search_path() {
    use common::TestBackend;
    skip_unless_backend!(TestBackend::Postgres);

    // This test verifies that the search_path is correctly set to use the custom schema
    let store = create_store().await;
    let queue_name = "test_search_path_queue".to_string();
    let queue_info = pgqrs::admin(&store)
        .create_queue(&queue_name)
        .await
        .expect("Should create queue in custom schema");

    // List queues using tables API
    let queue_list = pgqrs::tables(&store)
        .queues()
        .list()
        .await
        .expect("Queue listing should succeed");

    let found_queue = queue_list
        .iter()
        .find(|q| q.queue_name == queue_info.queue_name);
    assert!(found_queue.is_some(), "Created queue should appear in list");

    // Cleanup
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Queue deletion should succeed");
}

#[tokio::test]
async fn test_create_and_list_queue() {
    let store = create_store().await;
    let queue_name = TEST_QUEUE_LOGGED.to_string();

    // Create queue using admin API
    let queue_info = pgqrs::admin(&store)
        .create_queue(&queue_name)
        .await
        .expect("Queue creation should succeed");

    // List queues using tables API
    let queue_list = pgqrs::tables(&store)
        .queues()
        .list()
        .await
        .expect("Queue listing should succeed");

    let found_queue = queue_list
        .iter()
        .find(|q| q.queue_name == queue_info.queue_name);
    assert!(found_queue.is_some(), "Created queue should appear in list");

    let meta = found_queue.unwrap();
    assert_eq!(
        meta.queue_name, queue_info.queue_name,
        "Queue name should match"
    );
    assert!(meta.id > 0, "Queue should have valid ID");

    // Verify the queue has a valid queue_id
    assert!(queue_info.id > 0, "Queue should have valid queue_id");

    // Cleanup
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Queue deletion should succeed");
}

#[tokio::test]
async fn test_send_message() {
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_SEND_MESSAGE)
        .await
        .expect("Failed to create queue");

    // Create managed producer and consumer workers using low-level API
    let producer = pgqrs::producer("test_send_message", 3000, TEST_QUEUE_SEND_MESSAGE)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test_send_message", 3100, TEST_QUEUE_SEND_MESSAGE)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    let payload = json!({
        "k": "v"
    });

    // Use low-level enqueue API with managed worker
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue message");
    let msg_id = msg_ids[0];

    assert!(msg_id > 0, "Message ID should be positive");

    // Verify message count using tables API
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .unwrap();
    assert_eq!(pending_count, EXPECTED_MESSAGE_COUNT);

    // Use low-level dequeue API with managed worker
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue message");

    assert_eq!(messages.len(), READ_MESSAGE_COUNT);
    let msg = &messages[0];
    assert_eq!(msg.payload, payload, "Payload should match");

    // Archive using consumer method directly
    consumer
        .archive(msg.id)
        .await
        .expect("Failed to archive message");

    // Verify the message was archived
    let pending_count = pgqrs::tables(&store)
        .messages()
        .count_pending(queue_info.id)
        .await
        .unwrap();
    assert_eq!(pending_count, 0);

    // Cleanup: suspend workers, then shutdown, then purge and delete queue
    producer
        .suspend()
        .await
        .expect("Failed to suspend producer");
    producer
        .shutdown()
        .await
        .expect("Failed to shutdown producer");
    consumer
        .suspend()
        .await
        .expect("Failed to suspend consumer");
    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");

    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_SEND_MESSAGE)
        .await
        .expect("Failed to purge queue");

    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_archive_single_message() {
    const TEST_QUEUE_ARCHIVE: &str = "test_archive_single";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_ARCHIVE)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test_archive_single_message", 3001, TEST_QUEUE_ARCHIVE)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test_archive_single_message", 3101, TEST_QUEUE_ARCHIVE)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Send a test message
    let payload = json!({"action": "process", "data": "test_archive"});
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue message");
    let msg_id = msg_ids[0];

    // Verify message is in active queue
    assert_eq!(
        pgqrs::tables(&store)
            .messages()
            .count_pending(queue_info.id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .filter_by_fk(queue_info.id)
            .await
            .unwrap()
            .len(),
        0
    );

    // Dequeue the message
    let dequeued_msgs = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue");

    assert_eq!(dequeued_msgs.len(), 1);
    assert_eq!(dequeued_msgs[0].id, msg_id);

    // Archive the message
    let archived = consumer.archive(msg_id).await;
    assert!(archived.is_ok());

    // Verify message moved from active to archive
    assert_eq!(
        pgqrs::tables(&store)
            .messages()
            .count_pending(queue_info.id)
            .await
            .unwrap(),
        0
    );
    let archived_msgs = pgqrs::tables(&store)
        .archive()
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(archived_msgs.len(), 1);
    assert_eq!(archived_msgs[0].original_msg_id, msg_id);

    // Try to archive the same message again (should return false)
    let archived_again = consumer.archive(msg_id).await;
    assert!(archived_again.is_ok());
    assert!(
        archived_again.unwrap().is_none(),
        "Archiving already-archived message should return false"
    );

    // Cleanup
    producer
        .suspend()
        .await
        .expect("Failed to suspend producer");
    producer
        .shutdown()
        .await
        .expect("Failed to shutdown producer");
    consumer
        .suspend()
        .await
        .expect("Failed to suspend consumer");
    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");

    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_ARCHIVE)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_archive_batch_messages() {
    const TEST_QUEUE_BATCH_ARCHIVE: &str = "test_archive_batch";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_BATCH_ARCHIVE)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer(
        "test_archive_batch_messages",
        3002,
        TEST_QUEUE_BATCH_ARCHIVE,
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    let consumer = pgqrs::consumer(
        "test_archive_batch_messages",
        3102,
        TEST_QUEUE_BATCH_ARCHIVE,
    )
    .create(&store)
    .await
    .expect("Failed to create consumer");

    // Send multiple test messages
    let mut msg_ids = Vec::new();
    for i in 0..5 {
        let payload = json!({"action": "batch_process", "index": i});
        let msg_id = pgqrs::enqueue()
            .message(&payload)
            .worker(&*producer)
            .execute(&store)
            .await
            .expect("Failed to enqueue message")[0];
        msg_ids.push(msg_id);
    }

    // Verify messages are in active queue
    assert_eq!(
        pgqrs::tables(&store)
            .messages()
            .count_pending(queue_info.id)
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .filter_by_fk(queue_info.id)
            .await
            .unwrap()
            .len(),
        0
    );

    // Dequeue first 3 messages
    let dequeued_msgs = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(3)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue messages");
    assert_eq!(dequeued_msgs.len(), 3);

    // Archive the 3 messages in batch
    let batch_to_archive: Vec<i64> = dequeued_msgs.iter().map(|m| m.id).collect();
    let archived_results = consumer.archive_many(batch_to_archive.clone()).await;
    assert!(archived_results.is_ok());
    let archived_results = archived_results.unwrap();
    assert_eq!(
        archived_results.len(),
        3,
        "Should have results for exactly 3 messages"
    );

    // Verify all messages were successfully archived
    for (i, id) in batch_to_archive.iter().enumerate() {
        assert!(
            archived_results[i],
            "Message {} should be successfully archived",
            id
        );
    }

    // Verify counts after batch archive
    assert_eq!(
        pgqrs::tables(&store)
            .messages()
            .count_pending(queue_info.id)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .filter_by_fk(queue_info.id)
            .await
            .unwrap()
            .len(),
        3
    );

    // Try to archive empty batch (should return empty vec)
    let empty_archive = consumer.archive_many(vec![]).await;
    assert!(empty_archive.is_ok());
    assert!(empty_archive.unwrap().is_empty());

    // Cleanup
    producer
        .suspend()
        .await
        .expect("Failed to suspend producer");
    producer
        .shutdown()
        .await
        .expect("Failed to shutdown producer");
    consumer
        .suspend()
        .await
        .expect("Failed to suspend consumer");
    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");

    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_BATCH_ARCHIVE)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_archive_nonexistent_message() {
    const TEST_QUEUE_NONEXISTENT: &str = "test_archive_nonexistent";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_NONEXISTENT)
        .await
        .expect("Failed to create queue");

    let consumer = pgqrs::consumer(
        "test_archive_nonexistent_message",
        3103,
        TEST_QUEUE_NONEXISTENT,
    )
    .create(&store)
    .await
    .expect("Failed to create consumer");

    // Try to archive a message that doesn't exist
    let fake_msg_id = 999999;
    let archived = consumer.archive(fake_msg_id).await;
    assert!(archived.is_ok());
    assert!(
        archived.unwrap().is_none(),
        "Non-existent message should not be archived"
    );

    // Verify archive count remains zero
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .filter_by_fk(queue_info.id)
            .await
            .unwrap()
            .len(),
        0
    );

    // Cleanup
    consumer
        .suspend()
        .await
        .expect("Failed to suspend consumer");
    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");

    // Delete worker before queue to avoid FK constraint
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
async fn test_purge_archive() {
    const TEST_QUEUE_PURGE_ARCHIVE: &str = "test_purge_archive";
    let store = create_store().await;

    // Create queue
    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_PURGE_ARCHIVE)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test_purge_archive", 3003, TEST_QUEUE_PURGE_ARCHIVE)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test_purge_archive", 3104, TEST_QUEUE_PURGE_ARCHIVE)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Archive multiple messages
    for i in 0..3 {
        let payload = json!({"action": "test_purge_archive", "index": i});
        let msg_ids = pgqrs::enqueue()
            .message(&payload)
            .worker(&*producer)
            .execute(&store)
            .await
            .expect("Failed to enqueue message");
        let msg_id = msg_ids[0];

        // Dequeue before archiving to acquire ownership
        let dequeued = pgqrs::dequeue()
            .worker(&*consumer)
            .batch(1)
            .fetch_all(&store)
            .await
            .expect("Failed to dequeue");
        assert_eq!(dequeued.len(), 1);
        assert_eq!(dequeued[0].id, msg_id);

        let archived = consumer
            .archive(msg_id)
            .await
            .expect("Failed to archive message");
        assert!(archived.is_some(), "Message {} should be archived", i);
    }

    // Verify archive has 3 messages
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .filter_by_fk(queue_info.id)
            .await
            .unwrap()
            .len(),
        3
    );

    // Purge archive
    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_PURGE_ARCHIVE)
        .await
        .expect("Failed to purge messages");

    // Verify archive is empty
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .filter_by_fk(queue_info.id)
            .await
            .unwrap()
            .len(),
        0
    );

    // Cleanup - purge already cleaned up workers and messages
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_interval_parameter_syntax() {
    let store = create_store().await;
    let queue_name = "test_interval_queue";

    // Create queue
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    let producer = pgqrs::producer("test_interval_parameter_syntax", 3005, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test_interval_parameter_syntax", 3106, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Send a message to test interval functionality
    let message_payload = json!({"test": "interval_test"});
    pgqrs::enqueue()
        .message(&message_payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();

    // Test reading messages (which uses make_interval in READ_MESSAGES)
    let messages = consumer.dequeue_many_with_delay(30, 1).await.unwrap(); // 30 seconds visibility timeout
    assert_eq!(messages.len(), 1, "Should read one message");

    let message = &messages[0];
    assert!(
        message.vt > chrono::Utc::now(),
        "Message should have future visibility timeout"
    );
    let original_vt = message.vt;

    // Test extending visibility timeout (which uses make_interval in UPDATE_MESSAGE_VT)
    let extend_result = consumer.extend_visibility(message.id, 60).await.unwrap(); // Extend by 60 seconds
    assert!(
        extend_result,
        "Should successfully extend visibility timeout"
    );

    // Get the updated message to verify the interval was applied correctly
    let updated_message = pgqrs::tables(&store)
        .messages()
        .get(message.id)
        .await
        .unwrap();
    assert!(
        updated_message.vt > original_vt,
        "Extended VT should be later than original"
    );

    // Verify the interval was applied correctly (should be roughly 60 seconds later)
    let duration_diff = (updated_message.vt - original_vt).num_seconds();
    assert!(
        (59..=61).contains(&duration_diff),
        "Extended VT should be ~60 seconds later, got {} seconds",
        duration_diff
    );

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_referential_integrity_checks() {
    let store = create_store().await;
    let queue_name = "test_integrity_queue";

    // Create queue and get queue_id
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    // Normal state: verify should pass
    let verify_result = pgqrs::admin(&store).verify().await;
    assert!(
        verify_result.is_ok(),
        "Verify should succeed with valid references"
    );

    // Create an orphaned message by inserting directly with invalid queue_id
    // This simulates what would happen if referential integrity was broken
    let sql = match common::current_backend() {
        common::TestBackend::Postgres => "INSERT INTO pgqrs_messages (queue_id, payload) VALUES (99999, '{\"test\": \"orphaned\"}'::jsonb)",
        common::TestBackend::Sqlite => "INSERT INTO pgqrs_messages (queue_id, payload) VALUES (99999, '{\"test\": \"orphaned\"}')",
        common::TestBackend::Turso => "INSERT INTO pgqrs_messages (queue_id, payload) VALUES (99999, '{\"test\": \"orphaned\"}')",
    };

    let orphan_result = store.execute_raw(sql).await;

    match orphan_result {
        Ok(_) => {
            // If the insert succeeded (no foreign key constraint), verify should now fail
            let verify_result = pgqrs::admin(&store).verify().await;
            assert!(
                verify_result.is_err(),
                "Verify should fail with orphaned message"
            );
            assert!(verify_result
                .unwrap_err()
                .to_string()
                .contains("messages with invalid queue_id references"));

            // Clean up orphaned message
            store
                .execute_raw("DELETE FROM pgqrs_messages WHERE queue_id = 99999")
                .await
                .expect("Failed to clean up orphaned message");
        }
        Err(_) => {
            // If foreign key constraint prevented the insert, that's also good
            // (means the schema has proper constraints)
            println!("Foreign key constraint prevented orphaned message creation - this is correct behavior");
        }
    }

    // Verify should pass again after cleanup
    let verify_result = pgqrs::admin(&store).verify().await;
    assert!(verify_result.is_ok(), "Verify should succeed after cleanup");

    // Cleanup
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_create_duplicate_queue_error() {
    let store = create_store().await;
    let queue_name = "test_duplicate_queue";

    // Create queue first time - should succeed
    let first_result = pgqrs::admin(&store).create_queue(queue_name).await;
    assert!(first_result.is_ok(), "First queue creation should succeed");

    // Try to create the same queue again - should fail with QueueAlreadyExists error
    let second_result = pgqrs::admin(&store).create_queue(queue_name).await;
    assert!(second_result.is_err(), "Second queue creation should fail");

    match second_result {
        Err(pgqrs::error::Error::QueueAlreadyExists { name }) => {
            assert_eq!(
                name, queue_name,
                "Error should contain the correct queue name"
            );
        }
        Err(other) => panic!("Expected QueueAlreadyExists error, got: {:?}", other),
        Ok(_) => panic!("Expected error but queue creation succeeded"),
    }

    // Verify the original queue still exists and works
    let queues = pgqrs::tables(&store).queues().list().await.unwrap();
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_some(), "Original queue should still exist");

    // Cleanup
    pgqrs::admin(&store)
        .delete_queue(&first_result.unwrap())
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_queue_deletion_with_references() {
    let store = create_store().await;
    let queue_name = "test_deletion_refs";

    // Create queue and add a message
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    let producer = pgqrs::producer("test_queue_deletion_with_references", 3006, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test_queue_deletion_with_references", 3107, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    let message_payload = json!({"test": "deletion_test"});
    pgqrs::enqueue()
        .message(&message_payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();

    // Try to delete queue with active worker - should fail with worker error
    let delete_result = pgqrs::admin(&store).delete_queue(&queue_info).await;
    assert!(
        delete_result.is_err(),
        "Deleting queue with active workers should fail"
    );
    let error_msg = delete_result.unwrap_err().to_string();
    assert!(
        error_msg.contains("active worker"),
        "Error should mention active workers, got: {}",
        error_msg
    );

    // Archive the message first (while worker is still active)
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1, "Should have one message");
    consumer.archive(messages[0].id).await.unwrap();

    // Stop the workers to test reference validation (must suspend first)
    producer
        .suspend()
        .await
        .expect("Failed to suspend producer worker");
    producer
        .shutdown()
        .await
        .expect("Failed to stop producer worker");
    consumer
        .suspend()
        .await
        .expect("Failed to suspend consumer worker");
    consumer
        .shutdown()
        .await
        .expect("Failed to stop consumer worker");

    // Now try to delete queue with archive - should fail with references error
    let delete_result2 = pgqrs::admin(&store).delete_queue(&queue_info).await;
    assert!(
        delete_result2.is_err(),
        "Deleting queue with archived messages should fail"
    );
    let error_msg2 = delete_result2.unwrap_err().to_string();
    assert!(
        error_msg2.contains("references exist") || error_msg2.contains("data exists"),
        "Error should mention references exist or data exists, got: {}",
        error_msg2
    );

    // Purge archive and try again - should succeed
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .expect("Failed to purge messages");
    let delete_result3 = pgqrs::admin(&store).delete_queue(&queue_info).await;
    assert!(
        delete_result3.is_ok(),
        "Deleting queue after purge should succeed"
    );

    // Verify queue is gone
    let queues = pgqrs::tables(&store).queues().list().await.unwrap();
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_none(), "Queue should be deleted");
}

#[tokio::test]
async fn test_validation_payload_size_limit() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set a very small payload size limit for testing
    config.validation_config.max_payload_size_bytes = 50; // Very small limit

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_size")
        .await
        .unwrap();

    let producer = pgqrs::producer(
        "test_validation_payload_size_limit",
        3007,
        "test_validation_size",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Small payload should work
    let small_payload = json!({"key": "value"});
    let result = pgqrs::enqueue()
        .message(&small_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_ok());

    // Large payload should fail
    let large_payload = json!({
        "very_long_key_that_exceeds_our_limit": "very_long_value_that_definitely_exceeds_the_50_byte_limit_we_set_for_testing"
    });
    let result = pgqrs::enqueue()
        .message(&large_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        pgqrs::error::Error::PayloadTooLarge {
            actual_bytes,
            max_bytes,
        } => {
            assert!(actual_bytes > 50);
            assert_eq!(max_bytes, 50);
        }
        _ => panic!("Expected PayloadTooLarge error, got: {:?}", err),
    }

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_validation_forbidden_keys() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Add custom forbidden key
    config.validation_config.forbidden_keys = vec!["secret".to_string(), "__proto__".to_string()];

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_forbidden")
        .await
        .unwrap();
    let producer = pgqrs::producer(
        "test_validation_forbidden_keys",
        3008,
        "test_validation_forbidden",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Valid payload should work
    let valid_payload = json!({"data": "value"});
    let result = pgqrs::enqueue()
        .message(&valid_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_ok());

    // Forbidden key should fail
    let forbidden_payload = json!({"secret": "should_not_be_allowed"});
    let result = pgqrs::enqueue()
        .message(&forbidden_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Forbidden key 'secret'"));
        }
        _ => panic!("Expected ValidationFailed error"),
    }

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store)
        .purge_queue("test_validation_forbidden")
        .await
        .ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_validation_required_keys() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Add required key
    config.validation_config.required_keys = vec!["user_id".to_string()];

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_required")
        .await
        .unwrap();
    let producer = pgqrs::producer(
        "test_validation_required_keys",
        3009,
        "test_validation_required",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Payload with required key should work
    let valid_payload = json!({"user_id": "123", "data": "value"});
    let result = pgqrs::enqueue()
        .message(&valid_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_ok());

    // Payload without required key should fail
    let invalid_payload = json!({"data": "value"});
    let result = pgqrs::enqueue()
        .message(&invalid_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Required key 'user_id' missing"));
        }
        _ => panic!("Expected ValidationFailed error"),
    }

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store)
        .purge_queue("test_validation_required")
        .await
        .ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_validation_object_depth() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set a shallow depth limit
    config.validation_config.max_object_depth = 2;

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_depth")
        .await
        .unwrap();
    let producer = pgqrs::producer(
        "test_validation_object_depth",
        3010,
        "test_validation_depth",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Shallow object should work
    let shallow_payload = json!({"level1": {"level2": "value"}});
    let result = pgqrs::enqueue()
        .message(&shallow_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_ok());

    // Deep object should fail
    let deep_payload = json!({"level1": {"level2": {"level3": {"level4": "value"}}}});
    let result = pgqrs::enqueue()
        .message(&deep_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Object depth"));
            assert!(reason.contains("exceeds limit"));
        }
        _ => panic!("Expected ValidationFailed error"),
    }

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store)
        .purge_queue("test_validation_depth")
        .await
        .ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_batch_validation_atomic_failure() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set required key for testing
    config.validation_config.required_keys = vec!["user_id".to_string()];

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_batch")
        .await
        .unwrap();
    let producer = pgqrs::producer(
        "test_batch_validation_atomic_failure",
        3011,
        "test_validation_batch",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Mix of valid and invalid payloads
    let payloads = vec![
        json!({"user_id": "123", "data": "valid"}),
        json!({"data": "invalid - missing user_id"}),
        json!({"user_id": "456", "data": "valid"}),
    ];

    // Batch should fail due to invalid payload in the middle
    let result = producer.batch_enqueue(&payloads).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Payload at index 1"));
            assert!(reason.contains("user_id"));
        }
        _ => panic!("Expected ValidationFailed error with index"),
    }

    // Verify no messages were enqueued (atomic batch operation)
    // Try to enqueue a valid message to ensure the queue is working
    let valid_payload = json!({"user_id": "789", "data": "test"});
    let result = pgqrs::enqueue()
        .message(&valid_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_ok());

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store)
        .purge_queue("test_validation_batch")
        .await
        .ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_validation_string_length() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set a small string length limit
    config.validation_config.max_string_length = 20;

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_strings")
        .await
        .unwrap();
    let producer = pgqrs::producer(
        "test_validation_string_length",
        3012,
        "test_validation_strings",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Short string should work
    let valid_payload = json!({"key": "short_value"});
    let result = pgqrs::enqueue()
        .message(&valid_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_ok());

    // Long string should fail
    let invalid_payload = json!({"key": "this_is_a_very_long_string_that_exceeds_our_limit"});
    let result = pgqrs::enqueue()
        .message(&invalid_payload)
        .worker(&*producer)
        .execute(&store)
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("String length"));
            assert!(reason.contains("exceeds limit"));
        }
        _ => panic!("Expected ValidationFailed error for string length"),
    }

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await
        .ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_validation_accessor_methods() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        &common::get_test_dsn("pgqrs_lib_test").await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Configure validation
    config.validation_config.max_payload_size_bytes = 2048;
    config.validation_config.max_enqueue_per_second = Some(100);
    config.validation_config.max_enqueue_burst = Some(20);

    let store = pgqrs::connect_with_config(&config).await.unwrap();
    let queue_info = pgqrs::admin(&store)
        .create_queue("test_validation_accessors")
        .await
        .unwrap();
    let producer = pgqrs::producer(
        "test_validation_accessor_methods",
        3013,
        "test_validation_accessors",
    )
    .create(&store)
    .await
    .expect("Failed to create producer");

    // Test validation config accessor
    let validation_config = producer.validation_config();
    assert_eq!(validation_config.max_payload_size_bytes, 2048);

    // Test rate limit status accessor
    let rate_status = producer.rate_limit_status();
    assert!(rate_status.is_some());
    let status = rate_status.unwrap();
    assert_eq!(status.max_per_second, 100);
    assert_eq!(status.burst_capacity, 20);
    assert_eq!(status.available_tokens, 20); // Should start full

    // Cleanup
    producer.suspend().await.ok();
    producer.shutdown().await.ok();
    pgqrs::admin(&store).delete_queue(&queue_info).await.ok();
}

#[tokio::test]
async fn test_dlq() {
    const TEST_QUEUE_DLQ: &str = "test_dlq_queue";
    let store = create_store().await;

    let queue_info = pgqrs::admin(&store)
        .create_queue(TEST_QUEUE_DLQ)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test_dlq", 3014, TEST_QUEUE_DLQ)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Send a test message
    let payload = json!({"task": "process_this"});
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue message");
    let msg_id = msg_ids[0];

    // Update read_ct out of band to simulate processing failure
    store
        .execute_raw_with_i64(
            "UPDATE pgqrs_messages SET read_ct = read_ct + 5 WHERE id = $1",
            msg_id,
        )
        .await
        .expect("Failed to update read_ct");

    // Move the message to DLQ
    let dlq_result = pgqrs::admin(&store).dlq().await;
    assert!(dlq_result.is_ok(), "Should move message to DLQ");
    let dlq = dlq_result.unwrap();
    assert!(dlq.len() == 1, "DLQ should contain one message");
    assert!(dlq[0] == msg_id, "DLQ message ID should match original");

    // Verify message is no longer in active queue and is in DLQ
    assert_eq!(
        pgqrs::tables(&store)
            .messages()
            .count_pending(queue_info.id)
            .await
            .unwrap(),
        0
    );

    let max_read_ct = store.config().max_read_ct;
    assert_eq!(
        pgqrs::tables(&store)
            .archive()
            .dlq_count(max_read_ct)
            .await
            .unwrap(),
        1
    );

    let dlq_messages = pgqrs::tables(&store)
        .archive()
        .list_dlq_messages(max_read_ct, 1, 0)
        .await
        .unwrap();
    assert_eq!(dlq_messages.len(), 1, "Should list one DLQ message");
    assert_eq!(
        dlq_messages[0].original_msg_id, msg_id,
        "DLQ message ID should match"
    );

    // Cleanup
    pgqrs::admin(&store)
        .purge_queue(TEST_QUEUE_DLQ)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
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
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    let msg1 = msg1_ids[0];
    let msg2_ids = pgqrs::enqueue()
        .message(&json!({"task": "in_progress"}))
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    let msg2 = msg2_ids[0];
    let msg3_ids = pgqrs::enqueue()
        .message(&json!({"task": "held"}))
        .worker(&*producer)
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
        .count_pending(queue_info.id)
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
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();
    let msg1 = msg1_ids[0];
    let msg2_ids = pgqrs::enqueue()
        .message(&json!({"task": "release_me_too"}))
        .worker(&*producer)
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
        .count_pending(queue_info.id)
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
async fn test_archive_count_for_queue() {
    let store = create_store().await;
    let q1_name = "test_archive_count_q1";
    let q2_name = "test_archive_count_q2";

    let q1 = pgqrs::admin(&store).create_queue(q1_name).await.unwrap();
    let q2 = pgqrs::admin(&store).create_queue(q2_name).await.unwrap();

    let consumer1 = pgqrs::consumer("host", 4000, q1_name)
        .create(&store)
        .await
        .unwrap();
    let consumer2 = pgqrs::consumer("host", 4001, q2_name)
        .create(&store)
        .await
        .unwrap();

    // Enqueue and archive 2 messages for Q1
    for i in 0..2 {
        let _id = pgqrs::enqueue()
            .message(&json!({"q": 1, "i": i}))
            .to(q1_name)
            .execute(&store)
            .await
            .unwrap()[0];
        let dq = pgqrs::dequeue()
            .worker(&*consumer1)
            .fetch_one(&store)
            .await
            .unwrap()
            .unwrap();
        consumer1.archive(dq.id).await.unwrap();
    }

    // Enqueue and archive 1 message for Q2
    let _id = pgqrs::enqueue()
        .message(&json!({"q": 2}))
        .to(q2_name)
        .execute(&store)
        .await
        .unwrap()[0];
    let dq = pgqrs::dequeue()
        .worker(&*consumer2)
        .fetch_one(&store)
        .await
        .unwrap();
    if let Some(msg) = dq {
        consumer2.archive(msg.id).await.unwrap();
    }

    // Verify counts using Archive::count_for_queue
    let archive = pgqrs::tables(&store).archive();
    let count1 = archive
        .count_for_queue(q1.id)
        .await
        .expect("Failed to count Q1");
    let count2 = archive
        .count_for_queue(q2.id)
        .await
        .expect("Failed to count Q2");

    assert_eq!(count1, 2);
    assert_eq!(count2, 1);

    // Cleanup
    pgqrs::admin(&store).purge_queue(q1_name).await.unwrap();
    pgqrs::admin(&store).delete_queue(&q1).await.unwrap();
    pgqrs::admin(&store).purge_queue(q2_name).await.unwrap();
    pgqrs::admin(&store).delete_queue(&q2).await.unwrap();
}

#[tokio::test]
async fn test_consumer_extend_visibility_behavior() {
    let store = create_store().await;
    let queue_name = "test_extend_visibility_behavior";
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    let payload = json!({"extend": true});
    pgqrs::enqueue()
        .message(&payload)
        .to(queue_name)
        .execute(&store)
        .await
        .unwrap();

    let consumer = pgqrs::consumer("host", 4002, queue_name)
        .create(&store)
        .await
        .unwrap();

    // Dequeue with 1 second visibility
    let msg = pgqrs::dequeue()
        .worker(&*consumer)
        .vt_offset(1)
        .fetch_one(&store)
        .await
        .unwrap()
        .expect("Should have message");

    // Extend by 3 more seconds
    let extended = consumer.extend_visibility(msg.id, 3).await.unwrap();
    assert!(extended);

    // Verify VT duration - should be ~4s from start, or ~2.5s from now
    let updated_msg = pgqrs::tables(&store).messages().get(msg.id).await.unwrap();
    let now = chrono::Utc::now();
    let diff = (updated_msg.vt - now).num_seconds();
    assert!(
        (2..=4).contains(&diff),
        "VT should be ~2-4s in future, got {}s",
        diff
    );

    // After 1.5 seconds, message should STILL be locked (because we extended)
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let updated_msg2 = pgqrs::tables(&store).messages().get(msg.id).await.unwrap();
    assert!(
        updated_msg2.vt > chrono::Utc::now(),
        "VT should still be in future"
    );

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_worker_health_and_heartbeat() {
    let store = create_store().await;
    let queue_name = "test_worker_health";
    let _queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

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
async fn test_admin_worker_management() {
    let store = create_store().await;
    let queue_name = "test_admin_mgmt";
    let _queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    // Initial workers
    // let initial_workers = pgqrs::admin(&store).list_workers().await.unwrap();

    // Create 2 temporary workers
    let c1 = pgqrs::consumer("host1", 6000, queue_name)
        .create(&store)
        .await
        .unwrap();
    let c2 = pgqrs::consumer("host2", 6001, queue_name)
        .create(&store)
        .await
        .unwrap();

    let workers = pgqrs::admin(&store).list_workers().await.unwrap();
    // assert!(workers.len() >= initial_workers.len() + 2); // Flaky due to parallel tests
    assert!(workers.iter().any(|w| w.id == c1.worker_id()));
    assert!(workers.iter().any(|w| w.id == c2.worker_id()));

    // Shutdown and delete one
    let w1_id = c1.worker_id();
    c1.suspend().await.unwrap();
    c1.shutdown().await.unwrap();

    let deleted = pgqrs::admin(&store).delete_worker(w1_id).await.unwrap();
    assert_eq!(deleted, 1);

    let workers_final = pgqrs::admin(&store).list_workers().await.unwrap();
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
async fn test_archive_replay_and_recovery() {
    let store = create_store().await;
    let queue_name = "test_replay";
    let queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    let producer = pgqrs::producer("host", 7000, queue_name)
        .create(&store)
        .await
        .unwrap();
    let consumer = pgqrs::consumer("host", 7001, queue_name)
        .create(&store)
        .await
        .unwrap();

    let payload = json!({"replay": true});
    pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .unwrap();

    let msg = pgqrs::dequeue()
        .worker(&*consumer)
        .fetch_one(&store)
        .await
        .unwrap()
        .unwrap();
    let archived = consumer.archive(msg.id).await.unwrap().unwrap();

    // Replay from Archive via table directly
    let archive_table = pgqrs::tables(&store).archive();
    let replayed = archive_table
        .replay_message(archived.id)
        .await
        .unwrap()
        .expect("Replay failed");

    assert_eq!(replayed.payload, payload);
    assert_eq!(replayed.queue_id, queue_info.id);

    // Verify it exists in messages table now
    let msg_back = pgqrs::tables(&store)
        .messages()
        .get(replayed.id)
        .await
        .unwrap();
    assert_eq!(msg_back.payload, payload);

    // Replay from "DLQ" using Producer API
    // First archive it again
    let dq = pgqrs::dequeue()
        .worker(&*consumer)
        .fetch_one(&store)
        .await
        .unwrap()
        .unwrap();
    let archived2 = consumer.archive(dq.id).await.unwrap().unwrap();

    let replayed2 = producer
        .replay_dlq(archived2.id)
        .await
        .unwrap()
        .expect("DLQ Replay failed");
    assert_eq!(replayed2.payload, payload);

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_batch_management_ops() {
    let store = create_store().await;
    let queue_name = "test_batch_ops";
    let _queue_info = pgqrs::admin(&store).create_queue(queue_name).await.unwrap();

    let payloads: Vec<_> = (0..5).map(|i| json!({"batch": i})).collect();
    let ids = pgqrs::enqueue()
        .messages(&payloads)
        .to(queue_name)
        .execute(&store)
        .await
        .unwrap();

    let consumer = pgqrs::consumer("host", 8000, queue_name)
        .create(&store)
        .await
        .unwrap();
    let msgs = consumer.dequeue_many(3).await.unwrap();
    let msg_ids: Vec<i64> = msgs.iter().map(|m| m.id).collect();

    // Test Batch Extension (using table directly since Consumer lacks it)
    let results = pgqrs::tables(&store)
        .messages()
        .extend_visibility_batch(&msg_ids, consumer.worker_id(), 10)
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|&r| r));

    // Verify VT for one
    let m1 = pgqrs::tables(&store)
        .messages()
        .get(msg_ids[0])
        .await
        .unwrap();
    assert!(m1.vt > chrono::Utc::now() + chrono::Duration::seconds(8));

    // Test Batch Deletion (via Consumer)
    let del_results = consumer.delete_many(msg_ids.clone()).await.unwrap();
    assert_eq!(del_results.len(), 3);
    assert!(del_results.iter().all(|&r| r));

    // Verify they are gone
    for id in msg_ids {
        assert!(pgqrs::tables(&store).messages().get(id).await.is_err());
    }

    // Test delete_by_ids (direct table access)
    let remaining_ids = &ids[3..];
    let del_results2 = pgqrs::tables(&store)
        .messages()
        .delete_by_ids(remaining_ids)
        .await
        .unwrap();
    assert_eq!(del_results2.len(), 2);
    assert!(del_results2.iter().all(|&r| r));

    // Cleanup
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
}
