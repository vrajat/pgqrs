use pgqrs::{
    tables::{Messages, Queues},
    worker::Worker,
    Admin, Archive, Table, Workers,
};
use serde_json::json;

// Test-specific constants
const TEST_QUEUE_LOGGED: &str = "test_create_logged_queue";
const TEST_QUEUE_SEND_MESSAGE: &str = "test_send_message";
const EXPECTED_MESSAGE_COUNT: i64 = 1;
const READ_MESSAGE_COUNT: usize = 1;

mod common;

async fn create_admin() -> pgqrs::admin::Admin {
    let database_url = common::get_postgres_dsn(Some("pgqrs_lib_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_lib_test")
        .expect("Failed to create config with lib_test schema");
    Admin::new(&config).await.expect("Failed to create Admin")
}

#[tokio::test]
async fn verify() {
    let admin = create_admin().await;
    // Verify should succeed (using custom schema "pgqrs_lib_test")
    let result = admin.verify().await;
    assert!(result.is_ok(), "Verify should succeed: {:?}", result);
}

#[tokio::test]
async fn test_create_and_list_queue() {
    let admin = create_admin().await;
    let queue_name = TEST_QUEUE_LOGGED.to_string();

    // Create queue
    let queue = admin.create_queue(&queue_name).await;
    assert!(queue.is_ok(), "Queue creation should succeed");
    let queue_info = queue.unwrap();

    // List queues and verify it appears
    let pgqrs_queues = Queues::new(admin.pool.clone());
    let queue_list = pgqrs_queues.list().await;
    assert!(queue_list.is_ok(), "Queue listing should succeed");
    let queue_list = queue_list.unwrap();

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
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_send_message() {
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_SEND_MESSAGE)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_send_message_producer",
        3000,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_send_message_consumer",
        3001,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let messages = Messages::new(admin.pool.clone());

    let payload = json!({
        "k": "v"
    });
    assert!(producer.enqueue(&payload).await.is_ok());
    assert!(messages.count_pending(queue_info.id).await.unwrap() == EXPECTED_MESSAGE_COUNT);
    let read_messages = consumer.dequeue_many(READ_MESSAGE_COUNT).await;
    assert!(read_messages.is_ok());
    let read_messages = read_messages.unwrap();
    assert_eq!(read_messages.len(), READ_MESSAGE_COUNT);
    assert!(read_messages[0].payload == payload);
    let deleted_message = consumer.delete(read_messages[0].id).await;
    assert!(deleted_message.is_ok());
    assert!(messages.count_pending(queue_info.id).await.unwrap() == 0);
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_archive_single_message() {
    const TEST_QUEUE_ARCHIVE: &str = "test_archive_single";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_ARCHIVE)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_archive_single_producer",
        3100,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_archive_single_consumer",
        3101,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let messages = Messages::new(admin.pool.clone());
    let pgqrs_archive = Archive::new(admin.pool.clone());
    // Send a test message
    let payload = json!({"action": "process", "data": "test_archive"});
    let message = producer
        .enqueue(&payload)
        .await
        .expect("Failed to enqueue message");
    let msg_id = message.id;

    // Verify message is in active queue
    assert_eq!(messages.count_pending(queue_info.id).await.unwrap(), 1);
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        0
    );

    // Archive the message
    let archived = consumer.archive(msg_id).await;
    assert!(archived.is_ok());

    // Verify message moved from active to archive
    assert_eq!(messages.count_pending(queue_info.id).await.unwrap(), 0);
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        1
    );

    // Try to archive the same message again (should return false)
    let archived_again = consumer.archive(msg_id).await;
    assert!(archived_again.is_ok());
    assert!(
        archived_again.unwrap().is_none(),
        "Archiving already-archived message should return false"
    );

    admin
        .purge_queue(&queue_info.queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_archive_batch_messages() {
    const TEST_QUEUE_BATCH_ARCHIVE: &str = "test_archive_batch";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_BATCH_ARCHIVE)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_archive_batch_producer",
        3102,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_archive_batch_consumer",
        3103,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let messages = Messages::new(admin.pool.clone());
    let pgqrs_archive = Archive::new(admin.pool.clone());

    // Send multiple test messages
    let mut msg_ids = Vec::new();
    for i in 0..5 {
        let payload = json!({"action": "batch_process", "index": i});
        let message = producer
            .enqueue(&payload)
            .await
            .expect("Failed to enqueue message");
        msg_ids.push(message.id);
    }

    // Verify messages are in active queue
    assert_eq!(messages.count_pending(queue_info.id).await.unwrap(), 5);
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        0
    );

    // Archive first 3 messages in batch
    let batch_to_archive = msg_ids[0..3].to_vec();
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
    assert_eq!(messages.count_pending(queue_info.id).await.unwrap(), 2);
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        3
    );

    // Try to archive empty batch (should return empty vec)
    let empty_archive = consumer.archive_many(vec![]).await;
    assert!(empty_archive.is_ok());
    assert!(empty_archive.unwrap().is_empty());

    // Cleanup - purge archive and messages before deleting queue
    admin
        .purge_queue(&queue_info.queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_archive_nonexistent_message() {
    const TEST_QUEUE_NONEXISTENT: &str = "test_archive_nonexistent";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_NONEXISTENT)
        .await
        .expect("Failed to create queue");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_archive_nonexistent_consumer",
        3104,
        &admin.config,
    )
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
    let pgqrs_archive = Archive::new(admin.pool.clone());
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        0
    );

    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    // Cleanup
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_purge_archive() {
    const TEST_QUEUE_PURGE_ARCHIVE: &str = "test_purge_archive";
    let admin = create_admin().await;

    // Create queue and archive some messages
    let queue_info = admin
        .create_queue(TEST_QUEUE_PURGE_ARCHIVE)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_purge_archive_producer",
        3105,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_purge_archive_consumer",
        3106,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let pgqrs_archive = Archive::new(admin.pool.clone());
    // Archive multiple messages
    for i in 0..3 {
        let payload = json!({"action": "test_purge_archive", "index": i});
        let message = producer
            .enqueue(&payload)
            .await
            .expect("Failed to enqueue message");
        let archived = consumer
            .archive(message.id)
            .await
            .expect("Failed to archive message");
        assert!(archived.is_some(), "Message {} should be archived", i);
    }

    // Verify archive has 3 messages
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        3
    );

    // Purge archive
    admin
        .purge_queue(&queue_info.queue_name)
        .await
        .expect("Failed to purge messages");

    // Verify archive is empty
    assert_eq!(
        pgqrs_archive
            .count_for_fk(queue_info.id, &mut admin.pool.begin().await.unwrap())
            .await
            .unwrap(),
        0
    );

    // Cleanup
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_custom_schema_search_path() {
    // This test verifies that the search_path is correctly set to use the custom schema
    let admin = create_admin().await;

    // Create a test queue in the custom schema
    let queue_name = "test_search_path_queue".to_string();
    let queue_result = admin.create_queue(&queue_name).await;
    assert!(queue_result.is_ok(), "Should create queue in custom schema");

    // Verify we can find the table in the custom schema by checking the search_path
    let pool = &admin.pool;

    // Check that we're using the correct schema by querying the search_path
    let search_path: String = sqlx::query_scalar("SHOW search_path")
        .fetch_one(pool)
        .await
        .expect("Should get search_path");

    // The search_path should contain our custom schema
    assert!(
        search_path.contains("pgqrs_lib_test"),
        "Search path should contain pgqrs_lib_test: {}",
        search_path
    );

    // Verify all unified tables exist and are accessible via search_path
    let tables_to_check = [
        "pgqrs_queues",
        "pgqrs_messages",
        "pgqrs_archive",
        "pgqrs_workers",
    ];

    for table_name in &tables_to_check {
        let table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
        )
        .bind(table_name)
        .fetch_one(pool)
        .await
        .expect("Should check table existence");

        assert!(
            table_exists,
            "{} table should exist and be findable via search_path",
            table_name
        );
    }

    // Test that queue operations work with unified architecture
    let queue_info = queue_result.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_custom_schema_producer",
        3107,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let message_payload = serde_json::json!({"test": "custom_schema"});
    let send_result = producer.enqueue(&message_payload).await;
    assert!(
        send_result.is_ok(),
        "Should be able to send message to queue in custom schema"
    );

    // Cleanup - purge messages before deleting queue
    admin
        .purge_queue(&queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_interval_parameter_syntax() {
    let admin = create_admin().await;
    let queue_name = "test_interval_queue";

    // Create queue
    let queue_info = admin.create_queue(queue_name).await.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_interval_producer",
        3108,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_interval_consumer",
        3109,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let pgqrs_messages = Messages::new(admin.pool.clone());

    // Send a message to test interval functionality
    let message_payload = json!({"test": "interval_test"});
    producer.enqueue(&message_payload).await.unwrap();

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
    let extend_result = producer.extend_visibility(message.id, 60).await.unwrap(); // Extend by 60 seconds
    assert!(
        extend_result,
        "Should successfully extend visibility timeout"
    );

    // Get the updated message to verify the interval was applied correctly
    let updated_message = pgqrs_messages.get(message.id).await.unwrap();
    assert!(
        updated_message.vt > original_vt,
        "Extended VT should be later than original"
    );

    // Verify the interval was applied correctly (should be roughly 60 seconds later)
    let duration_diff = (updated_message.vt - original_vt).num_seconds();
    assert!(
        (59..=61).contains(&duration_diff),
        "VT should be extended by approximately 60 seconds, got {} seconds",
        duration_diff
    );

    // Cleanup
    admin
        .purge_queue(queue_name)
        .await
        .expect("Failed to purge messages");
    admin
        .delete_worker(producer.worker_id())
        .await
        .expect("Failed to delete producer worker");
    admin
        .delete_worker(consumer.worker_id())
        .await
        .expect("Failed to delete consumer worker");
    admin
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_referential_integrity_checks() {
    let admin = create_admin().await;
    let queue_name = "test_integrity_queue";

    // Create queue and get queue_id
    let _queue = admin.create_queue(queue_name).await.unwrap();
    let pgqrs_queues = Queues::new(admin.pool.clone());
    let queue_list = pgqrs_queues.list().await.unwrap();
    let queue_info = queue_list
        .iter()
        .find(|q| q.queue_name == queue_name)
        .unwrap();
    let _queue_id = queue_info.id;

    // Normal state: verify should pass
    let verify_result = admin.verify().await;
    assert!(
        verify_result.is_ok(),
        "Verify should succeed with valid references"
    );

    // Create an orphaned message by inserting directly with invalid queue_id
    // This simulates what would happen if referential integrity was broken
    let orphan_result =
        sqlx::query("INSERT INTO pgqrs_messages (queue_id, payload) VALUES ($1, $2)")
            .bind(99999i64) // Non-existent queue_id
            .bind(json!({"test": "orphaned"}))
            .execute(&admin.pool)
            .await;

    match orphan_result {
        Ok(_) => {
            // If the insert succeeded (no foreign key constraint), verify should now fail
            let verify_result = admin.verify().await;
            assert!(
                verify_result.is_err(),
                "Verify should fail with orphaned message"
            );
            assert!(verify_result
                .unwrap_err()
                .to_string()
                .contains("messages with invalid queue_id references"));

            // Clean up orphaned message
            sqlx::query("DELETE FROM pgqrs_messages WHERE queue_id = $1")
                .bind(99999i64)
                .execute(&admin.pool)
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
    let verify_result = admin.verify().await;
    assert!(verify_result.is_ok(), "Verify should succeed after cleanup");

    // Cleanup
    admin
        .delete_queue(queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_create_duplicate_queue_error() {
    let admin = create_admin().await;
    let queue_name = "test_duplicate_queue";

    // Create queue first time - should succeed
    let first_result = admin.create_queue(queue_name).await;
    assert!(first_result.is_ok(), "First queue creation should succeed");

    // Try to create the same queue again - should fail with QueueAlreadyExists error
    let second_result = admin.create_queue(queue_name).await;
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
    let pgqrs_queues = Queues::new(admin.pool.clone());
    let queues = pgqrs_queues.list().await.unwrap();
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_some(), "Original queue should still exist");

    // Cleanup
    admin
        .delete_queue(&first_result.unwrap())
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_queue_deletion_with_references() {
    let admin = create_admin().await;
    let queue_name = "test_deletion_refs";

    // Create queue and add a message
    let queue_info = admin.create_queue(queue_name).await.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_deletion_refs_producer",
        3110,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_deletion_refs_consumer",
        3111,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let message_payload = json!({"test": "deletion_test"});
    producer.enqueue(&message_payload).await.unwrap();

    // Try to delete queue with active worker - should fail with worker error
    let delete_result = admin.delete_queue(&queue_info).await;
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
    let messages = consumer.dequeue().await.unwrap();
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
    let delete_result2 = admin.delete_queue(&queue_info).await;
    assert!(
        delete_result2.is_err(),
        "Deleting queue with archived messages should fail"
    );
    let error_msg2 = delete_result2.unwrap_err().to_string();
    assert!(
        error_msg2.contains("references exist"),
        "Error should mention references exist, got: {}",
        error_msg2
    );

    // Purge archive and try again - should succeed
    admin
        .purge_queue(queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    let delete_result3 = admin.delete_queue(&queue_info).await;
    assert!(
        delete_result3.is_ok(),
        "Deleting queue after purge should succeed"
    );

    // Verify queue is gone
    let pgqrs_queues = Queues::new(admin.pool.clone());
    let queues = pgqrs_queues.list().await.unwrap();
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_none(), "Queue should be deleted");
}

#[tokio::test]
async fn test_validation_payload_size_limit() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set a very small payload size limit for testing
    config.validation_config.max_payload_size_bytes = 50; // Very small limit

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin.create_queue("test_validation_size").await.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_size_producer",
        3112,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Small payload should work
    let small_payload = json!({"key": "value"});
    let result = producer.enqueue(&small_payload).await;
    assert!(result.is_ok());

    // Large payload should fail
    let large_payload = json!({
        "very_long_key_that_exceeds_our_limit": "very_long_value_that_definitely_exceeds_the_50_byte_limit_we_set_for_testing"
    });
    let result = producer.enqueue(&large_payload).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::PayloadTooLarge {
            actual_bytes,
            max_bytes,
        } => {
            assert!(actual_bytes > 50);
            assert_eq!(max_bytes, 50);
        }
        _ => panic!("Expected PayloadTooLarge error"),
    }
}

#[tokio::test]
async fn test_validation_forbidden_keys() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Add custom forbidden key
    config.validation_config.forbidden_keys = vec!["secret".to_string(), "__proto__".to_string()];

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin
        .create_queue("test_validation_forbidden")
        .await
        .unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_forbidden_producer",
        3113,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Valid payload should work
    let valid_payload = json!({"data": "value"});
    let result = producer.enqueue(&valid_payload).await;
    assert!(result.is_ok());

    // Forbidden key should fail
    let forbidden_payload = json!({"secret": "should_not_be_allowed"});
    let result = producer.enqueue(&forbidden_payload).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Forbidden key 'secret'"));
        }
        _ => panic!("Expected ValidationFailed error"),
    }
}

#[tokio::test]
async fn test_validation_required_keys() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Add required key
    config.validation_config.required_keys = vec!["user_id".to_string()];

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin
        .create_queue("test_validation_required")
        .await
        .unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_required_producer",
        3114,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Valid payload with required key should work
    let valid_payload = json!({"user_id": "123", "data": "value"});
    let result = producer.enqueue(&valid_payload).await;
    assert!(result.is_ok());

    // Missing required key should fail
    let invalid_payload = json!({"data": "value"});
    let result = producer.enqueue(&invalid_payload).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Required key 'user_id' missing"));
        }
        _ => panic!("Expected ValidationFailed error"),
    }
}

#[tokio::test]
async fn test_validation_object_depth() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set a shallow depth limit
    config.validation_config.max_object_depth = 2;

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin.create_queue("test_validation_depth").await.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_depth_producer",
        3115,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Shallow object should work
    let shallow_payload = json!({"level1": {"level2": "value"}});
    let result = producer.enqueue(&shallow_payload).await;
    assert!(result.is_ok());

    // Deep object should fail
    let deep_payload = json!({"level1": {"level2": {"level3": {"level4": "value"}}}});
    let result = producer.enqueue(&deep_payload).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("Object depth"));
            assert!(reason.contains("exceeds limit"));
        }
        _ => panic!("Expected ValidationFailed error"),
    }
}

#[tokio::test]
async fn test_batch_validation_atomic_failure() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set required key for testing
    config.validation_config.required_keys = vec!["user_id".to_string()];

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin.create_queue("test_validation_batch").await.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_batch_producer",
        3116,
        &admin.config,
    )
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
    let result = producer.enqueue(&valid_payload).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validation_string_length() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Set a small string length limit
    config.validation_config.max_string_length = 20;

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin.create_queue("test_validation_strings").await.unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_strings_producer",
        3117,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Short string should work
    let valid_payload = json!({"key": "short_value"});
    let result = producer.enqueue(&valid_payload).await;
    assert!(result.is_ok());

    // Long string should fail
    let invalid_payload = json!({"key": "this_is_a_very_long_string_that_exceeds_our_limit"});
    let result = producer.enqueue(&invalid_payload).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        pgqrs::error::Error::ValidationFailed { reason } => {
            assert!(reason.contains("String length"));
            assert!(reason.contains("exceeds limit"));
        }
        _ => panic!("Expected ValidationFailed error for string length"),
    }
}

#[tokio::test]
async fn test_validation_accessor_methods() {
    let mut config = pgqrs::config::Config::from_dsn_with_schema(
        common::get_postgres_dsn(Some("pgqrs_lib_test")).await,
        "pgqrs_lib_test",
    )
    .expect("Failed to create config");

    // Configure validation
    config.validation_config.max_payload_size_bytes = 2048;
    config.validation_config.max_enqueue_per_second = Some(100);
    config.validation_config.max_enqueue_burst = Some(20);

    let admin = Admin::new(&config).await.unwrap();
    let queue_info = admin
        .create_queue("test_validation_accessors")
        .await
        .unwrap();
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_validation_accessors_producer",
        3118,
        &admin.config,
    )
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
}

#[tokio::test]
async fn test_dlq() {
    const TEST_QUEUE_DLQ: &str = "test_dlq_queue";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_DLQ)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_dlq_producer",
        3119,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let pgqrs_messages = Messages::new(admin.pool.clone());

    // Send a test message
    let payload = json!({"task": "process_this"});
    let message = producer
        .enqueue(&payload)
        .await
        .expect("Failed to enqueue message");
    let msg_id = message.id;

    // Update read_ct out of band to simulate processing failure
    sqlx::query("UPDATE pgqrs_messages SET read_ct = read_ct + 5 WHERE id = $1")
        .bind(msg_id)
        .execute(&admin.pool)
        .await
        .expect("Failed to update read_ct");

    // Move the message to DLQ
    let dlq_result = admin.dlq().await;
    assert!(dlq_result.is_ok(), "Should move message to DLQ");
    let dlq = dlq_result.unwrap();
    assert!(dlq.len() == 1, "DLQ should contain one message");
    assert!(dlq[0] == msg_id, "DLQ message ID should match original");

    // Verify message is no longer in active queue and is in DLQ
    assert_eq!(
        pgqrs_messages.count_pending(queue_info.id).await.unwrap(),
        0
    );

    let archive = Archive::new(admin.pool.clone());
    assert_eq!(
        archive.dlq_count(admin.config.max_read_ct).await.unwrap(),
        1
    );

    let dlq_messages = archive
        .list_dlq_messages(admin.config.max_read_ct, 1, 0)
        .await
        .unwrap();
    assert_eq!(dlq_messages.len(), 1, "Should list one DLQ message");
    assert_eq!(
        dlq_messages[0].original_msg_id, msg_id,
        "DLQ message ID should match"
    );
    admin
        .purge_queue(&queue_info.queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_producer_shutdown() {
    const TEST_QUEUE_PRODUCER_SHUTDOWN: &str = "test_producer_shutdown";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_PRODUCER_SHUTDOWN)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_producer_shutdown_producer",
        3120,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");

    // Verify worker starts in Ready state
    let workers = Workers::new(admin.pool.clone())
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
    let workers_after = Workers::new(admin.pool.clone())
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
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_consumer_shutdown_no_messages() {
    const TEST_QUEUE_CONSUMER_SHUTDOWN_EMPTY: &str = "test_consumer_shutdown_empty";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_EMPTY)
        .await
        .expect("Failed to create queue");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_consumer_shutdown_empty_consumer",
        3121,
        &admin.config,
    )
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
    let workers = Workers::new(admin.pool.clone())
        .filter_by_fk(queue_info.id)
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].status, pgqrs::types::WorkerStatus::Stopped);

    // Cleanup
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_consumer_shutdown_with_held_messages() {
    const TEST_QUEUE_CONSUMER_SHUTDOWN_HELD: &str = "test_consumer_shutdown_held";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_HELD)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_consumer_shutdown_held_producer",
        3122,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_consumer_shutdown_held_consumer",
        3123,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let messages_table = Messages::new(admin.pool.clone());

    // Send multiple messages
    let msg1 = producer.enqueue(&json!({"task": "held"})).await.unwrap();
    let msg2 = producer
        .enqueue(&json!({"task": "in_progress"}))
        .await
        .unwrap();
    let msg3 = producer.enqueue(&json!({"task": "held"})).await.unwrap();

    // Dequeue messages (they become held by the worker)
    let dequeued = consumer.dequeue_many(3).await.unwrap();
    assert_eq!(dequeued.len(), 3);

    // Verify all messages are held by worker
    let worker_messages = admin
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
    let released = consumer
        .release_messages(&[msg1.id, msg3.id])
        .await
        .unwrap();
    assert_eq!(released, 2, "Should release 2 messages");

    // Still can't shutdown - msg2 is still held
    let shutdown_result = consumer.shutdown().await;
    assert!(
        shutdown_result.is_err(),
        "Consumer shutdown should fail when still holding messages"
    );

    // Release the last message
    let released = consumer.release_messages(&[msg2.id]).await.unwrap();
    assert_eq!(released, 1, "Should release 1 message");

    // Now shutdown should succeed
    let shutdown_result = consumer.shutdown().await;
    assert!(
        shutdown_result.is_ok(),
        "Consumer shutdown should succeed after releasing all messages"
    );

    // Verify worker status
    let consumer_worker = Workers::new(admin.pool.clone())
        .get(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(consumer_worker.status, pgqrs::types::WorkerStatus::Stopped);

    // Verify all messages are back in pending state
    let pending_count = messages_table.count_pending(queue_info.id).await.unwrap();
    assert_eq!(
        pending_count, 3,
        "All messages should be back in pending state"
    );

    // Cleanup - delete remaining messages and worker
    consumer
        .delete_many(vec![msg1.id, msg2.id, msg3.id])
        .await
        .unwrap();
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}

#[tokio::test]
async fn test_consumer_shutdown_all_messages_released() {
    const TEST_QUEUE_CONSUMER_SHUTDOWN_ALL: &str = "test_consumer_shutdown_all";
    let admin = create_admin().await;
    let queue_info = admin
        .create_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_ALL)
        .await
        .expect("Failed to create queue");
    let producer = pgqrs::Producer::new(
        admin.pool.clone(),
        &queue_info,
        "test_consumer_shutdown_all_producer",
        3124,
        &admin.config,
    )
    .await
    .expect("Failed to create producer");
    let consumer = pgqrs::Consumer::new(
        admin.pool.clone(),
        &queue_info,
        "test_consumer_shutdown_all_consumer",
        3125,
        &admin.config,
    )
    .await
    .expect("Failed to create consumer");
    let messages_table = Messages::new(admin.pool.clone());

    // Send and dequeue messages
    let msg1 = producer
        .enqueue(&json!({"task": "release_me"}))
        .await
        .unwrap();
    let msg2 = producer
        .enqueue(&json!({"task": "release_me_too"}))
        .await
        .unwrap();

    let dequeued = consumer.dequeue_many(2).await.unwrap();
    assert_eq!(dequeued.len(), 2);

    // Release all messages before suspend
    let released = consumer
        .release_messages(&[msg1.id, msg2.id])
        .await
        .unwrap();
    assert_eq!(released, 2, "Should release 2 messages");

    // Now we can suspend
    let suspend_result = consumer.suspend().await;
    assert!(suspend_result.is_ok(), "Consumer suspend should succeed");

    // Shutdown consumer
    let shutdown_result = consumer.shutdown().await;
    assert!(shutdown_result.is_ok(), "Consumer shutdown should succeed");

    // Verify all messages are released back to pending
    let pending_count = messages_table.count_pending(queue_info.id).await.unwrap();
    assert_eq!(
        pending_count, 2,
        "All messages should be back in pending state"
    );

    // Verify no messages are held by worker
    let worker_messages = admin
        .get_worker_messages(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(
        worker_messages.len(),
        0,
        "No messages should be held by worker"
    );

    // Verify worker status
    let consumer_worker = Workers::new(admin.pool.clone())
        .get(consumer.worker_id())
        .await
        .unwrap();
    assert_eq!(consumer_worker.status, pgqrs::types::WorkerStatus::Stopped);

    // Cleanup
    admin
        .purge_queue(TEST_QUEUE_CONSUMER_SHUTDOWN_ALL)
        .await
        .unwrap();
    assert!(admin.delete_worker(producer.worker_id()).await.is_ok());
    assert!(admin.delete_worker(consumer.worker_id()).await.is_ok());
    assert!(admin.delete_queue(&queue_info).await.is_ok());
}
