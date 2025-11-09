use pgqrs::PgqrsAdmin;
use serde_json::json;

// Test-specific constants
const TEST_QUEUE_LOGGED: &str = "test_create_logged_queue";
const TEST_QUEUE_SEND_MESSAGE: &str = "test_send_message";
const EXPECTED_MESSAGE_COUNT: i64 = 1;
const READ_MESSAGE_COUNT: usize = 1;

mod common;

async fn create_admin() -> pgqrs::admin::PgqrsAdmin {
    let database_url = common::get_postgres_dsn(Some("pgqrs_lib_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_lib_test")
        .expect("Failed to create config with lib_test schema");
    PgqrsAdmin::new(&config)
        .await
        .expect("Failed to create PgqrsAdmin")
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
    let queue = queue.unwrap();

    // List queues and verify it appears
    let queue_list = admin.list_queues().await;
    assert!(queue_list.is_ok(), "Queue listing should succeed");
    let queue_list = queue_list.unwrap();

    let found_queue = queue_list.iter().find(|q| q.queue_name == queue.queue_name);
    assert!(found_queue.is_some(), "Created queue should appear in list");

    let meta = found_queue.unwrap();
    assert_eq!(meta.queue_name, queue.queue_name, "Queue name should match");
    assert!(meta.id > 0, "Queue should have valid ID");

    // Verify the queue has a valid queue_id
    assert!(queue.queue_id > 0, "Queue should have valid queue_id");

    // Cleanup
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_send_message() {
    let admin = create_admin().await;
    let queue = admin.create_queue(TEST_QUEUE_SEND_MESSAGE).await;
    assert!(queue.is_ok());
    let queue = queue.unwrap();
    let payload = json!({
        "k": "v"
    });
    assert!(queue.enqueue(&payload).await.is_ok());
    assert!(queue.pending_count().await.unwrap() == EXPECTED_MESSAGE_COUNT);
    let read_messages = queue.read(READ_MESSAGE_COUNT).await;
    assert!(read_messages.is_ok());
    let read_messages = read_messages.unwrap();
    assert_eq!(read_messages.len(), READ_MESSAGE_COUNT);
    assert!(read_messages[0].payload == payload);
    let dequeued_message = queue.dequeue(read_messages[0].id).await;
    assert!(dequeued_message.is_ok());
    let dequeued_message = dequeued_message.unwrap();
    assert!(dequeued_message.id == read_messages[0].id);
    assert!(queue.pending_count().await.unwrap() == 0);
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_single_message() {
    const TEST_QUEUE_ARCHIVE: &str = "test_archive_single";
    let admin = create_admin().await;
    let queue = admin
        .create_queue(TEST_QUEUE_ARCHIVE)
        .await
        .expect("Failed to create queue");

    // Send a test message
    let payload = json!({"action": "process", "data": "test_archive"});
    let message = queue
        .enqueue(&payload)
        .await
        .expect("Failed to enqueue message");
    let msg_id = message.id;

    // Verify message is in active queue
    assert_eq!(queue.pending_count().await.unwrap(), 1);
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 0);

    // Archive the message
    let archived = queue.archive(msg_id).await;
    assert!(archived.is_ok());
    assert!(archived.unwrap(), "Message should be successfully archived");

    // Verify message moved from active to archive
    assert_eq!(queue.pending_count().await.unwrap(), 0);
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 1);

    // Try to archive the same message again (should return false)
    let archived_again = queue.archive(msg_id).await;
    assert!(archived_again.is_ok());
    assert!(
        !archived_again.unwrap(),
        "Archiving already-archived message should return false"
    );

    // Cleanup - purge archive and messages before deleting queue
    admin
        .purge_archive(&queue.queue_name)
        .await
        .expect("Failed to purge archive");
    admin
        .purge_queue(&queue.queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_batch_messages() {
    const TEST_QUEUE_BATCH_ARCHIVE: &str = "test_archive_batch";
    let admin = create_admin().await;
    let queue = admin
        .create_queue(TEST_QUEUE_BATCH_ARCHIVE)
        .await
        .expect("Failed to create queue");

    // Send multiple test messages
    let mut msg_ids = Vec::new();
    for i in 0..5 {
        let payload = json!({"action": "batch_process", "index": i});
        let message = queue
            .enqueue(&payload)
            .await
            .expect("Failed to enqueue message");
        msg_ids.push(message.id);
    }

    // Verify messages are in active queue
    assert_eq!(queue.pending_count().await.unwrap(), 5);
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 0);

    // Archive first 3 messages in batch
    let batch_to_archive = msg_ids[0..3].to_vec();
    let archived_results = queue.archive_batch(batch_to_archive.clone()).await;
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
    assert_eq!(queue.pending_count().await.unwrap(), 2);
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 3);

    // Try to archive empty batch (should return empty vec)
    let empty_archive = queue.archive_batch(vec![]).await;
    assert!(empty_archive.is_ok());
    assert!(empty_archive.unwrap().is_empty());

    // Cleanup - purge archive and messages before deleting queue
    admin
        .purge_archive(&queue.queue_name)
        .await
        .expect("Failed to purge archive");
    admin
        .purge_queue(&queue.queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_nonexistent_message() {
    const TEST_QUEUE_NONEXISTENT: &str = "test_archive_nonexistent";
    let admin = create_admin().await;
    let queue = admin
        .create_queue(TEST_QUEUE_NONEXISTENT)
        .await
        .expect("Failed to create queue");

    // Try to archive a message that doesn't exist
    let fake_msg_id = 999999;
    let archived = queue.archive(fake_msg_id).await;
    assert!(archived.is_ok());
    assert!(
        !archived.unwrap(),
        "Non-existent message should not be archived"
    );

    // Verify archive count remains zero
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 0);

    // Cleanup
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_table_creation() {
    const TEST_QUEUE_TABLE: &str = "test_archive_table_creation";
    let admin = create_admin().await;

    // Create queue (should automatically create archive table)
    let queue = admin
        .create_queue(TEST_QUEUE_TABLE)
        .await
        .expect("Failed to create queue");

    // Verify archive table was created by trying to access (should not error)
    let archive_list = queue.archive_list(1000, 0).await;
    assert!(archive_list.is_ok());
    assert_eq!(archive_list.unwrap().len(), 0);

    // Test that queue creation includes archive tables automatically
    const TEST_QUEUE_STANDALONE: &str = "test_standalone_archive";
    let queue2 = admin
        .create_queue(TEST_QUEUE_STANDALONE)
        .await
        .expect("Failed to create second queue");

    // Verify archive table was automatically created (accessing archive_list should work)
    assert_eq!(queue2.archive_list(1000, 0).await.unwrap().len(), 0);

    // Cleanup both queues
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
    assert!(admin.delete_queue(&queue2.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_delete_queue_removes_archive() {
    const TEST_QUEUE_DELETE: &str = "test_delete_queue_removes_archive";
    let admin = create_admin().await;

    // Create queue and archive a message
    let queue = admin
        .create_queue(TEST_QUEUE_DELETE)
        .await
        .expect("Failed to create queue");

    // Send and archive a message
    let payload = json!({"action": "test_delete", "data": "cleanup_test"});
    let message = queue
        .enqueue(&payload)
        .await
        .expect("Failed to enqueue message");
    let archived = queue
        .archive(message.id)
        .await
        .expect("Failed to archive message");
    assert!(archived, "Message should be archived");

    // Verify archive has content
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 1);

    // Purge archive and messages before deleting queue
    admin
        .purge_archive(&queue.queue_name)
        .await
        .expect("Failed to purge archive");
    admin
        .purge_queue(&queue.queue_name)
        .await
        .expect("Failed to purge messages");

    // Delete the queue
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());

    // Try to recreate the queue and verify archive table is fresh
    let new_queue = admin
        .create_queue(TEST_QUEUE_DELETE)
        .await
        .expect("Failed to recreate queue");

    // Archive count should be 0 (fresh archive table)
    assert_eq!(new_queue.archive_list(1000, 0).await.unwrap().len(), 0);

    // Cleanup
    assert!(admin.delete_queue(&new_queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_purge_archive() {
    const TEST_QUEUE_PURGE_ARCHIVE: &str = "test_purge_archive";
    let admin = create_admin().await;

    // Create queue and archive some messages
    let queue = admin
        .create_queue(TEST_QUEUE_PURGE_ARCHIVE)
        .await
        .expect("Failed to create queue");

    // Archive multiple messages
    for i in 0..3 {
        let payload = json!({"action": "test_purge_archive", "index": i});
        let message = queue
            .enqueue(&payload)
            .await
            .expect("Failed to enqueue message");
        let archived = queue
            .archive(message.id)
            .await
            .expect("Failed to archive message");
        assert!(archived, "Message {} should be archived", i);
    }

    // Verify archive has 3 messages
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 3);

    // Purge archive
    assert!(admin.purge_archive(&queue.queue_name).await.is_ok());

    // Verify archive is empty
    assert_eq!(queue.archive_list(1000, 0).await.unwrap().len(), 0);

    // Cleanup
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
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
    let queue = queue_result.unwrap();
    let message_payload = serde_json::json!({"test": "custom_schema"});
    let send_result = queue.enqueue(&message_payload).await;
    assert!(
        send_result.is_ok(),
        "Should be able to send message to queue in custom schema"
    );

    // Cleanup - purge messages before deleting queue
    admin
        .purge_queue(&queue_name)
        .await
        .expect("Failed to purge messages");
    assert!(admin.delete_queue(&queue_name).await.is_ok());
}

#[tokio::test]
async fn test_interval_parameter_syntax() {
    let admin = create_admin().await;
    let queue_name = "test_interval_queue";

    // Create queue
    let queue = admin.create_queue(queue_name).await.unwrap();

    // Send a message to test interval functionality
    let message_payload = json!({"test": "interval_test"});
    queue.enqueue(&message_payload).await.unwrap();

    // Test reading messages (which uses make_interval in READ_MESSAGES)
    let messages = queue.read_delay(30, 1).await.unwrap(); // 30 seconds visibility timeout
    assert_eq!(messages.len(), 1, "Should read one message");

    let message = &messages[0];
    assert!(message.vt > chrono::Utc::now(), "Message should have future visibility timeout");
    let original_vt = message.vt;

    // Test extending visibility timeout (which uses make_interval in UPDATE_MESSAGE_VT)
    let extend_result = queue.extend_visibility(message.id, 60).await.unwrap(); // Extend by 60 seconds
    assert!(extend_result, "Should successfully extend visibility timeout");

    // Get the updated message to verify the interval was applied correctly
    let updated_message = queue.get_message_by_id(message.id).await.unwrap();
    assert!(updated_message.vt > original_vt, "Extended VT should be later than original");

    // Verify the interval was applied correctly (should be roughly 60 seconds later)
    let duration_diff = (updated_message.vt - original_vt).num_seconds();
    assert!(duration_diff >= 59 && duration_diff <= 61,
           "VT should be extended by approximately 60 seconds, got {} seconds", duration_diff);

    // Cleanup
    admin.purge_queue(queue_name).await.expect("Failed to purge messages");
    admin.delete_queue(queue_name).await.expect("Failed to delete queue");
}

#[tokio::test]
async fn test_referential_integrity_checks() {
    let admin = create_admin().await;
    let queue_name = "test_integrity_queue";

    // Create queue and get queue_id
    let _queue = admin.create_queue(queue_name).await.unwrap();
    let queue_list = admin.list_queues().await.unwrap();
    let queue_info = queue_list.iter().find(|q| q.queue_name == queue_name).unwrap();
    let _queue_id = queue_info.id;

    // Normal state: verify should pass
    let verify_result = admin.verify().await;
    assert!(verify_result.is_ok(), "Verify should succeed with valid references");

    // Create an orphaned message by inserting directly with invalid queue_id
    // This simulates what would happen if referential integrity was broken
    let orphan_result = sqlx::query(
        "INSERT INTO pgqrs_messages (queue_id, payload) VALUES ($1, $2)"
    )
    .bind(99999i64) // Non-existent queue_id
    .bind(json!({"test": "orphaned"}))
    .execute(&admin.pool)
    .await;

    match orphan_result {
        Ok(_) => {
            // If the insert succeeded (no foreign key constraint), verify should now fail
            let verify_result = admin.verify().await;
            assert!(verify_result.is_err(), "Verify should fail with orphaned message");
            assert!(verify_result.unwrap_err().to_string().contains("messages with invalid queue_id references"));

            // Clean up orphaned message
            sqlx::query("DELETE FROM pgqrs_messages WHERE queue_id = $1")
                .bind(99999i64)
                .execute(&admin.pool)
                .await
                .expect("Failed to clean up orphaned message");
        },
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
    admin.delete_queue(queue_name).await.expect("Failed to delete queue");
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
        Err(pgqrs::error::PgqrsError::QueueAlreadyExists { name }) => {
            assert_eq!(name, queue_name, "Error should contain the correct queue name");
        },
        Err(other) => panic!("Expected QueueAlreadyExists error, got: {:?}", other),
        Ok(_) => panic!("Expected error but queue creation succeeded"),
    }

    // Verify the original queue still exists and works
    let queues = admin.list_queues().await.unwrap();
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_some(), "Original queue should still exist");

    // Cleanup
    admin.delete_queue(queue_name).await.expect("Failed to delete queue");
}
