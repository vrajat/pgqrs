use pgqrs::PgqrsAdmin;
use serde_json::json;

// Test-specific constants
const TEST_QUEUE_LOGGED: &str = "test_create_logged_queue";
const TEST_QUEUE_UNLOGGED: &str = "test_create_unlogged_queue";
const TEST_QUEUE_SEND_MESSAGE: &str = "test_send_message";
const EXPECTED_MESSAGE_COUNT: i64 = 1;
const READ_MESSAGE_COUNT: usize = 1;

#[derive(sqlx::FromRow)]
struct RelPersistence {
    relpersistence: String,
}

mod common;

async fn create_admin() -> pgqrs::admin::PgqrsAdmin {
    let database_url = common::get_postgres_dsn().await;
    PgqrsAdmin::new(&pgqrs::config::Config::from_dsn(database_url))
        .await
        .expect("Failed to create PgqrsAdmin")
}

#[tokio::test]
async fn verify() {
    let admin = create_admin().await;
    // Verify should succeed
    assert!(admin.verify().await.is_ok());
}

#[tokio::test]
async fn test_create_logged_queue() {
    let admin = create_admin().await;
    let queue_name = TEST_QUEUE_LOGGED.to_string();
    let queue = admin.create_queue(&queue_name, false).await;
    let queue_list = admin.list_queues().await;
    assert!(queue.is_ok());
    assert!(queue_list.is_ok());
    let queue = queue.unwrap();
    let queue_list = queue_list.unwrap();
    let meta = queue_list
        .iter()
        .find(|q| q.queue_name == queue.queue_name)
        .unwrap();
    assert_eq!(
        meta.unlogged, false,
        "MetaResult.unlogged should be false for logged queue"
    );

    // Check system tables for logged table
    // removed unused variable table_name
    let sql = format!("SELECT relpersistence::TEXT as relpersistence FROM pg_class WHERE relname = 'q_{}' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgqrs')", queue_name);
    let pool = &admin.pool;
    let result = sqlx::query_as::<_, RelPersistence>(&sql)
        .fetch_all(pool)
        .await
        .unwrap();
    assert_eq!(
        result[0].relpersistence, "p",
        "Table should be logged (relpersistence = 'p')"
    );

    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_create_unlogged_queue() {
    let admin = create_admin().await;
    let queue_name = TEST_QUEUE_UNLOGGED.to_string();
    let queue = admin.create_queue(&queue_name, true).await;
    let queue_list = admin.list_queues().await;
    assert!(queue.is_ok());
    assert!(queue_list.is_ok());
    let queue = queue.unwrap();
    let queue_list = queue_list.unwrap();
    let meta = queue_list
        .iter()
        .find(|q| q.queue_name == queue.queue_name)
        .unwrap();
    assert_eq!(
        meta.unlogged, true,
        "MetaResult.unlogged should be true for unlogged queue"
    );

    // Check system tables for unlogged table
    let sql = format!("SELECT relpersistence::TEXT as relpersistence FROM pg_class WHERE relname = 'q_{}' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgqrs')", queue_name);
    let pool = &admin.pool;
    let result = sqlx::query_as::<_, RelPersistence>(&sql)
        .fetch_all(pool)
        .await
        .unwrap();
    assert_eq!(
        result[0].relpersistence, "u",
        "Table should be unlogged (relpersistence = 'u')"
    );

    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_send_message() {
    let admin = create_admin().await;
    let queue = admin
        .create_queue(&TEST_QUEUE_SEND_MESSAGE.to_string(), false)
        .await;
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
    assert!(read_messages[0].message == payload);
    let dequeued_message = queue.dequeue(read_messages[0].msg_id).await;
    assert!(dequeued_message.is_ok());
    let dequeued_message = dequeued_message.unwrap();
    assert!(dequeued_message.msg_id == read_messages[0].msg_id);
    assert!(queue.pending_count().await.unwrap() == 0);
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_single_message() {
    const TEST_QUEUE_ARCHIVE: &str = "test_archive_single";
    let admin = create_admin().await;
    let queue = admin
        .create_queue(&TEST_QUEUE_ARCHIVE.to_string(), false)
        .await
        .expect("Failed to create queue");

    // Send a test message
    let payload = json!({"action": "process", "data": "test_archive"});
    let message = queue.enqueue(&payload).await.expect("Failed to enqueue message");
    let msg_id = message.msg_id;

    // Verify message is in active queue
    assert_eq!(queue.pending_count().await.unwrap(), 1);
    assert_eq!(queue.archive_count().await.unwrap(), 0);

    // Archive the message
    let archived = queue.archive(msg_id, Some("test-worker")).await;
    assert!(archived.is_ok());
    assert!(archived.unwrap(), "Message should be successfully archived");

    // Verify message moved from active to archive
    assert_eq!(queue.pending_count().await.unwrap(), 0);
    assert_eq!(queue.archive_count().await.unwrap(), 1);

    // Try to archive the same message again (should return false)
    let archived_again = queue.archive(msg_id, Some("test-worker")).await;
    assert!(archived_again.is_ok());
    assert!(!archived_again.unwrap(), "Message should not be archived twice");

    // Cleanup
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_batch_messages() {
    const TEST_QUEUE_BATCH_ARCHIVE: &str = "test_archive_batch";
    let admin = create_admin().await;
    let queue = admin
        .create_queue(&TEST_QUEUE_BATCH_ARCHIVE.to_string(), false)
        .await
        .expect("Failed to create queue");

    // Send multiple test messages
    let mut msg_ids = Vec::new();
    for i in 0..5 {
        let payload = json!({"action": "batch_process", "index": i});
        let message = queue.enqueue(&payload).await.expect("Failed to enqueue message");
        msg_ids.push(message.msg_id);
    }

    // Verify messages are in active queue
    assert_eq!(queue.pending_count().await.unwrap(), 5);
    assert_eq!(queue.archive_count().await.unwrap(), 0);

    // Archive first 3 messages in batch
    let batch_to_archive = msg_ids[0..3].to_vec();
    let archived_ids = queue.archive_batch(batch_to_archive.clone(), Some("batch-worker")).await;
    assert!(archived_ids.is_ok());
    let archived_ids = archived_ids.unwrap();
    assert_eq!(archived_ids.len(), 3, "Should archive exactly 3 messages");
    
    // Verify the correct messages were archived
    for id in &batch_to_archive {
        assert!(archived_ids.contains(id), "Message {} should be in archived list", id);
    }

    // Verify counts after batch archive
    assert_eq!(queue.pending_count().await.unwrap(), 2);
    assert_eq!(queue.archive_count().await.unwrap(), 3);

    // Try to archive empty batch (should return empty vec)
    let empty_archive = queue.archive_batch(vec![], Some("empty-worker")).await;
    assert!(empty_archive.is_ok());
    assert!(empty_archive.unwrap().is_empty());

    // Cleanup
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_nonexistent_message() {
    const TEST_QUEUE_NONEXISTENT: &str = "test_archive_nonexistent";
    let admin = create_admin().await;
    let queue = admin
        .create_queue(&TEST_QUEUE_NONEXISTENT.to_string(), false)
        .await
        .expect("Failed to create queue");

    // Try to archive a message that doesn't exist
    let fake_msg_id = 999999;
    let archived = queue.archive(fake_msg_id, Some("test-worker")).await;
    assert!(archived.is_ok());
    assert!(!archived.unwrap(), "Non-existent message should not be archived");

    // Verify archive count remains zero
    assert_eq!(queue.archive_count().await.unwrap(), 0);

    // Cleanup
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
}

#[tokio::test]
async fn test_archive_table_creation() {
    const TEST_QUEUE_TABLE: &str = "test_archive_table_creation";
    let admin = create_admin().await;
    
    // Create queue (should automatically create archive table)
    let queue = admin
        .create_queue(&TEST_QUEUE_TABLE.to_string(), false)
        .await
        .expect("Failed to create queue");

    // Verify archive table was created by trying to count (should not error)
    let archive_count = queue.archive_count().await;
    assert!(archive_count.is_ok());
    assert_eq!(archive_count.unwrap(), 0);

    // Test the standalone archive table creation method
    const TEST_QUEUE_STANDALONE: &str = "test_standalone_archive";
    let queue2 = admin
        .create_queue(&TEST_QUEUE_STANDALONE.to_string(), false)
        .await
        .expect("Failed to create second queue");
    
    // Create additional archive table (should succeed even if it exists)
    let create_result = admin.create_archive_table(TEST_QUEUE_STANDALONE).await;
    assert!(create_result.is_ok());

    // Verify it still works
    assert_eq!(queue2.archive_count().await.unwrap(), 0);

    // Cleanup both queues
    assert!(admin.delete_queue(&queue.queue_name).await.is_ok());
    assert!(admin.delete_queue(&queue2.queue_name).await.is_ok());
}
