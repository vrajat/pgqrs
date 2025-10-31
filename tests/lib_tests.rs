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
