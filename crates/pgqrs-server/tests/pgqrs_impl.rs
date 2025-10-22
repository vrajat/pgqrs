use pgqrs_server::db::pgqrs_impl::{PgMessageRepo, PgQueueRepo};
use pgqrs_server::db::traits::{MessageRepo, QueueRepo};
use pgqrs_test_utils::postgres::get_pgqrs_client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

async fn setup_test_pool() -> sqlx::PgPool {
    // Use the pgqrs_test_utils helper that properly manages container lifecycle
    let database_url = get_pgqrs_client().await;

    PgPoolOptions::new()
        .max_connections(2) // Small pool per test
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres")
}

#[tokio::test]
async fn test_db_connectivity() {
    let pool = setup_test_pool().await;
    let val: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("SELECT 1 failed");
    assert_eq!(val, 1);
}

#[tokio::test]
async fn test_create_and_list_queue() {
    let pool = setup_test_pool().await;
    let repo = PgQueueRepo { pool: pool.clone() };
    let queue_name = "testq_create_and_list_queue";
    let queue = repo.create_queue(&queue_name, false).await.expect("create");
    assert!(queue.queue_name == queue_name);
    let queues = repo.list_queues().await.expect("list");
    assert!(queues.iter().any(|q| q.queue_name == queue_name));
    repo.delete_queue(&queue_name).await.expect("delete");
}

#[tokio::test]
async fn test_enqueue_and_peek() {
    let pool = setup_test_pool().await;
    let qrepo = PgQueueRepo { pool: pool.clone() };
    let mrepo = PgMessageRepo { pool: pool.clone() };
    let queue_name = "testq_enqueue_and_peek";
    qrepo
        .create_queue(&queue_name, false)
        .await
        .expect("create");
    let payload = json!({"foo": "bar"});
    let msg = mrepo.enqueue(&queue_name, &payload).await.expect("enqueue");
    let peeked = mrepo.peek(&queue_name, 10).await.expect("peek");
    assert!(peeked.iter().any(|m| m.id == msg.id));
    qrepo.delete_queue(&queue_name).await.expect("delete");
}

#[tokio::test]
async fn test_enqueue_dequeue() {
    let pool = setup_test_pool().await;
    let qrepo = PgQueueRepo { pool: pool.clone() };
    let mrepo = PgMessageRepo { pool: pool.clone() };
    let queue_name = "testq_enqueue_dequeue";
    qrepo
        .create_queue(&queue_name, false)
        .await
        .expect("create");
    let payload = json!({"foo": "bar"});
    let msg = mrepo.enqueue(&queue_name, &payload).await.expect("enqueue");
    let dequeued = mrepo.dequeue(&queue_name, msg.id).await.expect("dequeue");
    assert_eq!(dequeued.id, msg.id);
    qrepo.delete_queue(&queue_name).await.expect("delete");
}
