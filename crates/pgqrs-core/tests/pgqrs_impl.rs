use chrono::Utc;
use pgqrs_core::{
    traits::{MessageRepo, QueueRepo},
    PgMessageRepo, PgQueueRepo,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

async fn setup_pool() -> sqlx::PgPool {
    let database_url = std::env::var("PGQRS_TEST_DSN")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/pgqrs_test".to_string());
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres")
}

#[tokio::test]
async fn test_create_and_list_queue() {
    let pool = setup_pool().await;
    let repo = PgQueueRepo { pool: pool.clone() };
    let queue_name = format!("testq_{}", Utc::now().timestamp());
    let queue = repo.create_queue(&queue_name, false).await.expect("create");
    assert!(queue.queue_name == queue_name);
    let queues = repo.list_queues().await.expect("list");
    assert!(queues.iter().any(|q| q.queue_name == queue_name));
    repo.delete_queue(&queue_name).await.expect("delete");
}

#[tokio::test]
async fn test_enqueue_and_peek() {
    let pool = setup_pool().await;
    let qrepo = PgQueueRepo { pool: pool.clone() };
    let mrepo = PgMessageRepo { pool: pool.clone() };
    let queue_name = format!("testq_{}", Utc::now().timestamp());
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
    let pool = setup_pool().await;
    let qrepo = PgQueueRepo { pool: pool.clone() };
    let mrepo = PgMessageRepo { pool: pool.clone() };
    let queue_name = format!("testq_{}", Utc::now().timestamp());
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
