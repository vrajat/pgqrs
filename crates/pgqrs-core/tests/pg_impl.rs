use pgqrs_core::{config::Config, pool::create_pool, traits::{QueueRepo, MessageRepo}};
use sqlx::types::JsonValue;
use sqlx::Executor;

#[tokio::test]
async fn test_create_and_list_queues() {
    let config = Config {
        database_url: std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for tests"),
        max_connections: 5,
        visibility_timeout: 30,
        dead_letter_after: 5,
    };
    let pool = create_pool(&config).await.expect("pool");
    // TODO: Use PgQueueRepo implementation
    // let repo = PgQueueRepo::new(pool.clone());
    // repo.create_queue("testq", false).await.unwrap();
    // let queues = repo.list_queues().await.unwrap();
    // assert!(queues.contains(&"testq".to_string()));
}

// More tests for MessageRepo, enqueue, dequeue, etc. to be added.
