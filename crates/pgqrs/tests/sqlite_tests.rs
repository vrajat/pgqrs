use pgqrs::config::Config;

use pgqrs::store::{ConcurrencyModel, Store};

mod common;

#[tokio::test]
async fn test_sqlite_connect() {
    let dsn = "sqlite::memory:";
    let config = Config::from_dsn(dsn);
    let store = pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to connect to sqlite");

    assert_eq!(store.backend_name(), "sqlite");
    assert_eq!(store.concurrency_model(), ConcurrencyModel::SingleProcess);

    // 1. Install schema (Admin trait not fully ready but explicit migration run is what Install does anyway)
    // Actually, create_store installs schema via migration automatically on connect for testing.
    // In our test_sqlite_connect, we used AnyStore::connect, which runs migrations.

    // 2. Queue Operations
    let queue_name = "test_queue_sqlite";
    if store.queues().exists(queue_name).await.unwrap() {
        store.queues().delete_by_name(queue_name).await.unwrap();
    }

    let queue = store.queues().insert(pgqrs::types::NewQueue {
        queue_name: queue_name.to_string(),
    }).await.expect("Failed to create queue");
    assert_eq!(queue.queue_name, queue_name);

    // 3. Worker Operations
    let worker = store.workers().register(Some(queue.id), "test-host", 1234).await.expect("Failed to register worker");
    assert_eq!(worker.hostname, "test-host");

    // 4. Message Operations
    let payload = serde_json::json!({"foo": "bar"});
    let now = chrono::Utc::now();
    let msg = store.messages().insert(pgqrs::types::NewMessage {
        queue_id: queue.id,
        payload: payload.clone(),
        read_ct: 0,
        enqueued_at: now,
        vt: now,
        producer_worker_id: Some(worker.id),
        consumer_worker_id: None,
    }).await.expect("Failed to insert message");

    assert_eq!(msg.payload, payload);

    let count = store.messages().count_pending(queue.id).await.expect("Failed to count");
    // Should be 1 (vt <= now)
    assert!(count >= 1);
}
