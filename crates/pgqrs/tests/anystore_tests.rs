use pgqrs::store::{BackendType, ConcurrencyModel, Store};
use serde_json::json;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    common::create_store("pgqrs_anystore_test").await
}

#[tokio::test]
async fn test_anystore_delegates_to_backend() {
    let store = create_store().await;

    // Test that AnyStore correctly delegates to the underlying store
    // by performing basic operations

    // Create a queue
    let queue_name = "test_anystore_delegation";
    let queue_info = store
        .queue(queue_name)
        .await
        .expect("Failed to create queue via AnyStore");

    assert!(queue_info.id > 0);
    assert_eq!(queue_info.queue_name, queue_name);

    // List queues
    let queues = store.queues().list().await.expect("Failed to list queues");
    assert!(queues.iter().any(|q| q.queue_name == queue_name));

    // Create a worker
    let producer = store
        .producer_ephemeral(queue_name, &store.config().clone())
        .await
        .expect("Failed to create ephemeral producer");

    assert!(producer.worker_id() > 0);

    // Enqueue a message
    let payload = json!({"test": "anystore_delegation"});
    let queue_msg = producer.enqueue(&payload).await.expect("Failed to enqueue");
    assert!(queue_msg.id > 0);

    // Verify message count
    let count = store
        .messages()
        .count_pending_for_queue(queue_info.id)
        .await
        .expect("Failed to count messages");
    assert_eq!(count, 1);

    // Cleanup
    producer.suspend().await.expect("Failed to suspend");
    producer.shutdown().await.expect("Failed to shutdown");

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_anystore_backend_name() {
    let store = create_store().await;

    // Verify backend name is correct
    let backend_name = store.backend_name();
    let expected = match common::current_backend() {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => "postgres",
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => "sqlite",
        #[cfg(feature = "turso")]
        BackendType::Turso => "turso",
    };
    assert_eq!(backend_name, expected);
}

#[tokio::test]
async fn test_anystore_concurrency_model() {
    let store = create_store().await;

    // Verify concurrency model matches backend capability
    let concurrency_model = store.concurrency_model();
    let expected = match common::current_backend() {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => ConcurrencyModel::MultiProcess,
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => ConcurrencyModel::SingleProcess,
        #[cfg(feature = "turso")]
        BackendType::Turso => ConcurrencyModel::SingleProcess,
    };
    assert_eq!(concurrency_model, expected);
}

#[tokio::test]
async fn test_anystore_config_access() {
    let store = create_store().await;

    // Verify we can access the config through AnyStore
    let config = store.config();
    assert_eq!(config.schema, "pgqrs_anystore_test");

    // Check DSN scheme matches backend
    match common::current_backend() {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => assert!(config.dsn.contains("postgres")),
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => assert!(config.dsn.contains("sqlite")),
        #[cfg(feature = "turso")]
        BackendType::Turso => assert!(config.dsn.contains("turso")),
    };
}

#[tokio::test]
async fn test_anystore_query_access() {
    let store = create_store().await;

    // Verify we can execute queries through the Store trait
    // Use dialect-agnostic query or backend-specific one
    let sql = match common::current_backend() {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => "SELECT 1::bigint", // Postgres returns int4 by default for SELECT 1
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => "SELECT 1",
        #[cfg(feature = "turso")]
        BackendType::Turso => "SELECT 1",
    };

    let result: i64 = store.query_int(sql).await.expect("Failed to execute query");

    assert_eq!(result, 1);
}

#[tokio::test]
async fn test_anystore_all_table_accessors() {
    let store = create_store().await;

    // Test all table accessor methods
    let _queues = store.queues();
    let _messages = store.messages();
    let _workers = store.workers();
    let _workflows = store.workflows();

    // Verify they all work by calling a method on each
    let queue_count = store
        .queues()
        .count()
        .await
        .expect("Failed to count queues");
    assert!(queue_count >= 0);

    let message_count = store
        .messages()
        .count()
        .await
        .expect("Failed to count messages");
    assert!(message_count >= 0);

    let worker_count = store
        .workers()
        .count()
        .await
        .expect("Failed to count workers");
    assert!(worker_count >= 0);

    let workflow_count = store
        .workflows()
        .count()
        .await
        .expect("Failed to count workflows");
    assert!(workflow_count >= 0);
}

#[tokio::test]
async fn test_anystore_admin_operations() {
    let store = create_store().await;

    // Test admin operations through AnyStore
    let admin = store
        .admin_ephemeral(&store.config().clone())
        .await
        .expect("Failed to get admin");

    // Verify schema
    let verify_result = admin.verify().await;
    assert!(verify_result.is_ok());

    // Create and delete a queue
    let queue_name = "test_anystore_admin";
    let queue_info = store
        .queue(queue_name)
        .await
        .expect("Failed to create queue");

    admin
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_anystore_worker_creation() {
    let store = create_store().await;

    // Create a test queue
    let queue_name = "test_anystore_workers";
    let queue_info = store
        .queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Test producer creation
    let producer = store
        .producer(queue_name, "test-host", 8000, &store.config().clone())
        .await
        .expect("Failed to create producer");

    assert!(producer.worker_id() > 0);

    // Test consumer creation
    let consumer = store
        .consumer(queue_name, "test-host", 8001, &store.config().clone())
        .await
        .expect("Failed to create consumer");

    assert!(consumer.worker_id() > 0);

    // Test ephemeral producer creation
    let ephemeral_producer = store
        .producer_ephemeral(queue_name, &store.config().clone())
        .await
        .expect("Failed to create ephemeral producer");

    assert!(ephemeral_producer.worker_id() > 0);

    // Test ephemeral consumer creation
    let ephemeral_consumer = store
        .consumer_ephemeral(queue_name, &store.config().clone())
        .await
        .expect("Failed to create ephemeral consumer");

    assert!(ephemeral_consumer.worker_id() > 0);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    ephemeral_producer.suspend().await.unwrap();
    ephemeral_producer.shutdown().await.unwrap();
    ephemeral_consumer.suspend().await.unwrap();
    ephemeral_consumer.shutdown().await.unwrap();

    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_anystore_connect_with_dsn() {
    // This test specifically tests connect_with_dsn for Postgres
    let dsn = common::get_test_dsn("pgqrs_anystore_dsn_test").await;

    // Test connect_with_dsn method
    let backend = common::current_backend();
    let store = pgqrs::store::AnyStore::connect_with_dsn(&dsn)
        .await
        .expect("Failed to connect with DSN");

    // Verify it works
    let backend_name = store.backend_name();
    let expected = match backend {
        #[cfg(feature = "postgres")]
        BackendType::Postgres => "postgres",
        #[cfg(feature = "sqlite")]
        BackendType::Sqlite => "sqlite",
        #[cfg(feature = "turso")]
        BackendType::Turso => "turso",
    };
    assert_eq!(backend_name, expected);

    // Install schema first
    pgqrs::admin(&store)
        .install()
        .await
        .expect("Failed to install schema");

    // Verify we can perform operations
    let verify_result = pgqrs::admin(&store).verify().await;
    assert!(verify_result.is_ok());
}

#[tokio::test]
async fn test_anystore_invalid_dsn() {
    // Test with invalid DSN format
    let result = pgqrs::store::AnyStore::connect_with_dsn("invalid://dsn").await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unsupported DSN format"));
}
