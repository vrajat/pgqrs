#![cfg(feature = "turso")]

use pgqrs::store::turso::TursoStore;
use pgqrs::{Config, Store};
use uuid::Uuid;

async fn create_test_store() -> (TursoStore, String) {
    let db_name = format!("test_turso_{}.db", Uuid::new_v4());
    let dir = std::env::temp_dir();
    let path = dir.join(db_name);
    let path_str = path.to_str().expect("Valid path");
    let dsn = format!("turso://{}", path_str);

    let config = Config::default();
    let store = TursoStore::new(&dsn, &config)
        .await
        .expect("Failed to create TursoStore");

    pgqrs::admin(&store)
        .install()
        .await
        .expect("Failed to install schema");

    (store, path_str.to_string())
}

#[tokio::test]
async fn test_batch_size_limit() {
    let (store, _path) = create_test_store().await;

    // Create > 100 ids
    let ids: Vec<i64> = (0..101).collect();

    let result = store.messages().get_by_ids(&ids).await;
    assert!(result.is_err());

    let err = result.err().unwrap();
    match err {
        pgqrs::Error::ValidationFailed { reason } => {
            assert!(reason.contains("exceeds limit"));
        }
        _ => panic!("Expected ValidationFailed, got {:?}", err),
    }
}

#[tokio::test]
async fn test_foreign_key_enforcement() {
    let (store, _path) = create_test_store().await;

    // Let's try raw insert into pgqrs_messages with invalid queue_id
    let res = store
        .execute_raw("INSERT INTO pgqrs_messages (queue_id, payload) VALUES (99999, '{}')")
        .await;

    assert!(res.is_err());
    let err = res.err().unwrap();
    // LibSQL might return "FOREIGN KEY constraint failed" or generic query error.
    let msg = err.to_string();
    assert!(msg.contains("FOREIGN KEY constraint failed") || msg.contains("constraint failed"));
}

#[tokio::test]
async fn test_migration_versioning() {
    let (store, _path) = create_test_store().await;

    // Verify pgqrs_schema_version has entries
    let count = store
        .query_int("SELECT COUNT(*) FROM pgqrs_schema_version")
        .await
        .expect("query failed");
    assert!(count >= 5);

    // Create new store on SAME path, should not fail and not re-run (idempotent)
    let dsn = format!("turso://{}", _path);
    let config = Config::default();
    let _store2 = TursoStore::new(&dsn, &config)
        .await
        .expect("Should succeed reopening");

    pgqrs::admin(&_store2)
        .install()
        .await
        .expect("Should be idempotent");
}

#[tokio::test]
async fn test_no_phantom_duplicates_with_ephemeral_workers() {
    let (store, _path) = create_test_store().await;

    // Create a queue
    let queue_name = format!("test_queue_{}", Uuid::new_v4());
    let queue = store
        .queues()
        .insert(pgqrs::types::NewQueue {
            queue_name: queue_name.clone(),
        })
        .await
        .expect("Failed to create queue");

    const NUM_MESSAGES: usize = 10;
    let mut successes = 0;
    let mut failures = 0;

    // Send 10 messages using ephemeral producers (high contention scenario)
    for i in 0..NUM_MESSAGES {
        let payload = serde_json::json!({ "index": i });
        let result = pgqrs::enqueue()
            .message(&payload)
            .to(&queue_name)
            .execute(&store)
            .await;

        match result {
            Ok(_) => successes += 1,
            Err(e) => {
                // It's OK to fail with busy/lock errors
                let msg = e.to_string();
                if msg.contains("database is locked")
                    || msg.contains("SQLITE_BUSY")
                    || msg.contains("snapshot is stale")
                {
                    failures += 1;
                } else {
                    // Other errors should not happen
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    // CRITICAL: successes + failures must equal total attempts
    assert_eq!(
        successes + failures,
        NUM_MESSAGES,
        "successes ({}) + failures ({}) must equal total attempts ({})",
        successes,
        failures,
        NUM_MESSAGES
    );

    // CRITICAL: No phantom duplicates - actual row count should equal successes
    let actual_count = store
        .messages()
        .count_pending(queue.id)
        .await
        .expect("Failed to count messages");

    assert_eq!(
        actual_count as usize, successes,
        "Database has {} rows but we only had {} successful inserts! Phantom duplicates detected!",
        actual_count, successes
    );

    // Also verify by consuming all messages
    let mut consumed = 0;
    for _ in 0..NUM_MESSAGES {
        match pgqrs::dequeue().from(&queue_name).fetch_one(&store).await {
            Ok(Some(_msg)) => {
                consumed += 1;
            }
            Ok(None) => break, // No more messages
            Err(_) => break,   // Error dequeuing
        }
    }

    assert_eq!(
        consumed, successes,
        "Consumed {} messages but expected {} (based on successful inserts)",
        consumed, successes
    );

    println!(
        "✅ Test passed: {} attempts, {} successes, {} failures, {} messages in DB, {} consumed",
        NUM_MESSAGES, successes, failures, actual_count, consumed
    );
}
