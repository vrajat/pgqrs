#![cfg(feature = "turso")]

use pgqrs::{Config, Store};
use pgqrs::store::turso::TursoStore;
use uuid::Uuid;

async fn create_test_store() -> (TursoStore, String) {
    let db_name = format!("test_turso_{}.db", Uuid::new_v4());
    let dir = std::env::temp_dir();
    let path = dir.join(db_name);
    let path_str = path.to_str().expect("Valid path");
    let dsn = format!("turso://{}", path_str);

    let config = Config::default();
    let store = TursoStore::new(&dsn, &config).await.expect("Failed to create TursoStore");

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

    // Try to insert message with non-existent queue_id
    // This requires calling the internal table method or using a public API that maps to insert.
    // Since `store.messages()` returns `&dyn MessageTable`, we can use `enqueue` via `Producer`.

    // But `Producer` checks queue existence first usually?
    // Let's rely on internal table calls if possible, or straight SQL execution to verify FKs are ON.

    // Using execute_raw to verify FK behavior on raw SQL is safer to prove the PRAGMA is on.
    // But we want to test the application behavior.

    // Let's try raw insert into pgqrs_messages with invalid queue_id
    let res = store.execute_raw(
        "INSERT INTO pgqrs_messages (queue_id, payload) VALUES (99999, '{}')"
    ).await;

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
    let count = store.query_int("SELECT COUNT(*) FROM pgqrs_schema_version").await.expect("query failed");
    assert!(count >= 6);

    // Create new store on SAME path, should not fail and not re-run (idempotent)
    let dsn = format!("turso://{}", _path);
    let config = Config::default();
    let _store2 = TursoStore::new(&dsn, &config).await.expect("Should succeed reopening");
}
