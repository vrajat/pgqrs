use pgqrs::Store;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    let dsn = match common::current_backend() {
        #[cfg(feature = "postgres")]
        pgqrs::store::BackendType::Postgres => common::get_test_dsn("public").await,
        #[cfg(feature = "sqlite")]
        pgqrs::store::BackendType::Sqlite => format!(
            "sqlite:file:{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        ),
        #[cfg(feature = "turso")]
        pgqrs::store::BackendType::Turso => {
            let path = std::env::temp_dir().join(format!(
                "test_default_schema_turso_{}.db",
                uuid::Uuid::new_v4()
            ));
            std::fs::File::create(&path).expect("Failed to create test DB file");
            format!("turso://{}", path.display())
        }
    };
    let config = pgqrs::config::Config::from_dsn(&dsn);
    let store = pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to create Store");

    // Install schema
    pgqrs::admin(&store)
        .install()
        .await
        .expect("Failed to install schema");

    store
}

#[tokio::test]
async fn verify() {
    let store = create_store().await;
    // Verify should succeed (using default schema "public")
    assert!(pgqrs::admin(&store).verify().await.is_ok());
}

#[tokio::test]
async fn test_default_schema_backward_compatibility() {
    // This test ensures that the default behavior works without any explicit schema configuration
    let database_url = match common::current_backend() {
        #[cfg(feature = "postgres")]
        pgqrs::store::BackendType::Postgres => common::get_test_dsn("public").await,
        #[cfg(feature = "sqlite")]
        pgqrs::store::BackendType::Sqlite => format!(
            "sqlite:file:{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        ),
        #[cfg(feature = "turso")]
        pgqrs::store::BackendType::Turso => {
            let path = std::env::temp_dir().join(format!(
                "test_default_schema_turso_{}.db",
                uuid::Uuid::new_v4()
            ));
            std::fs::File::create(&path).expect("Failed to create test DB file");
            format!("turso://{}", path.display())
        }
    };

    // Test Config::from_dsn creates config with default schema (public for Postgres)
    let config = pgqrs::config::Config::from_dsn(&database_url);

    #[cfg(feature = "postgres")]
    if common::current_backend() == pgqrs::store::BackendType::Postgres {
        assert_eq!(config.schema, "public");
    }

    // Test that store operations work with default schema
    let store = pgqrs::connect_with_config(&config)
        .await
        .expect("Failed to create store");

    // Install schema
    pgqrs::admin(&store)
        .install()
        .await
        .expect("Failed to install schema");

    // Verify installation in default schema
    assert!(pgqrs::admin(&store).verify().await.is_ok());

    // Test basic queue operations in default schema
    let queue_name = "test_default_schema_queue".to_string();
    let queue_result = store.queue(&queue_name).await;
    assert!(
        queue_result.is_ok(),
        "Should create queue in default schema"
    );
    let queue_info = queue_result.unwrap();

    // Test queue listing using tables API
    let queues = pgqrs::tables(&store)
        .queues()
        .list()
        .await
        .expect("Should list queues");
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_some(), "Should find created queue in list");

    // Cleanup
    assert!(
        pgqrs::admin(&store).delete_queue(&queue_info).await.is_ok(),
        "Should delete queue"
    );
}
