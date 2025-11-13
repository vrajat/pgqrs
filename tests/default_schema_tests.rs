use pgqrs::{tables::PgqrsQueues, PgqrsAdmin, Table};

mod common;

async fn create_admin() -> pgqrs::admin::PgqrsAdmin {
    let database_url = common::get_postgres_dsn(None).await;
    PgqrsAdmin::new(&pgqrs::config::Config::from_dsn(database_url))
        .await
        .expect("Failed to create PgqrsAdmin")
}

#[tokio::test]
async fn verify() {
    let admin = create_admin().await;
    // Verify should succeed (using default schema "public")
    assert!(admin.verify().await.is_ok());
}

#[tokio::test]
async fn test_default_schema_backward_compatibility() {
    // This test ensures that the default behavior (using "public" schema)
    // works without any explicit schema configuration
    let database_url = common::get_postgres_dsn(None).await;

    // Test Config::from_dsn creates config with default "public" schema
    let config = pgqrs::config::Config::from_dsn(&database_url);
    assert_eq!(config.schema, "public");

    // Test that admin operations work with default schema
    let admin = PgqrsAdmin::new(&config)
        .await
        .expect("Failed to create admin");

    // Verify installation in default schema
    assert!(admin.verify().await.is_ok());

    // Test basic queue operations in default schema
    let queue_name = "test_default_schema_queue".to_string();
    let queue_result = admin.create_queue(&queue_name).await;
    assert!(
        queue_result.is_ok(),
        "Should create queue in default schema"
    );

    // Test queue listing
    let queue_obj = PgqrsQueues::new(admin.pool.clone());
    let queues = queue_obj.list().await.expect("Should list queues");
    let found_queue = queues.iter().find(|q| q.queue_name == queue_name);
    assert!(found_queue.is_some(), "Should find created queue in list");

    // Cleanup
    assert!(
        admin.delete_queue(&found_queue.unwrap()).await.is_ok(),
        "Should delete queue"
    );
}
