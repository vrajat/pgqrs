//! Integration tests for error handling scenarios with schema operations

use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::error::PgqrsError;
use serial_test::serial;

mod common;

#[tokio::test]
#[serial]
async fn test_invalid_schema_name_empty() {
    let database_url = "postgres://user:password@localhost:5432/testdb";
    // Test empty schema name
    let result = Config::from_dsn_with_schema(database_url, "");
    assert!(result.is_err());

    if let Err(PgqrsError::InvalidConfig { field, message }) = result {
        assert_eq!(field, "schema");
        assert!(message.contains("cannot be empty"));
    } else {
        panic!("Expected InvalidConfig error for empty schema name");
    }
}

#[tokio::test]
#[serial]
async fn test_invalid_schema_name_too_long() {
    let database_url = "postgres://user:password@localhost:5432/testdb";

    // Test schema name longer than 63 characters
    let long_schema = "a".repeat(64);
    let result = Config::from_dsn_with_schema(database_url, &long_schema);
    assert!(result.is_err());

    if let Err(PgqrsError::InvalidConfig { field, message }) = result {
        assert_eq!(field, "schema");
        assert!(message.contains("exceeds maximum length"));
    } else {
        panic!("Expected InvalidConfig error for too long schema name");
    }
}

#[tokio::test]
#[serial]
async fn test_invalid_schema_name_bad_start() {
    let database_url = "postgres://user:password@localhost:5432/testdb";

    // Test schema name starting with digit
    let result = Config::from_dsn_with_schema(database_url, "123invalid");
    assert!(result.is_err());

    if let Err(PgqrsError::InvalidConfig { field, message }) = result {
        assert_eq!(field, "schema");
        assert!(message.contains("must start with a letter or underscore"));
    } else {
        panic!("Expected InvalidConfig error for schema starting with digit");
    }
}

#[tokio::test]
#[serial]
async fn test_invalid_schema_name_bad_characters() {
    let database_url = "postgres://user:password@localhost:5432/testdb";

    // Test schema name with invalid characters
    let result = Config::from_dsn_with_schema(database_url, "test-schema");
    assert!(result.is_err());

    if let Err(PgqrsError::InvalidConfig { field, message }) = result {
        assert_eq!(field, "schema");
        assert!(message.contains("invalid character"));
    } else {
        panic!("Expected InvalidConfig error for schema with invalid characters");
    }
}

#[tokio::test]
#[serial]
async fn test_valid_schema_names() {
    let database_url = "postgres://user:password@localhost:5432/testdb";

    // Test various valid schema names
    let long_schema = "a".repeat(63);
    let valid_names = vec![
        "test_schema",
        "_private_schema",
        "schema123",
        "Schema_With_Mixed_Case",
        "schema$with$dollars",
        "a",                  // Single character
        long_schema.as_str(), // Maximum length
    ];

    for schema_name in valid_names {
        let result = Config::from_dsn_with_schema(database_url, schema_name);
        assert!(result.is_ok(), "Schema '{}' should be valid", schema_name);

        let config = result.unwrap();
        assert_eq!(config.schema, schema_name);
    }
}

#[tokio::test]
#[serial]
async fn test_nonexistent_schema_operations() {
    let database_url = common::get_postgres_dsn(Some("pgqrs_error_test")).await;

    // Create config with a schema that doesn't exist
    let config = Config::from_dsn_with_schema(database_url, "nonexistent_schema_test")
        .expect("Valid schema name");

    // Creating admin should succeed (connection pool is created)
    let admin = PgqrsAdmin::new(&config)
        .await
        .expect("Creating admin should succeed");

    // However, operations should fail when PostgreSQL realizes the schema doesn't exist
    // Let's check if we can verify the schema exists - this should fail
    let result = admin.verify().await;
    assert!(
        result.is_err(),
        "Verify should fail for non-existent schema"
    );

    if let Err(PgqrsError::Connection { message }) = result {
        assert!(
            message.contains("relation \"pgqrs_messages\" does not exist"),
            "Error should mention schema not existing, got: {}",
            message
        );
    } else {
        panic!(
            "Expected Connection error for non-existent schema verification {}",
            result.err().unwrap()
        );
    }
}

#[tokio::test]
#[serial]
async fn test_verify_requires_existing_schema() {
    let database_url = common::get_postgres_dsn(Some("pgqrs_error_test")).await;

    // Create admin with existing schema
    let config =
        Config::from_dsn_with_schema(database_url, "pgqrs_error_test").expect("Valid schema name");
    let admin = PgqrsAdmin::new(&config)
        .await
        .expect("Should be able to create admin with existing schema");

    // Install should work with existing schema
    let result = admin.verify().await;
    assert!(result.is_ok(), "Verify should work with existing schema");
}
