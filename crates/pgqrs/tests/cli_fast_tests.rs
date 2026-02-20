//! Fast CLI tests using pre-built binary testing.
//!
//! These tests focus on CLI-specific concerns:
//! - Binary execution and argument parsing
//! - Output formatting (JSON, table)
//! - Error handling and exit codes
//!
//! Business logic is tested in lib_tests.rs, not here.
//!
//! Performance improvement: Uses pre-built binary (built once) instead of
//! `cargo run` per test (which recompiles each time).

mod common;

use pgqrs::store::Store;
use serde_json::Value;
use std::process::Command;

/// Helper to get test database URL
fn get_test_db_url(schema: &str) -> String {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { common::create_store(schema).await.config().dsn.clone() })
}

/// Helper to run CLI command and parse JSON output
fn run_cli_json<T: serde::de::DeserializeOwned>(db_url: &str, schema: &str, args: &[&str]) -> T {
    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .args(["--dsn", db_url, "--schema", schema, "--format", "json"])
        .args(args)
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);
    serde_json::from_slice(&output.stdout).expect("Failed to parse JSON output")
}

/// Helper to run CLI command and get stdout as string
fn run_cli_output(db_url: &str, schema: &str, format: &str, args: &[&str]) -> String {
    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .args(["--dsn", db_url, "--schema", schema, "--format", format])
        .args(args)
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);
    String::from_utf8(output.stdout).expect("Invalid UTF-8 output")
}

// =============================================================================
// Basic Binary Tests
// =============================================================================

#[test]
fn test_cli_help() {
    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .arg("--help")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("PostgreSQL-backed job queue CLI"));
}

#[test]
fn test_cli_version() {
    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .arg("--version")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pgqrs"));
}

// =============================================================================
// Queue Commands - JSON Format
// =============================================================================

#[test]
fn test_queue_create_json() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    let result: Value = run_cli_json(&db_url, schema, &["queue", "create", "test_create"]);

    assert_eq!(result["queue_name"], "test_create");
    assert!(result["id"].is_number());
}

#[test]
fn test_queue_list_json() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create a queue first
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_list"]);

    // List queues
    let result: Vec<Value> = run_cli_json(&db_url, schema, &["queue", "list"]);

    assert!(!result.is_empty());
    assert!(result.iter().any(|q| q["queue_name"] == "test_list"));
}

#[test]
fn test_queue_get_json() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_get"]);

    // Get queue
    let result: Value = run_cli_json(&db_url, schema, &["queue", "get", "test_get"]);

    assert_eq!(result["queue_name"], "test_get");
}

#[test]
fn test_queue_metrics_single_json() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_metrics"]);

    // Get metrics
    let result: Value = run_cli_json(&db_url, schema, &["queue", "metrics", "test_metrics"]);

    assert_eq!(result["name"], "test_metrics");
    assert_eq!(result["total_messages"], 0);
}

#[test]
fn test_queue_metrics_all_json() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_metrics_all"]);

    // Get all metrics
    let result: Vec<Value> = run_cli_json(&db_url, schema, &["queue", "metrics"]);

    assert!(!result.is_empty());
    assert!(result.iter().any(|m| m["name"] == "test_metrics_all"));
}

// =============================================================================
// Queue Commands - Table Format
// =============================================================================

#[test]
fn test_queue_list_table() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_table"]);

    // List in table format
    let output = run_cli_output(&db_url, schema, "table", &["queue", "list"]);

    assert!(output.contains("test_table"));
    assert!(output.contains("queue_name") || output.contains("|") || output.contains("+"));
}

#[test]
fn test_queue_metrics_table() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_table_metrics"]);

    // Get metrics in table format
    let output = run_cli_output(
        &db_url,
        schema,
        "table",
        &["queue", "metrics", "test_table_metrics"],
    );

    assert!(output.contains("test_table_metrics"));
    assert!(output.contains("total_messages") || output.contains("name"));
}

// =============================================================================
// Delete and Purge Operations
// =============================================================================

#[test]
fn test_queue_delete() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_delete"]);

    // Delete queue
    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .args(["--dsn", &db_url, "--schema", schema, "--format", "json"])
        .args(["queue", "delete", "test_delete"])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Delete command failed: {:?}",
        output
    );

    // Verify deleted
    let queues: Vec<Value> = run_cli_json(&db_url, schema, &["queue", "list"]);
    assert!(!queues.iter().any(|q| q["queue_name"] == "test_delete"));
}

#[test]
fn test_queue_purge() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create queue
    let queue: Value = run_cli_json(&db_url, schema, &["queue", "create", "test_purge"]);
    let queue_id = queue["id"].as_i64().unwrap();

    // Add a message using the library (faster than CLI)
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let config = pgqrs::config::Config::from_dsn_with_schema(&db_url, schema).unwrap();
        let store = pgqrs::connect_with_config(&config).await.unwrap();
        let producer = pgqrs::producer("test_host", 8080, "test_purge")
            .create(&store)
            .await
            .unwrap();

        pgqrs::enqueue()
            .message(&serde_json::json!({"test": "purge"}))
            .worker(&producer)
            .execute(&store)
            .await
            .unwrap();

        // Verify message exists
        let messages = pgqrs::tables(&store)
            .messages()
            .filter_by_fk(queue_id)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
    });

    // Purge via CLI
    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .args(["--dsn", &db_url, "--schema", schema])
        .args(["queue", "purge", "test_purge"])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Purge command failed: {:?}",
        output
    );

    // Verify purged
    rt.block_on(async {
        let config = pgqrs::config::Config::from_dsn_with_schema(&db_url, schema).unwrap();
        let store = pgqrs::connect_with_config(&config).await.unwrap();
        let messages = pgqrs::tables(&store)
            .messages()
            .filter_by_fk(queue_id)
            .await
            .unwrap();
        assert!(messages.is_empty());
    });
}

// =============================================================================
// Error Handling
// =============================================================================

#[test]
fn test_queue_get_nonexistent_fails() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .args(["--dsn", &db_url, "--schema", schema, "--format", "json"])
        .args(["queue", "get", "nonexistent"])
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success(), "Expected command to fail");
}

#[test]
fn test_invalid_format_flag() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    let output = Command::new(assert_cmd::cargo_bin!("pgqrs"))
        .args(["--dsn", &db_url, "--schema", schema, "--format", "xml"])
        .args(["queue", "list"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);
}

// =============================================================================
// Admin Commands
// =============================================================================

#[test]
fn test_admin_stats_json() {
    let db_url = get_test_db_url("pgqrs_cli_test");
    let schema = "pgqrs_cli_test";

    // Create a queue to ensure stats are non-empty
    run_cli_json::<Value>(&db_url, schema, &["queue", "create", "test_stats"]);

    let result: Value = run_cli_json(&db_url, schema, &["admin", "stats"]);

    assert!(result["total_queues"].is_number());
    assert!(result["total_queues"].as_i64().unwrap() >= 1);
}
