fn get_test_db_url() -> String {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { common::get_postgres_dsn(Some("pgqrs_cli_test")).await })
}

fn run_cli_command(db_url: &str, args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            db_url,
            "--schema",
            "pgqrs_cli_test",
            "--format",
            "json",
        ])
        .args(args)
        .output()
        .expect("Failed to run CLI command")
}

/// Run a CLI command and ensure it succeeds, returning the raw output for non-JSON commands
fn run_cli_command_expect_success(db_url: &str, args: &[&str]) -> std::process::Output {
    let output = run_cli_command(db_url, args);
    assert!(
        output.status.success(),
        "CLI command failed: {}\nStdout: {}\nStderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// Run a CLI command, ensure it succeeds, and deserialize the JSON output to the specified type
fn run_cli_command_json<T: DeserializeOwned>(db_url: &str, args: &[&str]) -> T {
    let output = run_cli_command_expect_success(db_url, args);
    let output_str = String::from_utf8_lossy(&output.stdout);
    println!(
        "JSON output for command '{}': {}",
        args.join(" "),
        output_str
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to deserialize JSON output from command '{}': {}\nOutput: {}",
            args.join(" "),
            e,
            output_str
        )
    })
}

mod common;

use pgqrs::types::{ArchivedMessage, QueueInfo, QueueMessage, WorkerInfo};
use serde::de::DeserializeOwned;
use std::process::Command;
use tokio::runtime::Runtime;

#[test]
fn test_cli_create_list_delete_queue() {
    // Bring up test DB and get DSN
    let db_url = get_test_db_url();
    let queue_name = "test_queue_cli";

    // Create queue - this doesn't return JSON, just success/failure
    run_cli_command_expect_success(&db_url, &["queue", "create", queue_name]);

    // List queues - this returns JSON
    let queues: Vec<QueueInfo> = run_cli_command_json(&db_url, &["queue", "list"]);
    let created_queue = queues
        .iter()
        .find(|q| q.queue_name == queue_name)
        .unwrap_or_else(|| panic!("Queue '{}' not found in list: {:?}", queue_name, queues));
    assert_eq!(created_queue.queue_name, queue_name);

    let queue: QueueInfo = run_cli_command_json(&db_url, &["queue", "get", queue_name]);
    assert_eq!(queue.queue_name, queue_name);

    // Delete queue - this doesn't return JSON, just success/failure
    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);

    // List queues again to verify deletion
    let queues_after_delete: Vec<QueueInfo> = run_cli_command_json(&db_url, &["queue", "list"]);
    assert!(
        !queues_after_delete
            .iter()
            .any(|q| q.queue_name == queue_name),
        "Queue '{}' still found after deletion: {:?}",
        queue_name,
        queues_after_delete
    );
}

#[test]
fn test_cli_create_send_dequeue_delete_queue() {
    let db_url = get_test_db_url();
    let queue_name = "test_queue_msg_cli";
    let payload = r#"{"hello":"world"}"#;

    // Create queue
    let created_queue: QueueInfo = run_cli_command_json(&db_url, &["queue", "create", queue_name]);
    assert_eq!(created_queue.queue_name, queue_name);

    // Send message
    let sent_message: QueueMessage =
        run_cli_command_json(&db_url, &["message", "enqueue", queue_name, payload]);
    assert_eq!(
        sent_message.payload,
        serde_json::from_str::<serde_json::Value>(payload).unwrap()
    );

    // Create worker
    let created_worker: WorkerInfo = run_cli_command_json(
        &db_url,
        &[
            "worker",
            "create",
            queue_name,
            "test_cli_create_send_dequeue_delete_host",
            "8080",
        ],
    );
    let worker_id = created_worker.id;

    // Dequeue message
    let dequeued_messages: Vec<QueueMessage> = run_cli_command_json(
        &db_url,
        &["message", "dequeue", queue_name, &worker_id.to_string()],
    );
    assert!(
        dequeued_messages.len() == 1,
        "Expected 1 dequeued message, found {}",
        dequeued_messages.len()
    );
    let dequeued_message = &dequeued_messages[0];
    assert_eq!(dequeued_message.payload["hello"], "world");

    // Purge queue
    run_cli_command_expect_success(&db_url, &["queue", "purge", queue_name]);

    // Delete worker
    run_cli_command_expect_success(&db_url, &["worker", "delete", &worker_id.to_string()]);

    // Delete queue
    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}

#[test]
fn test_cli_archive_functionality() {
    // Bring up test DB and get DSN
    let db_url = get_test_db_url();
    let queue_name = "test_archive_cli";

    // Create queue
    let created_queue: QueueInfo = run_cli_command_json(&db_url, &["queue", "create", queue_name]);
    assert_eq!(created_queue.queue_name, queue_name);

    // Create worker
    let created_worker: WorkerInfo = run_cli_command_json(
        &db_url,
        &[
            "worker",
            "create",
            queue_name,
            "test_cli_archive_functionality_host",
            "8080",
        ],
    );
    let worker_id = created_worker.id;

    // Send a test message
    let message_payload = r#"{"test": "archive_message", "timestamp": "2023-01-01"}"#;
    let _sent_message: QueueMessage = run_cli_command_json(
        &db_url,
        &["message", "enqueue", queue_name, message_payload],
    );

    // Dequeue the message to simulate processing
    let dequeued_messages: Vec<QueueMessage> = run_cli_command_json(
        &db_url,
        &["message", "dequeue", queue_name, &worker_id.to_string()],
    );
    assert_eq!(
        dequeued_messages.len(),
        1,
        "Expected 1 dequeued message, found {}",
        dequeued_messages.len()
    );
    let dequeued_message = &dequeued_messages[0];
    assert_eq!(dequeued_message.payload["test"], "archive_message");

    assert!(dequeued_message.vt > chrono::Utc::now());

    // Archive the dequeued message
    let archive: Option<ArchivedMessage> = run_cli_command_json(
        &db_url,
        &[
            "message",
            "archive",
            queue_name,
            &dequeued_message.id.to_string(),
        ],
    );
    assert!(
        archive.is_some(),
        "Expected archived message to be returned, found None"
    );
    let archived_message = archive.unwrap();
    assert_eq!(archived_message.original_msg_id, dequeued_message.id);

    let archived_list: Vec<ArchivedMessage> =
        run_cli_command_json(&db_url, &["archive", "list", queue_name]);
    assert!(
        archived_list.len() == 1,
        "Expected 1 archived message, found {}",
        archived_list.len()
    );

    // Clean up
    run_cli_command_expect_success(&db_url, &["queue", "purge", queue_name]);
    run_cli_command_expect_success(&db_url, &["archive", "delete", queue_name]);
    run_cli_command_expect_success(&db_url, &["worker", "delete", &worker_id.to_string()]);
    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}
