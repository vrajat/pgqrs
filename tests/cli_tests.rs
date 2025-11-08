fn get_test_db_url() -> String {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { common::get_database_dsn_with_schema("pgqrs_cli_test").await })
}

fn run_cli_command(db_url: &str, args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", db_url, "--schema", "pgqrs_cli_test"])
        .args(args)
        .output()
        .expect("Failed to run CLI command")
}

mod common;

use std::process::Command;
use tokio::runtime::Runtime;

#[test]
fn test_cli_create_list_delete_queue() {
    // Bring up test DB and get DSN
    let db_url = get_test_db_url();
    let queue_name = "test_queue_cli";

    // Create queue
    let create_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "--schema",
            "pgqrs_cli_test",
            "queue",
            "create",
            queue_name,
        ])
        .output()
        .expect("Failed to run CLI create queue");
    assert!(
        create_output.status.success(),
        "Create queue failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    // List queues
    let list_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "--schema",
            "pgqrs_cli_test",
            "queue",
            "list",
        ])
        .output()
        .expect("Failed to run CLI list queues");
    assert!(
        list_output.status.success(),
        "List queues failed: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains(queue_name),
        "Queue not found in list output: {}",
        stdout
    );

    // Delete queue
    let delete_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "--schema",
            "pgqrs_cli_test",
            "queue",
            "delete",
            queue_name,
        ])
        .output()
        .expect("Failed to run CLI delete queue");
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );

    // List queues again to verify deletion
    let list_output2 = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "--schema",
            "pgqrs_cli_test",
            "queue",
            "list",
        ])
        .output()
        .expect("Failed to run CLI list queues after delete");
    assert!(
        list_output2.status.success(),
        "List queues after delete failed: {}",
        String::from_utf8_lossy(&list_output2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&list_output2.stdout);
    assert!(
        !stdout2.contains(queue_name),
        "Queue still found after deletion: {}",
        stdout2
    );
}

#[test]
fn test_cli_create_list_delete_unlogged_queue() {
    // Bring up test DB and get DSN
    let db_url = get_test_db_url();
    let queue_name = "test_unlogged_queue_cli";

    // Create unlogged queue
    let create_output = std::process::Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "queue",
            "create",
            queue_name,
            "--unlogged",
        ])
        .output()
        .expect("Failed to run CLI create unlogged queue");
    assert!(
        create_output.status.success(),
        "Create unlogged queue failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    // List queues and check unlogged field
    let list_output = std::process::Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "--format", "json", "queue", "list"])
        .output()
        .expect("Failed to run CLI list queues");
    assert!(
        list_output.status.success(),
        "List queues failed: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    let queues: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");
    let found = queues
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["queue_name"] == queue_name);
    assert!(
        found.is_some(),
        "Unlogged queue not found in list output: {}",
        stdout
    );
    let queue_obj = found.unwrap();
    assert!(
        queue_obj.get("unlogged").is_some(),
        "List output does not contain 'unlogged' field: {}",
        stdout
    );
    assert_eq!(
        queue_obj["unlogged"], true,
        "Unlogged field not set to true in output: {}",
        stdout
    );

    // Delete queue
    let delete_output = std::process::Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "delete", queue_name])
        .output()
        .expect("Failed to run CLI delete queue");
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
}

#[test]
fn test_cli_create_send_dequeue_delete_queue() {
    let db_url = get_test_db_url();
    let queue_name = "test_queue_msg_cli";
    let payload = r#"{"hello":"world"}"#;

    // Create queue
    let create_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "create", queue_name])
        .output()
        .expect("Failed to run CLI create queue");
    assert!(
        create_output.status.success(),
        "Create queue failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    // Send message
    let send_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "message", "send", queue_name, payload])
        .output()
        .expect("Failed to run CLI send message");
    assert!(
        send_output.status.success(),
        "Send message failed: {}",
        String::from_utf8_lossy(&send_output.stderr)
    );

    // Dequeue message
    let dequeue_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "message", "dequeue", queue_name])
        .output()
        .expect("Failed to run CLI dequeue message");
    assert!(
        dequeue_output.status.success(),
        "Dequeue message failed: {}",
        String::from_utf8_lossy(&dequeue_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&dequeue_output.stdout);
    assert!(
        stdout.contains("hello"),
        "Dequeued message not found in output: {}",
        stdout
    );

    // Delete queue
    let delete_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "delete", queue_name])
        .output()
        .expect("Failed to run CLI delete queue");
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
}

#[test]
fn test_cli_archive_functionality() {
    // Bring up test DB and get DSN
    let db_url = get_test_db_url();
    let queue_name = "test_archive_cli";

    // Create queue
    let create_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "create", queue_name])
        .output()
        .expect("Failed to run CLI create queue");
    assert!(
        create_output.status.success(),
        "Create queue failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    // Send a test message
    let message_payload = r#"{"test": "archive_message", "timestamp": "2023-01-01"}"#;
    let send_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "message",
            "send",
            queue_name,
            message_payload,
        ])
        .output()
        .expect("Failed to run CLI send message");
    assert!(
        send_output.status.success(),
        "Send message failed: {}",
        String::from_utf8_lossy(&send_output.stderr)
    );

    // Read the message to get its ID - just verify it exists
    let read_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn", &db_url, "message", "read", queue_name, "--count", "1",
        ])
        .output()
        .expect("Failed to run CLI read message");
    assert!(
        read_output.status.success(),
        "Read message failed: {}",
        String::from_utf8_lossy(&read_output.stderr)
    );

    // Check archived message count (should be 0 initially)
    let count_archive_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "message",
            "count",
            queue_name,
            "--archive",
        ])
        .output()
        .expect("Failed to run CLI count archived messages");
    assert!(
        count_archive_output.status.success(),
        "Count archived messages failed: {}",
        String::from_utf8_lossy(&count_archive_output.stderr)
    );

    // Read archived messages (should be empty initially)
    let read_archive_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "message",
            "read",
            queue_name,
            "--archive",
            "--count",
            "10",
        ])
        .output()
        .expect("Failed to run CLI read archived messages");
    assert!(
        read_archive_output.status.success(),
        "Read archived messages failed: {}",
        String::from_utf8_lossy(&read_archive_output.stderr)
    );

    // Delete queue
    let delete_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "delete", queue_name])
        .output()
        .expect("Failed to run CLI delete queue");
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
}

#[test]
fn test_cli_message_show_archive() {
    // Bring up test DB and get DSN
    let db_url = get_test_db_url();
    let queue_name = "test_show_archive_cli";

    // Create queue
    let create_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "create", queue_name])
        .output()
        .expect("Failed to run CLI create queue");
    assert!(
        create_output.status.success(),
        "Create queue failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    // Send a test message
    let message_payload = r#"{"test": "show_archive", "id": 12345}"#;
    let send_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "message",
            "send",
            queue_name,
            message_payload,
        ])
        .output()
        .expect("Failed to run CLI send message");
    assert!(
        send_output.status.success(),
        "Send message failed: {}",
        String::from_utf8_lossy(&send_output.stderr)
    );

    // Try to show an archived message that doesn't exist (should fail gracefully)
    let show_archive_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            &db_url,
            "message",
            "show",
            queue_name,
            "999",
            "--archive",
        ])
        .output()
        .expect("Failed to run CLI show archived message");
    assert!(
        show_archive_output.status.success(),
        "Show archived message command should succeed even if message not found: {}",
        String::from_utf8_lossy(&show_archive_output.stderr)
    );
    // Note: The command succeeds but may not produce output if message not found
    // This is expected behavior for the archive functionality

    // Delete queue
    let delete_output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args(["--dsn", &db_url, "queue", "delete", queue_name])
        .output()
        .expect("Failed to run CLI delete queue");
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
}

#[test]
fn test_cli_schema_parameter() {
    // Test that the --schema parameter works correctly
    let db_url = get_test_db_url();
    let queue_name = "test_schema_param_queue";

    // Create queue with explicit schema parameter
    let create_output = run_cli_command(&db_url, &["queue", "create", queue_name]);
    assert!(
        create_output.status.success(),
        "Create queue with schema failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    // List queues with schema parameter to verify it exists
    let list_output = run_cli_command(&db_url, &["queue", "list"]);
    assert!(
        list_output.status.success(),
        "List queues with schema failed: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains(queue_name),
        "Queue not found in schema-specific list: {}",
        stdout
    );

    // Cleanup the queue
    let delete_output = run_cli_command(&db_url, &["queue", "delete", queue_name]);
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
}
