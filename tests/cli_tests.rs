fn get_test_db_url() -> String {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { common::get_postgres_dsn().await.to_string() })
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
        .args(["--database-url", &db_url, "queue", "create", queue_name])
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
        .args(["--database-url", &db_url, "queue", "list"])
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
        .args(["--database-url", &db_url, "queue", "delete", queue_name])
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
        .args(["--database-url", &db_url, "queue", "list"])
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
            "--database-url",
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
        .args([
            "--database-url",
            &db_url,
            "--format",
            "json",
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
        .args(["--database-url", &db_url, "queue", "delete", queue_name])
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
        .args(["--database-url", &db_url, "queue", "create", queue_name])
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
        .args([
            "--database-url",
            &db_url,
            "message",
            "send",
            queue_name,
            payload,
        ])
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
        .args(["--database-url", &db_url, "message", "dequeue", queue_name])
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
        .args(["--database-url", &db_url, "queue", "delete", queue_name])
        .output()
        .expect("Failed to run CLI delete queue");
    assert!(
        delete_output.status.success(),
        "Delete queue failed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
}
