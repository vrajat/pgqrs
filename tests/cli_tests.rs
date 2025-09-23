fn get_test_db_url() -> String {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let admin = common::get_pgqrs_client().await;
        admin.config().dsn.clone()
    })
}

mod common;

use std::fs;
use std::process::Command;
use tokio::runtime::Runtime;

fn setup_writer_tests_queue() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let admin = crate::common::get_pgqrs_client().await;
        // Create queue
        let _ = admin.create_queue(&"writer_tests".to_string()).await;
        // Purge queue to start clean
        let _ = admin.purge_queue(&"writer_tests".to_string()).await;
        // Add a message
        let queue = admin.get_queue(&"writer_tests".to_string()).await.unwrap();
        let payload = serde_json::json!({"test": "message"});
        let enqueue_result = queue.enqueue(&payload).await;
        assert!(
            enqueue_result.is_ok(),
            "Failed to enqueue message: {:?}",
            enqueue_result
        );
        // Assert that the queue has a message
        let count = queue
            .pending_count()
            .await
            .expect("Failed to get pending count");
        assert!(
            count > 0,
            "Queue should have at least one message after setup"
        );
    });
}

fn teardown_writer_tests_queue() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let admin = crate::common::get_pgqrs_client().await;
        let _ = admin.purge_queue(&"writer_tests".to_string()).await;
        let _ = admin.delete_queue(&"writer_tests".to_string()).await;
    });
}

// #[test]
fn cli_json_output_to_stdout() {
    setup_writer_tests_queue();
    let db_url = get_test_db_url();
    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "message",
            "read",
            "--queue",
            "writer_tests",
            "--output-format",
            "json",
            "--output-dest",
            "stdout",
            "--database-url",
            &db_url,
        ])
        .output()
        .expect("failed to execute process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("msg_id") || stdout.contains("No messages available"));
    teardown_writer_tests_queue();
}

// #[test]
fn cli_csv_output_to_file() {
    setup_writer_tests_queue();
    let file_path = "test_output.csv";
    let _ = fs::remove_file(file_path);
    let db_url = get_test_db_url();
    let _ = Command::new("cargo")
        .args([
            "run",
            "--",
            "message",
            "read",
            "--queue",
            "writer_tests",
            "--output-format",
            "csv",
            "--output-dest",
            file_path,
            "--database-url",
            &db_url,
        ])
        .output()
        .expect("failed to execute process");
    let contents = fs::read_to_string(file_path).expect("file not found");
    assert!(
        contents.contains("msg_id,enqueued_at,read_ct,vt,message")
            || contents.contains("No messages available")
    );
    let _ = fs::remove_file(file_path);
    teardown_writer_tests_queue();
}

// #[test]
fn cli_yaml_output_to_file() {
    setup_writer_tests_queue();
    let file_path = "test_output.yaml";
    let _ = fs::remove_file(file_path);
    let db_url = get_test_db_url();
    let _ = Command::new("cargo")
        .args([
            "run",
            "--",
            "message",
            "read",
            "--queue",
            "writer_tests",
            "--output-format",
            "yaml",
            "--output-dest",
            file_path,
            "--database-url",
            &db_url,
        ])
        .output()
        .expect("failed to execute process");
    let contents = fs::read_to_string(file_path).expect("file not found");
    assert!(contents.contains("msg_id:") || contents.contains("No messages available"));
    let _ = fs::remove_file(file_path);
    teardown_writer_tests_queue();
}
