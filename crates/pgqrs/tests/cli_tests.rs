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

use pgqrs::{
    store::AnyStore,
    types::{ArchivedMessage, QueueInfo, QueueMessage},
};
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

    let config = pgqrs::config::Config::from_dsn_with_schema(&db_url, "pgqrs_cli_test").unwrap();

    // Send message using a Producer
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sent_message, producer_worker_id): (QueueMessage, i64) = rt.block_on(async {
        let store = AnyStore::connect(&config).await.unwrap();
        let producer = pgqrs::producer(
            "test_cli_create_send_dequeue_delete_producer_host",
            8080,
            queue_name,
        )
        .create(&store)
        .await
        .expect("Failed to create producer");

        let msg_id = pgqrs::enqueue(&serde_json::from_str::<serde_json::Value>(payload).unwrap())
            .worker(&*producer)
            .execute(&store)
            .await
            .unwrap();

        // Fetch the message to return full object (for tests)
        let messages = pgqrs::tables(&store)
            .messages()
            .get_by_ids(&[msg_id])
            .await
            .unwrap();
        (messages[0].clone(), producer.worker_id())
    });
    assert_eq!(
        sent_message.payload,
        serde_json::from_str::<serde_json::Value>(payload).unwrap()
    );
    assert!(sent_message.producer_worker_id.is_some());

    let messages: Vec<QueueMessage> =
        run_cli_command_json(&db_url, &["queue", "messages", queue_name]);
    assert!(
        messages.len() == 1,
        "Expected 1 message in queue, found {}",
        messages.len()
    );
    let message_in_queue = &messages[0];
    assert_eq!(message_in_queue.id, sent_message.id);
    assert_eq!(message_in_queue.payload, sent_message.payload);
    assert_eq!(message_in_queue.queue_id, created_queue.id);

    // Dequeue message using a Consumer
    let (dequeued_messages, consumer_worker_id): (Vec<QueueMessage>, i64) = rt.block_on(async {
        let store = AnyStore::connect(&config).await.unwrap();
        let consumer = pgqrs::consumer(
            "test_cli_create_send_dequeue_delete_consumer_host",
            8081,
            queue_name,
        )
        .create(&store)
        .await
        .expect("Failed to create consumer");

        let msgs = consumer.dequeue().await.unwrap();
        (msgs, consumer.worker_id())
    });
    assert!(
        dequeued_messages.len() == 1,
        "Expected 1 dequeued message, found {}",
        dequeued_messages.len()
    );
    let dequeued_message = &dequeued_messages[0];
    assert_eq!(dequeued_message.payload["hello"], "world");

    // Purge queue
    run_cli_command_expect_success(&db_url, &["queue", "purge", queue_name]);

    // Delete workers
    run_cli_command_expect_success(
        &db_url,
        &["worker", "delete", &producer_worker_id.to_string()],
    );

    run_cli_command_expect_success(
        &db_url,
        &["worker", "delete", &consumer_worker_id.to_string()],
    );

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

    let config = pgqrs::config::Config::from_dsn_with_schema(&db_url, "pgqrs_cli_test").unwrap();

    // Send a test message using a Producer
    let message_payload = r#"{"test": "archive_message", "timestamp": "2023-01-01"}"#;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _sent_message: i64 = rt.block_on(async {
        let store = AnyStore::connect(&config).await.unwrap();
        let producer = pgqrs::producer(
            "test_cli_archive_functionality_producer_host",
            8080,
            queue_name,
        )
        .create(&store)
        .await
        .expect("Failed to create producer");

        pgqrs::enqueue(&serde_json::from_str::<serde_json::Value>(message_payload).unwrap())
            .worker(&*producer)
            .execute(&store)
            .await
            .unwrap()
    });

    // Dequeue and archive using a Consumer
    // We cannot return Box<dyn Consumer> out of block_on easily if it's not Send + Sync + 'static (it is, but type erasure)
    // Actually we can just do everything inside block_on for simplicity
    rt.block_on(async {
        let store = AnyStore::connect(&config).await.unwrap();
        let consumer = pgqrs::consumer(
            "test_cli_archive_functionality_consumer_host",
            8081,
            queue_name,
        )
        .create(&store)
        .await
        .expect("Failed to create consumer");

        let dequeued_messages = consumer.dequeue().await.unwrap();

        assert_eq!(
            dequeued_messages.len(),
            1,
            "Expected 1 dequeued message, found {}",
            dequeued_messages.len()
        );
        let dequeued_message = &dequeued_messages[0];
        assert_eq!(dequeued_message.payload["test"], "archive_message");

        assert!(dequeued_message.vt > chrono::Utc::now());

        // Archive the dequeued message using the same consumer
        let archive: Option<ArchivedMessage> =
            consumer.archive(dequeued_message.id).await.unwrap();

        assert!(
            archive.is_some(),
            "Expected archived message to be returned, found None"
        );
        let archived_message = archive.unwrap();
        assert_eq!(archived_message.original_msg_id, dequeued_message.id);

        let archived_list: Vec<ArchivedMessage> = pgqrs::tables(&store)
            .archive()
            .filter_by_fk(created_queue.id)
            .await
            .unwrap();

        assert!(
            archived_list.len() == 1,
            "Expected 1 archived message, found {}",
            archived_list.len()
        );
    });

    // Clean up
    run_cli_command_expect_success(&db_url, &["queue", "purge", queue_name]);
    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}

#[test]
fn test_cli_metrics_output() {
    let db_url = get_test_db_url();
    let queue_name = "test_metrics_cli_output";

    // Create queue
    run_cli_command_expect_success(&db_url, &["queue", "create", queue_name]);

    // Test 1: Single queue metrics (JSON)
    let metrics: pgqrs::types::QueueMetrics =
        run_cli_command_json(&db_url, &["queue", "metrics", queue_name]);
    assert_eq!(metrics.name, queue_name);
    assert_eq!(metrics.total_messages, 0);

    // Test 2: All queues metrics (JSON)
    let all_metrics: Vec<pgqrs::types::QueueMetrics> =
        run_cli_command_json(&db_url, &["queue", "metrics"]);
    assert!(all_metrics.iter().any(|m| m.name == queue_name));

    // Cleanup
    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}

#[test]
fn test_cli_admin_stats() {
    let db_url = get_test_db_url();
    let queue_name = "test_admin_stats_cli";

    run_cli_command_expect_success(&db_url, &["queue", "create", queue_name]);

    // Test JSON output
    let stats: pgqrs::types::SystemStats = run_cli_command_json(&db_url, &["admin", "stats"]);

    assert!(stats.total_queues >= 1);
    // We can't guarantee exact numbers for other fields as tests run in parallel/shared DB
    // but we can check basic validity
    assert!(!stats.schema_version.is_empty());

    // Test Table output (success check)
    run_cli_table_command(&db_url, &["admin", "stats"]);

    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}

fn run_cli_table_command(db_url: &str, args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .args([
            "--dsn",
            db_url,
            "--schema",
            "pgqrs_cli_test",
            "--format",
            "table",
        ])
        .args(args)
        .output()
        .expect("Failed to run CLI command")
}

#[test]
fn test_cli_metrics_output_table() {
    let db_url = get_test_db_url();
    let queue_name = "test_metrics_cli_table";

    run_cli_command_expect_success(&db_url, &["queue", "create", queue_name]);

    // Check table output success
    let output = run_cli_table_command(&db_url, &["queue", "metrics", queue_name]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(queue_name));
    // Table should contain headers
    assert!(stdout.contains("total_messages"));

    let output_all = run_cli_table_command(&db_url, &["queue", "metrics"]);
    assert!(output_all.status.success());

    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}

#[test]
fn test_cli_worker_health() {
    let db_url = get_test_db_url();
    let queue_name = "test_worker_health_cli";

    run_cli_command_expect_success(&db_url, &["queue", "create", queue_name]);
    let queue_info: QueueInfo = run_cli_command_json(&db_url, &["queue", "get", queue_name]);

    // Manually insert a stale worker directly into DB to test timeout logic
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let pool = sqlx::PgPool::connect(&db_url).await.unwrap();
        // Insert a worker with last_heartbeat 1 hour ago
        // Ensure we use the correct schema
        sqlx::query(
            "INSERT INTO pgqrs_cli_test.pgqrs_workers (queue_id, hostname, port, status, heartbeat_at)
             VALUES ($1, 'stale_host', 1234, 'ready', NOW() - INTERVAL '1 hour')"
        )
        .bind(queue_info.id)
        .execute(&pool)
        .await
        .unwrap();
    });

    // Test Global Health (JSON)
    // Default timeout is 60s, so our 1h old worker should be stale
    let global_stats: Vec<pgqrs::types::WorkerHealthStats> =
        run_cli_command_json(&db_url, &["worker", "health"]);

    let global = global_stats
        .iter()
        .find(|s| s.queue_name == "Global")
        .unwrap();
    assert!(global.total_workers >= 1);
    assert!(global.stale_workers >= 1);

    // Test Per-Queue Health (JSON)
    let queue_stats: Vec<pgqrs::types::WorkerHealthStats> =
        run_cli_command_json(&db_url, &["worker", "health", "--group-by-queue"]);

    let q_stat = queue_stats
        .iter()
        .find(|s| s.queue_name == queue_name)
        .unwrap();
    assert_eq!(q_stat.stale_workers, 1);
    assert_eq!(q_stat.total_workers, 1);

    // Test Table output
    run_cli_table_command(&db_url, &["worker", "health"]);

    // Cleanup
    rt.block_on(async {
        let pool = sqlx::PgPool::connect(&db_url).await.unwrap();
        sqlx::query("DELETE FROM pgqrs_cli_test.pgqrs_workers WHERE hostname = 'stale_host'")
            .execute(&pool)
            .await
            .unwrap();
    });

    run_cli_command_expect_success(&db_url, &["queue", "delete", queue_name]);
}
