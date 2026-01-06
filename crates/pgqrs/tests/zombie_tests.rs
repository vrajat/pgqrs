use pgqrs::{config::Config, store::Store, types::WorkerStatus};
use serial_test::serial;
use std::process::Command;

mod common;

#[tokio::test]
#[serial]
async fn test_zombie_lifecycle_and_reclamation() -> anyhow::Result<()> {
    // 1. Setup
    // 1. Setup
    let db_name = "test_zombie_reclamation";

    // For SQLite, we MUST use a file-based DB to share with the CLI process
    // common::create_store uses in-memory which CLI can't access
    let dsn = match common::current_backend() {
        common::TestBackend::Postgres => common::get_postgres_dsn(Some(db_name)).await,
        common::TestBackend::Sqlite => {
            let path = std::env::temp_dir().join(format!("zombie_{}.db", uuid::Uuid::new_v4()));
            std::fs::File::create(&path).expect("Failed to create test DB file");
            format!("sqlite://{}", path.display())
        }
        common::TestBackend::Turso => panic!("Turso requires DSN"),
    };

    let config = Config::from_dsn(&dsn);
    let store = pgqrs::connect_with_config(&config).await?;

    // Create admin via builder
    pgqrs::admin(&store).install().await?;

    // 2. Create Queue
    let queue_name = "zombie-queue";
    let queue = pgqrs::admin(&store).create_queue(queue_name).await?;

    // 3. Create Producer & Enqueue
    let producer = pgqrs::producer("producer-host", 1001, queue_name)
        .create(&store)
        .await?;

    let payload = serde_json::json!({"task": "brains"});
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .execute(&store)
        .await?;
    let msg_id = msg_ids[0];
    // We can't easily verify payload without fetching, so trust msg_id returned.
    // Or fetch it.
    let stored_msg = pgqrs::tables(&store).messages().get(msg_id).await?;
    assert_eq!(stored_msg.payload, payload);

    // 4. Create Consumer & Dequeue
    let consumer = pgqrs::consumer("consumer-host", 2001, queue_name)
        .create(&store)
        .await?;

    let dequeued_messages = pgqrs::dequeue()
        .worker(&*consumer)
        .fetch_all(&store)
        .await?;
    assert_eq!(dequeued_messages.len(), 1);
    let locked_msg = &dequeued_messages[0];
    assert_eq!(locked_msg.id, msg_id);

    // 5. Simulate Zombie (Update Heartbeat)
    // Manually set the worker's heartbeat to be old (e.g., 1 hour ago)
    let consumer_worker_id = consumer.worker_id();

    let update_sql = match common::current_backend() {
        common::TestBackend::Postgres => "UPDATE pgqrs_workers SET heartbeat_at = NOW() - $1 * INTERVAL '1 second' WHERE id = $2",
        // SQLite: datetime(now, -N seconds)
        // Bind $1 is duration in seconds
        common::TestBackend::Sqlite => "UPDATE pgqrs_workers SET heartbeat_at = datetime('now', '-' || $1 || ' seconds') WHERE id = $2",
        _ => panic!("Unsupported backend"),
    };

    store
        .execute_raw_with_two_i64(update_sql, 3600, consumer_worker_id)
        .await?;

    // 6. Test pgqrs_workers functions
    // Use pgqrs::tables(&store).workers()
    // Verify it's counted as a zombie
    let zombie_count = pgqrs::tables(&store)
        .workers()
        .count_zombies_for_queue(queue.id, chrono::Duration::seconds(60))
        .await?;
    // We intentionally violate encapsulation here for the test setup
    assert_eq!(zombie_count, 1, "Should detect 1 zombie worker");

    let zombies = pgqrs::tables(&store)
        .workers()
        .list_zombies_for_queue(queue.id, chrono::Duration::seconds(60))
        .await?;
    assert_eq!(zombies.len(), 1);
    assert_eq!(zombies[0].id, consumer_worker_id);

    // 7. Test admin.reclaim_messages
    let reclaimed = pgqrs::admin(&store)
        .reclaim_messages(queue.id, Some(chrono::Duration::seconds(60)))
        .await?;
    assert_eq!(reclaimed, 1, "Should reclaim 1 message");

    // Verify message is released
    let stored_msg = pgqrs::tables(&store).messages().get(msg_id).await?;
    assert_eq!(
        stored_msg.consumer_worker_id, None,
        "Message should have no consumer"
    );
    assert_eq!(
        stored_msg.read_ct, 1,
        "Read count should be preserved/incremented"
    );
    assert_eq!(stored_msg.read_ct, 1, "Read count should be preserved at 1");
    // Verify worker is stopped
    let updated_worker = pgqrs::tables(&store)
        .workers()
        .get(consumer_worker_id)
        .await?;
    assert!(
        matches!(updated_worker.status, WorkerStatus::Stopped),
        "Worker should be stopped"
    );

    // 8. Test CLI (Optional / Extension)
    // We'll reset the state and try to reclaim via CLI

    // Reset: Make message owned by a new zombie
    // Use a new consumer for the next test phase
    let consumer_2 = pgqrs::consumer("consumer-2", 2002, queue_name)
        .create(&store)
        .await?;

    // Dequeue again (should get the released message)
    let msgs_2 = pgqrs::dequeue()
        .worker(&*consumer_2)
        .fetch_all(&store)
        .await?;
    assert_eq!(msgs_2.len(), 1);
    assert_eq!(msgs_2[0].id, msg_id);

    // Make consumer_2 a zombie
    let c2_id = consumer_2.worker_id();
    let update_sql = match common::current_backend() {
        common::TestBackend::Postgres => "UPDATE pgqrs_workers SET heartbeat_at = NOW() - $1 * INTERVAL '1 second' WHERE id = $2",
        common::TestBackend::Sqlite => "UPDATE pgqrs_workers SET heartbeat_at = datetime('now', '-' || $1 || ' seconds') WHERE id = $2",
        _ => panic!("Unsupported backend"),
    };

    store
        .execute_raw_with_two_i64(update_sql, 3600, c2_id)
        .await?;

    // Run CLI command
    // cargo run -- -d <dsn> admin reclaim --queue <queue_name> --older-than 1m
    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "-d",
            &store.config().dsn,
            "admin",
            "reclaim",
            "--queue",
            queue_name,
            "--older-than",
            "1m",
        ])
        .output()
        .expect("Failed to execute CLI command");

    assert!(
        output.status.success(),
        "CLI command failed: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Reclaimed 1 message"),
        "CLI output mismatch: {}",
        stdout
    );

    let c2_worker = pgqrs::tables(&store).workers().get(c2_id).await?;
    assert!(
        matches!(c2_worker.status, WorkerStatus::Stopped),
        "Consumer 2 should be stopped by CLI"
    );

    // Cleanup
    pgqrs::admin(&store).purge_queue(queue_name).await?;
    pgqrs::admin(&store)
        .delete_worker(producer.worker_id())
        .await?;
    pgqrs::admin(&store)
        .delete_worker(consumer_worker_id)
        .await?; // consumer 1
    pgqrs::admin(&store).delete_worker(c2_id).await?; // consumer 2
    pgqrs::admin(&store).delete_queue(&queue).await?;

    Ok(())
}
