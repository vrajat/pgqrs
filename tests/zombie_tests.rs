use pgqrs::{
    admin::PgqrsAdmin,
    config::Config,
    consumer::Consumer,
    producer::Producer,
    tables::{PgqrsMessages, PgqrsWorkers},
    types::WorkerStatus,
    Table, Worker,
};
use serial_test::serial;
use sqlx::PgPool;
use std::process::Command;
use std::time::Duration;

mod common;

#[tokio::test]
#[serial]
async fn test_zombie_lifecycle_and_reclamation() -> anyhow::Result<()> {
    // 1. Setup
    let db_name = "test_zombie_reclamation";
    let dsn = common::get_postgres_dsn(Some(db_name)).await;
    let config = Config::from_dsn(dsn.clone());
    let pool = PgPool::connect(&dsn).await?;

    let admin = PgqrsAdmin::new(&config).await?;
    admin.install().await?;

    // 2. Create Queue
    let queue_name = "zombie-queue";
    let queue = admin.create_queue(queue_name).await?;

    // 3. Create Producer & Enqueue
    let producer = Producer::new(pool.clone(), &queue, "producer-host", 1001, &config).await?;

    let payload = serde_json::json!({"task": "brains"});
    let msg = producer.enqueue(&payload).await?;
    assert_eq!(msg.payload, payload);

    // 4. Create Consumer & Dequeue
    let consumer = Consumer::new(pool.clone(), &queue, "consumer-host", 2001, &config).await?;

    let dequeued_messages = consumer.dequeue().await?;
    assert_eq!(dequeued_messages.len(), 1);
    let locked_msg = &dequeued_messages[0];
    assert_eq!(locked_msg.id, msg.id);

    // 5. Simulate Zombie (Update Heartbeat)
    // Manually set the worker's heartbeat to be old (e.g., 1 hour ago)
    // We intentionally valid encapsulation here for the test setup
    let consumer_worker_id = consumer.worker_id();
    sqlx::query("UPDATE pgqrs_workers SET heartbeat_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(consumer_worker_id)
        .execute(&pool)
        .await?;

    // 6. Test pgqrs_workers functions
    let workers_table = PgqrsWorkers::new(pool.clone());

    // Verify it's counted as a zombie
    let zombie_count = workers_table
        .count_zombies_for_queue(queue.id, Duration::from_secs(60))
        .await?;
    assert_eq!(zombie_count, 1, "Should detect 1 zombie worker");

    let zombies = workers_table
        .list_zombies_for_queue(queue.id, Duration::from_secs(60))
        .await?;
    assert_eq!(zombies.len(), 1);
    assert_eq!(zombies[0].id, consumer_worker_id);

    // 7. Test admin.reclaim_messages
    let reclaimed = admin
        .reclaim_messages(queue.id, Some(Duration::from_secs(60)))
        .await?;
    assert_eq!(reclaimed, 1, "Should reclaim 1 message");

    // Verify message is released
    let messages_table = PgqrsMessages::new(pool.clone());
    let stored_msg = messages_table.get(msg.id).await?;
    assert_eq!(
        stored_msg.consumer_worker_id, None,
        "Message should have no consumer"
    );
    assert_eq!(
        stored_msg.read_ct, 1,
        "Read count should be preserved/incremented"
    );

    // Verify worker is stopped
    let updated_worker = workers_table.get(consumer_worker_id).await?;
    assert!(
        matches!(updated_worker.status, WorkerStatus::Stopped),
        "Worker should be stopped"
    );

    // 8. Test CLI (Optional / Extension)
    // We'll reset the state and try to reclaim via CLI

    // Reset: Make message owned by a new zombie
    // Use a new consumer for the next test phase
    let consumer_2 = Consumer::new(pool.clone(), &queue, "consumer-2", 2002, &config).await?;

    // Dequeue again (should get the released message)
    let msgs_2 = consumer_2.dequeue().await?;
    assert_eq!(msgs_2.len(), 1);
    assert_eq!(msgs_2[0].id, msg.id);

    // Make consumer_2 a zombie
    let c2_id = consumer_2.worker_id();
    sqlx::query("UPDATE pgqrs_workers SET heartbeat_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(c2_id)
        .execute(&pool)
        .await?;

    // Run CLI command
    // cargo run -- -d <dsn> admin reclaim --queue <queue_name> --older-than 1m
    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "-d",
            &dsn,
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
        stdout.contains("Reclaimed 1 messages"),
        "CLI output mismatch: {}",
        stdout
    );

    let c2_worker = workers_table.get(c2_id).await?;
    assert!(
        matches!(c2_worker.status, WorkerStatus::Stopped),
        "Consumer 2 should be stopped by CLI"
    );

    // Cleanup
    admin.purge_queue(queue_name).await?;
    admin.delete_worker(producer.worker_id()).await?;
    admin.delete_worker(consumer_worker_id).await?; // consumer 1
    admin.delete_worker(c2_id).await?; // consumer 2
    admin.delete_queue(&queue).await?;

    Ok(())
}
