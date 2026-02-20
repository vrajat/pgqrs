//! Example demonstrating counting operations using the new Builder API.
//!
//! This example shows how to:
//! 1. Count all records in each table
//! 2. Count records by foreign key relationships
//! 3. Use the unified table accessor interface for counting operations

use chrono::Utc;
use pgqrs::Config;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration and initialize the store
    let config = Config::from_dsn("postgres://postgres:password@localhost:5432/postgres");
    let store = pgqrs::connect_with_config(&config).await?;

    // Install the schema
    pgqrs::admin(&store).install().await?;

    // Create some test queues
    let queue1 = pgqrs::tables(&store)
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "test_queue_1".to_string(),
        })
        .await?;

    let queue2 = pgqrs::tables(&store)
        .queues()
        .insert(pgqrs::types::NewQueueRecord {
            queue_name: "test_queue_2".to_string(),
        })
        .await?;

    // Create some test workers
    let worker1 = pgqrs::tables(&store)
        .workers()
        .insert(pgqrs::types::NewWorkerRecord {
            hostname: "worker-1".to_string(),
            port: 8080,
            queue_id: Some(queue1.id),
        })
        .await?;

    let _worker2 = pgqrs::tables(&store)
        .workers()
        .insert(pgqrs::types::NewWorkerRecord {
            hostname: "worker-2".to_string(),
            port: 8081,
            queue_id: Some(queue1.id),
        })
        .await?;

    let _worker3 = pgqrs::tables(&store)
        .workers()
        .insert(pgqrs::types::NewWorkerRecord {
            hostname: "worker-3".to_string(),
            port: 8082,
            queue_id: Some(queue2.id),
        })
        .await?;

    // Create some test messages directly in the messages table
    let now = Utc::now();
    let _msg1 = pgqrs::tables(&store)
        .messages()
        .insert(pgqrs::types::NewQueueMessage {
            queue_id: queue1.id,
            payload: json!({"task": "process_data_1"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
            producer_worker_id: Some(worker1.id),
            consumer_worker_id: None,
        })
        .await?;

    let _msg2 = pgqrs::tables(&store)
        .messages()
        .insert(pgqrs::types::NewQueueMessage {
            queue_id: queue1.id,
            payload: json!({"task": "process_data_2"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
            producer_worker_id: Some(worker1.id),
            consumer_worker_id: None,
        })
        .await?;

    let _msg3 = pgqrs::tables(&store)
        .messages()
        .insert(pgqrs::types::NewQueueMessage {
            queue_id: queue2.id,
            payload: json!({"task": "backup_data_1"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
            producer_worker_id: None,
            consumer_worker_id: None,
        })
        .await?;

    // Demonstrate count() methods - count all records in each table
    println!("=== Count All Records ===");
    let total_queues = pgqrs::tables(&store).queues().count().await?;
    let total_workers = pgqrs::tables(&store).workers().count().await?;
    let total_messages = pgqrs::tables(&store).messages().count().await?;

    println!("Total queues: {}", total_queues);
    println!("Total workers: {}", total_workers);
    println!("Total messages: {}", total_messages);

    // Demonstrate count by foreign key - count by queue
    println!("\n=== Count By Queue ===");

    // Count workers by queue
    let queue1_workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue1.id)
        .await?
        .len();
    let queue2_workers = pgqrs::tables(&store)
        .workers()
        .filter_by_fk(queue2.id)
        .await?
        .len();
    println!(
        "Workers in queue '{}': {}",
        queue1.queue_name, queue1_workers
    );
    println!(
        "Workers in queue '{}': {}",
        queue2.queue_name, queue2_workers
    );

    // Count messages by queue
    let queue1_messages = pgqrs::tables(&store)
        .messages()
        .filter_by_fk(queue1.id)
        .await?
        .len();
    let queue2_messages = pgqrs::tables(&store)
        .messages()
        .filter_by_fk(queue2.id)
        .await?
        .len();
    println!(
        "Messages in queue '{}': {}",
        queue1.queue_name, queue1_messages
    );
    println!(
        "Messages in queue '{}': {}",
        queue2.queue_name, queue2_messages
    );

    println!("\n=== Summary ===");
    println!("The table accessors now provide a unified interface for:");
    println!("- count(): Get total record count in any table");
    println!("- filter_by_fk(): Get records matching a foreign key");
    println!("- This enables consistent operations across all tables");

    Ok(())
}
