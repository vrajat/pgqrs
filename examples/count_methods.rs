//! Example demonstrating the new count() and count_by_fk() methods in the Table trait.
//!
//! This example shows how to:
//! 1. Count all records in each table
//! 2. Count records by foreign key relationships
//! 3. Use the unified Table trait interface for counting operations

use chrono::Utc;
use pgqrs::config::Config;
use pgqrs::tables::pgqrs_messages::{NewMessage, PgqrsMessages};
use pgqrs::tables::pgqrs_queues::{NewQueue, PgqrsQueues};
use pgqrs::tables::pgqrs_workers::{NewWorker, PgqrsWorkers};
use pgqrs::tables::{PgqrsArchive, Table};
use pgqrs::PgqrsAdmin;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration and initialize the admin
    let config = Config::from_dsn("postgres://postgres:password@localhost:5432/postgres");
    let admin = PgqrsAdmin::new(&config).await?;
    let pool = admin.pool.clone();

    // Initialize table instances
    let queues = PgqrsQueues::new(pool.clone());
    let workers = PgqrsWorkers::new(pool.clone());
    let messages = PgqrsMessages::new(pool.clone());
    let archives = PgqrsArchive::new(pool.clone());

    // Install the schema and create test data
    admin.install().await?;

    // Create some test queues
    let queue1 = queues
        .insert(NewQueue {
            queue_name: "test_queue_1".to_string(),
        })
        .await?;

    let queue2 = queues
        .insert(NewQueue {
            queue_name: "test_queue_2".to_string(),
        })
        .await?;

    // Create some test workers
    let worker1 = workers
        .insert(NewWorker {
            hostname: "worker-1".to_string(),
            port: 8080,
            queue_id: queue1.id,
        })
        .await?;

    let _worker2 = workers
        .insert(NewWorker {
            hostname: "worker-2".to_string(),
            port: 8081,
            queue_id: queue1.id,
        })
        .await?;

    let _worker3 = workers
        .insert(NewWorker {
            hostname: "worker-3".to_string(),
            port: 8082,
            queue_id: queue2.id,
        })
        .await?;

    // Create some test messages directly in the messages table
    let now = Utc::now();
    let _msg1 = messages
        .insert(NewMessage {
            queue_id: queue1.id,
            payload: json!({"task": "process_data_1"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
        })
        .await?;

    let _msg2 = messages
        .insert(NewMessage {
            queue_id: queue1.id,
            payload: json!({"task": "process_data_2"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
        })
        .await?;

    let _msg3 = messages
        .insert(NewMessage {
            queue_id: queue2.id,
            payload: json!({"task": "backup_data_1"}),
            read_ct: 0,
            enqueued_at: now,
            vt: now,
        })
        .await?;

    // Create some test archived messages (simulating processed messages)
    let _archive1 = archives
        .insert(pgqrs::types::NewArchivedMessage {
            original_msg_id: 100, // Simulated original message ID
            queue_id: queue1.id,
            worker_id: Some(worker1.id),
            payload: json!({"task": "completed_task_1"}),
            enqueued_at: now,
            vt: now,
            read_ct: 1,
            dequeued_at: Some(now),
        })
        .await?;

    let _archive2 = archives
        .insert(pgqrs::types::NewArchivedMessage {
            original_msg_id: 101,
            queue_id: queue2.id,
            worker_id: None,
            payload: json!({"task": "completed_task_2"}),
            enqueued_at: now,
            vt: now,
            read_ct: 2,
            dequeued_at: None,
        })
        .await?;

    // Demonstrate count() methods - count all records in each table
    println!("=== Count All Records ===");
    let total_queues = queues.count().await?;
    let total_workers = workers.count().await?;
    let total_messages = messages.count().await?;
    let total_archives = archives.count().await?;

    println!("Total queues: {}", total_queues);
    println!("Total workers: {}", total_workers);
    println!("Total messages: {}", total_messages);
    println!("Total archived messages: {}", total_archives);

    // Demonstrate count_by_fk() methods - count by foreign key relationships
    println!("\n=== Count By Foreign Key ===");

    // Count workers by queue
    {
        let mut txn = pool.begin().await?;
        let queue1_workers = workers.count_for_fk(queue1.id, &mut txn).await?;
        let queue2_workers = workers.count_for_fk(queue2.id, &mut txn).await?;
        println!(
            "Workers in queue '{}': {}",
            queue1.queue_name, queue1_workers
        );
        println!(
            "Workers in queue '{}': {}",
            queue2.queue_name, queue2_workers
        );

        // Count messages by queue
        let queue1_messages = messages.count_for_fk(queue1.id, &mut txn).await?;
        let queue2_messages = messages.count_for_fk(queue2.id, &mut txn).await?;
        println!(
            "Messages in queue '{}': {}",
            queue1.queue_name, queue1_messages
        );
        println!(
            "Messages in queue '{}': {}",
            queue2.queue_name, queue2_messages
        );

        // Count archived messages by queue
        let queue1_archives = archives.count_for_fk(queue1.id, &mut txn).await?;
        let queue2_archives = archives.count_for_fk(queue2.id, &mut txn).await?;
        println!(
            "Archived messages in queue '{}': {}",
            queue1.queue_name, queue1_archives
        );
        println!(
            "Archived messages in queue '{}': {}",
            queue2.queue_name, queue2_archives
        );
    }

    {
        let mut txn = pool.begin().await?;
        // Queues don't have meaningful foreign keys, so this returns 0
        let queue_foreign_count = queues.count_for_fk(999, &mut txn).await?;
        println!("Queues by foreign key (always 0): {}", queue_foreign_count);
    }

    println!("\n=== Summary ===");
    println!("The Table trait now provides a unified interface for:");
    println!("- count(): Get total record count in any table");
    println!("- count_for_fk(): Get count of records matching a foreign key");
    println!("- This enables consistent counting operations across all tables");
    println!("- Archive table is now included with full CRUD + count support");

    // Clean up
    admin.uninstall().await?;

    Ok(())
}
