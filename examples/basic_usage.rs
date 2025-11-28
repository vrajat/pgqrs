//! Basic usage example for pgqrs.
//!
//! This example demonstrates the core functionality of pgqrs:
//! - Installing the schema
//! - Creating queues
//! - Sending messages (immediate and delayed)
//! - Reading messages from queues
//! - Batch operations
//! - Message archiving (recommended for data retention)
//! - Traditional message deletion
//! - Counting pending and archived messages
//!
//! Run this example with:
//! ```sh
//! cargo run --example basic_usage
//! ```

use pgqrs::admin::PgqrsAdmin;
use pgqrs::config::Config;
use pgqrs::tables::PgqrsMessages;
use pgqrs::{Consumer, PgqrsArchive, Producer, Table};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration
    // In a real application, you would use one of these approaches:
    // let config = Config::load().expect("Failed to load configuration");
    // let config = Config::from_env().expect("PGQRS_DSN required");
    // let config = Config::from_file("pgqrs.yaml").expect("Failed to load config");

    // For this example, we'll use a hardcoded DSN (replace with your database)
    let config = Config::from_dsn("postgresql://postgres:postgres@localhost:5432/postgres");

    // Create client
    let admin = PgqrsAdmin::new(&config).await?;

    // Install schema (if needed)
    println!("Installing pgqrs schema...");
    admin.install().await?;

    // Create queues
    println!("Creating queues...");
    admin.create_queue(&String::from("email")).await?;
    admin.create_queue(&String::from("task")).await?;

    // Send some messages
    println!("Sending messages...");

    let email_payload = json!({
        "to": "user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our service!"
    });

    let task_payload = json!({
        "task_type": "process_image",
        "image_url": "https://example.com/image.jpg"
    });

    let email_queue_info = admin.get_queue("email").await?;
    let task_queue_info = admin.get_queue("task").await?;
    // Register workers for each queue
    let email_producing_worker = admin
        .register(
            email_queue_info.queue_name.clone(),
            "http://localhost".to_string(),
            3000,
        )
        .await?;
    let task_producing_worker = admin
        .register(
            task_queue_info.queue_name.clone(),
            "http://localhost".to_string(),
            3001,
        )
        .await?;

    let email_consumer = Consumer::new(
        admin.pool.clone(),
        &email_queue_info,
        &email_producing_worker,
        &admin.config,
    );
    let task_consumer = Consumer::new(
        admin.pool.clone(),
        &task_queue_info,
        &task_producing_worker,
        &admin.config,
    );
    let email_producer = Producer::new(
        admin.pool.clone(),
        &email_queue_info,
        &email_producing_worker,
        &admin.config,
    );
    let task_producer = Producer::new(
        admin.pool.clone(),
        &task_queue_info,
        &task_producing_worker,
        &admin.config,
    );

    let email_id = email_producer.enqueue(&email_payload).await?;
    let task_id = task_producer.enqueue(&task_payload).await?;

    println!("Sent email message with ID: {}", email_id);
    println!("Sent task message with ID: {}", task_id);

    // Send batch of messages
    let batch_messages = vec![
        (json!({
            "to": "user1@example.com",
            "subject": "Newsletter",
            "body": "Monthly newsletter"
        })),
        (json!({
            "to": "user2@example.com",
            "subject": "Newsletter",
            "body": "Monthly newsletter"
        })),
        (json!({
            "to": "admin@example.com",
            "subject": "System Alert",
            "body": "Server maintenance scheduled"
        })),
    ];

    let batch_ids = email_producer.batch_enqueue(&batch_messages).await?;
    println!("Sent batch of {} emails", batch_ids.len());

    // Send delayed message
    let delayed_payload = json!({
        "reminder": "Follow up with customer",
        "customer_id": 456,
        "due_date": "2024-02-15"
    });

    let delayed_id = task_producer
        .enqueue_delayed(
            &delayed_payload,
            300, // 5 minutes delay
        )
        .await?;
    println!("Sent delayed message with ID: {}", delayed_id);

    // Read messages
    println!("Reading messages...");

    // Create a worker.
    let email_messages = email_consumer.dequeue_many_with_delay(10, 2).await?;
    println!("Read {} newsletter messages", email_messages.len());

    for msg in &email_messages {
        if let Some(to) = msg.payload.get("to") {
            if let Some(subject) = msg.payload.get("subject") {
                println!("Email ID {}: {} -> {}", msg.id, subject, to);
            }
        }
        println!("  Enqueued at: {}", msg.enqueued_at);
        println!("  Read count: {}", msg.read_ct);
    }

    let task_messages = task_consumer.dequeue_many_with_delay(5, 5).await?;
    println!("Read {} task messages", task_messages.len());

    for msg in &task_messages {
        println!("Task ID {}", msg.id);
    }

    if let Some(task_msg) = task_messages.first() {
        // Simulate long processing - extend lock first
        println!(
            "Processing task message {} (extending lock)...",
            task_msg.id
        );

        let extended = task_producer.extend_visibility(task_msg.id, 30).await?;
        if extended {
            println!("Extended lock for task message {}", task_msg.id);
        }

        // Simulate processing time
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // PREFERRED: Archive the message instead of deleting for data retention
        println!("Archiving processed message...");
        let archived = task_consumer.archive(task_msg.id).await?;
        if archived.is_some() {
            println!("Successfully archived task message {}", task_msg.id);
        } else {
            println!(
                "Failed to archive task message {} (may not exist)",
                task_msg.id
            );
        }
    }

    // Demonstrate batch archiving for email messages
    println!("Batch archiving email messages...");
    let email_msg_ids: Vec<i64> = email_messages.iter().map(|m| m.id).collect();
    if !email_msg_ids.is_empty() {
        let archived_ids = email_consumer.archive_batch(email_msg_ids.clone()).await?;
        println!(
            "Successfully archived {} email messages",
            archived_ids.len()
        );

        for (msg_id, archived) in email_msg_ids.iter().zip(archived_ids.iter()) {
            if *archived {
                println!("  Archived email message {}", msg_id);
            } else {
                println!("  Failed to archive email message {}", msg_id);
            }
        }
    }

    // Show archive counts
    {
        let tx = &mut admin.pool.begin().await?;
        println!("Archive counts:");
        let pgqrs_archive = PgqrsArchive::new(admin.pool.clone());
        let email_archive_count = pgqrs_archive.count_for_fk(email_id.id, tx).await?;
        let task_archive_count = pgqrs_archive.count_for_fk(task_id.id, tx).await?;
        println!("  email_consumer archived: {}", email_archive_count);
        println!("  task_consumer archived: {}", task_archive_count);
    }

    // Example of traditional deletion for comparison
    // Note: Use archiving instead for data retention and audit trails
    if let Some(remaining_task) = task_messages.get(1) {
        println!("Traditional deletion (not recommended for data retention):");

        // Delete the message completely
        let deleted = task_consumer.delete_many(vec![remaining_task.id]).await?;
        if deleted.first().copied().unwrap_or(false) {
            println!("Deleted task message {}", remaining_task.id);
        } else {
            println!("Failed to delete task message {}", remaining_task.id);
        }
    }

    // Show queue metrics
    println!("\nQueue metrics:");
    let all_metrics = admin.all_queues_metrics().await?;
    for metrics in all_metrics {
        println!(
            "  {}: {} total, {} pending, {} locked, {} archived",
            metrics.name,
            metrics.total_messages,
            metrics.pending_messages,
            metrics.locked_messages,
            metrics.archived_messages
        );

        if let Some(oldest) = metrics.oldest_pending_message {
            println!("    Oldest pending: {}", oldest);
        }
        if let Some(newest) = metrics.newest_message {
            println!("    Newest message: {}", newest);
        }
    }

    // Show pending count
    let messages = PgqrsMessages::new(admin.pool.clone());
    let email_pending = messages.count_pending(email_id.id).await?;
    let task_pending = messages.count_pending(task_id.id).await?;
    println!("\nPending messages:");
    println!("  email_consumer: {}", email_pending);
    println!("  task_consumer: {}", task_pending);

    // Demonstrate admin archive management operations
    println!("\n--- Archive Management Example ---");

    // Check archive counts before operations
    {
        let tx = &mut admin.pool.begin().await?;
        let pgqrs_archive = PgqrsArchive::new(admin.pool.clone());
        let email_archive_count = pgqrs_archive.count_for_fk(email_id.id, tx).await?;
        let task_archive_count = pgqrs_archive.count_for_fk(task_id.id, tx).await?;
        println!(
            "Archive counts - email: {}, task: {}",
            email_archive_count, task_archive_count
        );
    }

    // Note about queue deletion behavior
    println!("\nNote: When deleting a queue with admin.delete_queue(), both the queue");
    println!("and its archive table are removed to prevent orphaned archive tables.");
    println!("Use admin.purge_archive() to clear archives while preserving structure.");

    println!("\nExample completed successfully!");

    Ok(())
}
