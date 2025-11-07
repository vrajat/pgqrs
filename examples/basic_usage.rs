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
    admin.create_queue(&String::from("email"), false).await?;
    admin.create_queue(&String::from("task"), false).await?;

    // Send some messages
    println!("Sending messages...");

    let email_payload = json!({
        "to": "user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our service!"
    });

    let task_payload = json!({
        "task_type": "process_image",
        "image_url": "https://example.com/image.jpg",
        "priority": 1
    });

    let email_queue = admin.get_queue("email_queue").await?;
    let task_queue = admin.get_queue("task_queue").await?;

    let email_id = email_queue.enqueue(&email_payload).await?;
    let task_id = task_queue.enqueue(&task_payload).await?;

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

    let batch_ids = email_queue.batch_enqueue(&batch_messages).await?;
    println!("Sent batch of {} emails", batch_ids.len());

    // Send delayed message
    let delayed_payload = json!({
        "reminder": "Follow up with customer",
        "customer_id": 456,
        "due_date": "2024-02-15"
    });

    let delayed_id = task_queue
        .enqueue_delayed(
            &delayed_payload,
            300, // 5 minutes delay
        )
        .await?;
    println!("Sent delayed message with ID: {}", delayed_id);

    // Read messages
    println!("Reading messages...");

    let email_messages = email_queue.read_delay(10, 2).await?;
    println!("Read {} newsletter messages", email_messages.len());

    for msg in &email_messages {
        if let Some(to) = msg.message.get("to") {
            if let Some(subject) = msg.message.get("subject") {
                println!("Email ID {}: {} -> {}", msg.msg_id, subject, to);
            }
        }
        println!("  Enqueued at: {}", msg.enqueued_at);
        println!("  Read count: {}", msg.read_ct);
    }

    let task_messages = task_queue.read_delay(5, 5).await?;
    println!("Read {} task messages", task_messages.len());

    for msg in &task_messages {
        println!("Task ID {}", msg.msg_id);
    }

    if let Some(task_msg) = task_messages.first() {
        // Simulate long processing - extend lock first
        println!(
            "Processing task message {} (extending lock)...",
            task_msg.msg_id
        );

        let extended = task_queue.extend_visibility(task_msg.msg_id, 30).await?;
        if extended {
            println!("Extended lock for task message {}", task_msg.msg_id);
        }

        // Simulate processing time
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // PREFERRED: Archive the message instead of deleting for data retention
        println!("Archiving processed message...");
        let archived = task_queue
            .archive(task_msg.msg_id)
            .await?;
        if archived {
            println!("Successfully archived task message {}", task_msg.msg_id);
        } else {
            println!(
                "Failed to archive task message {} (may not exist)",
                task_msg.msg_id
            );
        }
    }

    // Demonstrate batch archiving for email messages
    println!("Batch archiving email messages...");
    let email_msg_ids: Vec<i64> = email_messages.iter().map(|m| m.msg_id).collect();
    if !email_msg_ids.is_empty() {
        let archived_ids = email_queue
            .archive_batch(email_msg_ids)
            .await?;
        println!(
            "Successfully archived {} email messages",
            archived_ids.len()
        );

        for archived_id in &archived_ids {
            println!("  Archived email message {}", archived_id);
        }
    }

    // Show archive counts
    println!("Archive counts:");
    let email_archive_count = email_queue.archive_list(1000, 0).await?.len();
    let task_archive_count = task_queue.archive_list(1000, 0).await?.len();
    println!("  email_queue archived: {}", email_archive_count);
    println!("  task_queue archived: {}", task_archive_count);

    // Example of traditional deletion for comparison
    // Note: Use archiving instead for data retention and audit trails
    if let Some(remaining_task) = task_messages.get(1) {
        println!("Traditional deletion (not recommended for data retention):");

        // Delete the message completely
        let deleted = task_queue.delete_batch(vec![remaining_task.msg_id]).await?;
        if deleted.first().copied().unwrap_or(false) {
            println!("Deleted task message {}", remaining_task.msg_id);
        } else {
            println!("Failed to delete task message {}", remaining_task.msg_id);
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
    let email_pending = email_queue.pending_count().await?;
    let task_pending = task_queue.pending_count().await?;
    println!("\nPending messages:");
    println!("  email_queue: {}", email_pending);
    println!("  task_queue: {}", task_pending);

    // Demonstrate admin archive management operations
    println!("\n--- Archive Management Example ---");

    // Check archive counts before operations
    let email_archive_count = email_queue.archive_list(1000, 0).await?.len();
    let task_archive_count = task_queue.archive_list(1000, 0).await?.len();
    println!(
        "Archive counts - email: {}, task: {}",
        email_archive_count, task_archive_count
    );

    if email_archive_count > 0 {
        println!("Purging email queue archive...");
        admin.purge_archive("email_queue").await?;
        let new_count = email_queue.archive_list(1000, 0).await?.len();
        println!("Email archive count after purge: {}", new_count);
    }

    // Note about queue deletion behavior
    println!("\nNote: When deleting a queue with admin.delete_queue(), both the queue");
    println!("and its archive table are removed to prevent orphaned archive tables.");
    println!("Use admin.purge_archive() to clear archives while preserving structure.");

    println!("\nExample completed successfully!");

    Ok(())
}
