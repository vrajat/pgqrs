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
    let dsn = "postgresql://postgres:postgres@localhost:5432/postgres";

    // Create store using the new connect() function
    let store = pgqrs::connect(dsn).await?;

    // Install schema (if needed)
    println!("Installing pgqrs schema...");
    pgqrs::admin(&store).install().await?;

    // Create queues
    println!("Creating queues...");
    let email_queue = pgqrs::admin(&store).create_queue("email").await?;
    let task_queue = pgqrs::admin(&store).create_queue("task").await?;

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

    // Create producers and consumers for each queue
    let email_producer = pgqrs::producer("localhost", 3000, &email_queue.queue_name)
        .create(&store)
        .await?;

    let task_producer = pgqrs::producer("localhost", 3001, &task_queue.queue_name)
        .create(&store)
        .await?;

    let email_consumer = pgqrs::consumer("localhost", 3002, &email_queue.queue_name)
        .create(&store)
        .await?;

    let task_consumer = pgqrs::consumer("localhost", 3003, &task_queue.queue_name)
        .create(&store)
        .await?;

    // Enqueue messages using the new builder API
    let email_ids = pgqrs::enqueue()
        .message(&email_payload)
        .worker(&*email_producer)
        .execute(&store)
        .await?;
    let email_id = email_ids[0];

    let task_ids = pgqrs::enqueue()
        .message(&task_payload)
        .worker(&*task_producer)
        .execute(&store)
        .await?;
    let task_id = task_ids[0];

    println!("Sent email message with ID: {}", email_id);
    println!("Sent task message with ID: {}", task_id);

    // Send batch of messages
    let batch_messages = vec![
        json!({
            "to": "user1@example.com",
            "subject": "Newsletter",
            "body": "Monthly newsletter"
        }),
        json!({
            "to": "user2@example.com",
            "subject": "Newsletter",
            "body": "Monthly newsletter"
        }),
        json!({
            "to": "admin@example.com",
            "subject": "System Alert",
            "body": "Server maintenance scheduled"
        }),
    ];

    let batch_ids = pgqrs::enqueue()
        .messages(&batch_messages)
        .worker(&*email_producer)
        .execute(&store)
        .await?;
    println!("Sent batch of {} emails", batch_ids.len());

    // Send delayed message
    let delayed_payload = json!({
        "reminder": "Follow up with customer",
        "customer_id": 456,
        "due_date": "2024-02-15"
    });

    let delayed_ids = pgqrs::enqueue()
        .message(&delayed_payload)
        .worker(&*task_producer)
        .delay(300) // 5 minutes delay
        .execute(&store)
        .await?;
    let delayed_id = delayed_ids[0];
    println!("Sent delayed message with ID: {}", delayed_id);

    // Read messages
    println!("Reading messages...");

    let email_messages = pgqrs::dequeue()
        .worker(&*email_consumer)
        .batch(10)
        .vt_offset(2)
        .fetch_all(&store)
        .await?;
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

    let task_messages = pgqrs::dequeue()
        .worker(&*task_consumer)
        .batch(5)
        .vt_offset(5)
        .fetch_all(&store)
        .await?;
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

        // Use the consumer to extend visibility
        let extended = task_consumer.extend_visibility(task_msg.id, 30).await?;
        if extended {
            println!("Extended lock for task message {}", task_msg.id);
        }

        // Simulate processing time
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // PREFERRED: Archive the message instead of deleting for data retention
        println!("Archiving processed message...");
        task_consumer.archive(task_msg.id).await?;
        println!("Successfully archived task message {}", task_msg.id);
    }

    // Demonstrate batch archiving for email messages
    println!("Batch archiving email messages...");
    let email_msg_ids: Vec<i64> = email_messages.iter().map(|m| m.id).collect();
    if !email_msg_ids.is_empty() {
        for msg_id in &email_msg_ids {
            email_consumer.archive(*msg_id).await?;
        }
        println!(
            "Successfully archived {} email messages",
            email_msg_ids.len()
        );
    }

    // Show archive counts
    println!("Archive counts:");
    let email_archive_count = pgqrs::tables(&store)
        .archive()
        .count_for_queue(email_queue.id)
        .await?;
    let task_archive_count = pgqrs::tables(&store)
        .archive()
        .count_for_queue(task_queue.id)
        .await?;
    println!("  email queue archived: {}", email_archive_count);
    println!("  task queue archived: {}", task_archive_count);

    // Example of traditional deletion for comparison
    // Note: Use archiving instead for data retention and audit trails
    if let Some(remaining_task) = task_messages.get(1) {
        println!("Traditional deletion (not recommended for data retention):");
        task_consumer.delete(remaining_task.id).await?;
        println!("Deleted task message {}", remaining_task.id);
    }

    // Show queue metrics
    println!("\nQueue metrics:");
    let all_metrics = pgqrs::admin(&store).all_queues_metrics().await?;
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
    let email_pending = pgqrs::tables(&store)
        .messages()
        .count_pending(email_queue.id)
        .await?;
    let task_pending = pgqrs::tables(&store)
        .messages()
        .count_pending(task_queue.id)
        .await?;
    println!("\nPending messages:");
    println!("  email queue: {}", email_pending);
    println!("  task queue: {}", task_pending);

    println!("\nNote: When deleting a queue with admin.delete_queue(), both the queue");
    println!("and its archive table are removed to prevent orphaned archive tables.");

    println!("\nExample completed successfully!");

    Ok(())
}
