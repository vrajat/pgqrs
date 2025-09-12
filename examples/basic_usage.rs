use pgqrs::{Config, PgqrsClient};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration (will use defaults for now since implementation is todo)
    let config = Config::default();

    // Create client
    let client = PgqrsClient::new(config).await?;

    // Install schema (if needed)
    println!("Installing pgqrs schema...");
    client.admin().install(false)?;

    // Create queues
    println!("Creating queues...");
    client.admin().create_queue(&String::from("email")).await?;
    client.admin().create_queue(&String::from("task")).await?;

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

    let email_id = client
        .queue()
        .enqueue("email_queue", &email_payload)
        .await?;

    let task_id = client
        .queue()
        .enqueue("task_queue", &task_payload)
        .await?;

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

    let batch_ids = client
        .queue()
        .batch_enqueue("email_queue", &batch_messages)
        .await?;
    println!("Sent batch of {} emails", batch_ids.len());

    // Send delayed message
    let delayed_payload = json!({
        "reminder": "Follow up with customer",
        "customer_id": 456,
        "due_date": "2024-02-15"
    });

    let delayed_id = client
        .queue()
        .enqueue_delayed(
            "task_queue",
            &delayed_payload,
            300, // 5 minutes delay
        )
        .await?;
    println!("Sent delayed message with ID: {}", delayed_id);

    // Read messages
    println!("Reading messages...");

    let email_messages = client
        .queue()
        .read("email_queue", 10, 2)
        .await?.unwrap();
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

    let task_messages = client
        .queue()
        .read("task_queue", 5, 5)
        .await?.unwrap();
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

        let extended = client
            .queue()
            .extend_visibility("task_queue", task_msg.msg_id, 30)
            .await?;
        if extended {
            println!("Extended lock for task message {}", task_msg.msg_id);
        }

        // Simulate processing time
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Delete the message completely
        let deleted = client
            .queue()
            .delete_batch("task_queue", vec![task_msg.msg_id])
            .await?;
        if deleted.first().copied().unwrap_or(false) {
            println!("Deleted task message {}", task_msg.msg_id);
        } else {
            println!("Failed to delete task message {}", task_msg.msg_id);
        }
    }

    // Show queue metrics
    println!("\nQueue metrics:");
    let all_metrics = client.admin().all_queues_metrics().await?;
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
    let email_pending = client.queue().pending_count("email_queue").await?;
    let task_pending = client.queue().pending_count("task_queue").await?;
    println!("\nPending messages:");
    println!("  email_queue: {}", email_pending);
    println!("  task_queue: {}", task_pending);

    println!("\nExample completed successfully!");

    Ok(())
}
