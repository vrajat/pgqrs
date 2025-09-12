use pgqrs::types::ReadOptions;
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
        .producer()
        .enqueue("email_queue", &email_payload)
        .await?;

    let task_id = client
        .producer()
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
        .producer()
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
        .producer()
        .enqueue_delayed(
            "task_queue",
            &delayed_payload,
            300, // 5 minutes delay
        )
        .await?;
    println!("Sent delayed message with ID: {}", delayed_id);

    // Read messages
    println!("Reading messages...");

    let email_read_opts = ReadOptions {
        lock_time_seconds: 10,
        batch_size: Some(2),
        message_type: Some("newsletter".to_string()), // Only read newsletter emails
    };

    let email_messages = client
        .consumer()
        .read_batch("email_queue", email_read_opts)
        .await?;
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

    // Read all task messages (no filter)
    let task_read_opts = ReadOptions {
        lock_time_seconds: 15,
        batch_size: Some(5),
        message_type: None, // Read all message types
    };

    let task_messages = client
        .consumer()
        .read_batch("task_queue", task_read_opts)
        .await?;
    println!("Read {} task messages", task_messages.len());

    for msg in &task_messages {
        println!("Task ID {}", msg.msg_id);
    }

    // Process and acknowledge messages
    if let Some(email_msg) = email_messages.first() {
        // Simulate processing
        println!("Processing email message {}...", email_msg.msg_id);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Archive the message (move to archive table)
        let archived = client
            .consumer()
            .archive("email_queue", email_msg.msg_id)
            .await?;
        if archived {
            println!("Archived email message {}", email_msg.msg_id);
        } else {
            println!("Failed to archive email message {}", email_msg.msg_id);
        }
    }

    if let Some(task_msg) = task_messages.first() {
        // Simulate long processing - extend lock first
        println!(
            "Processing task message {} (extending lock)...",
            task_msg.msg_id
        );

        let extended = client
            .consumer()
            .extend_lock("task_queue", task_msg.msg_id, 30)
            .await?;
        if extended {
            println!("Extended lock for task message {}", task_msg.msg_id);
        }

        // Simulate processing time
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Delete the message completely
        let deleted = client
            .consumer()
            .dequeue("task_queue", task_msg.msg_id)
            .await?;
        if deleted {
            println!("Deleted task message {}", task_msg.msg_id);
        } else {
            println!("Failed to delete task message {}", task_msg.msg_id);
        }
    }

    // Batch operations example
    if email_messages.len() > 1 {
        let remaining_ids: Vec<_> = email_messages.iter().skip(1).map(|m| m.msg_id).collect();
        let batch_archived = client
            .consumer()
            .archive_batch("email_queue", remaining_ids.clone())
            .await?;
        let successful_archives = batch_archived.iter().filter(|&&success| success).count();
        println!(
            "Batch archived {} out of {} remaining email messages",
            successful_archives,
            remaining_ids.len()
        );
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

    // Demonstrate message type filtering
    println!("\nReading only alert messages:");
    let alert_opts = ReadOptions {
        lock_time_seconds: 5,
        batch_size: Some(10),
        message_type: Some("alert".to_string()),
    };

    let alert_messages = client
        .consumer()
        .read_batch("email_queue", alert_opts)
        .await?;
    println!("Found {} alert messages", alert_messages.len());

    for msg in alert_messages {
        println!(
            "Alert {}: {}",
            msg.msg_id,
            msg.message.get("subject").unwrap_or(&json!("No subject"))
        );
        // Clean up alert messages
        client.consumer().dequeue("email_queue", msg.msg_id).await?;
    }

    // Show pending count
    let email_pending = client.producer().pending_count("email_queue").await?;
    let task_pending = client.producer().pending_count("task_queue").await?;
    println!("\nPending messages:");
    println!("  email_queue: {}", email_pending);
    println!("  task_queue: {}", task_pending);

    println!("\nExample completed successfully!");

    Ok(())
}
