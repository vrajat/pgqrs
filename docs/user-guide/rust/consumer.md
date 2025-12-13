# Consumer API

The `Consumer` is responsible for fetching and processing messages from a queue.

## Creating a Consumer

```rust
use pgqrs::{Admin, Consumer, Config};

let config = Config::from_dsn("postgresql://localhost/mydb");
let admin = Admin::new(&config).await?;
let queue = admin.get_queue("tasks").await?;

let consumer = Consumer::new(
    admin.pool.clone(),  // Database connection pool
    &queue,              // Queue to consume from
    "worker-1",          // Hostname for worker identification
    3001,                // Port for worker identification
    &config,             // Configuration
).await?;
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pool` | `PgPool` | SQLx PostgreSQL connection pool |
| `queue` | `&QueueInfo` | Queue to consume messages from |
| `hostname` | `&str` | Hostname for worker identification |
| `port` | `i32` | Port for worker identification |
| `config` | `&Config` | pgqrs configuration |

## Methods

### dequeue

Fetch available messages from the queue with default settings.

```rust
let messages = consumer.dequeue().await?;

for message in messages {
    println!("ID: {}", message.id);
    println!("Payload: {:?}", message.payload);
    println!("Enqueued at: {}", message.enqueued_at);
    println!("Read count: {}", message.read_ct);
}
```

**Returns:** `Result<Vec<QueueMessage>>` - Available messages (empty if none available).

Messages are automatically locked with the default lock time from configuration.

### dequeue_many_with_delay

Fetch messages with custom batch size and lock time.

```rust
// Fetch up to 50 messages with 30-second lock
let messages = consumer.dequeue_many_with_delay(50, 30).await?;
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `count` | `i32` | Maximum messages to fetch |
| `lock_time_seconds` | `i64` | Lock duration in seconds |

**Returns:** `Result<Vec<QueueMessage>>` - Available messages.

!!! tip "Lock Time Selection"
    Set lock time based on expected processing duration plus buffer:

    - Quick tasks (< 1s): 5-10 seconds
    - Normal tasks (1-10s): 30 seconds
    - Long tasks (> 10s): 60+ seconds, or extend as needed

### archive

Archive a processed message for audit trail retention.

```rust
let archived = consumer.archive(message.id).await?;

match archived {
    Some(archived_msg) => println!("Archived message {}", archived_msg.id),
    None => println!("Message not found or already archived"),
}
```

**Returns:** `Result<Option<ArchivedMessage>>` - The archived message if successful.

Archiving:
1. Copies the message to `pgqrs_archive` table
2. Deletes from `pgqrs_messages` table
3. Records `archived_at` timestamp

### archive_many

Archive multiple messages in a single transaction.

```rust
let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
let results = consumer.archive_many(ids).await?;

for (id, success) in message_ids.iter().zip(results.iter()) {
    if *success {
        println!("Archived message {}", id);
    } else {
        println!("Failed to archive message {}", id);
    }
}
```

**Returns:** `Result<Vec<bool>>` - Success status for each message.

### delete

Delete a message without archiving.

```rust
let deleted = consumer.delete(message.id).await?;
if deleted {
    println!("Message deleted");
}
```

**Returns:** `Result<bool>` - `true` if deleted, `false` if not found.

!!! warning
    Deleted messages are gone forever with no audit trail. Use `archive()` if you need to retain message history.

### delete_many

Delete multiple messages in a single transaction.

```rust
let ids = vec![1, 2, 3];
let results = consumer.delete_many(ids).await?;

let deleted_count = results.iter().filter(|&&r| r).count();
println!("Deleted {} messages", deleted_count);
```

**Returns:** `Result<Vec<bool>>` - Success status for each message.

## Worker Methods

`Consumer` implements the `Worker` trait:

```rust
use pgqrs::Worker;

// Get worker ID
let id = consumer.worker_id();

// Check status
let status = consumer.status().await?;

// Send heartbeat
consumer.heartbeat().await?;

// Check health
let healthy = consumer.is_healthy(chrono::Duration::minutes(5)).await?;

// Lifecycle operations
consumer.suspend().await?;  // Note: must have no pending messages
consumer.resume().await?;
consumer.shutdown().await?;
```

## Message Structure

The `QueueMessage` struct returned by dequeue:

```rust
pub struct QueueMessage {
    pub id: i64,              // Unique message ID
    pub queue_id: i64,        // Queue this message belongs to
    pub payload: Value,       // JSON payload
    pub enqueued_at: DateTime<Utc>,  // When created
    pub vt: DateTime<Utc>,    // Visibility timeout (lock expiry)
    pub read_ct: i32,         // Number of dequeue attempts
}
```

## Patterns

### Basic Consumer Loop

```rust
use std::time::Duration;

async fn consume_loop(consumer: &Consumer) -> Result<()> {
    loop {
        let messages = consumer.dequeue().await?;

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        for message in messages {
            match process_message(&message).await {
                Ok(_) => {
                    consumer.archive(message.id).await?;
                }
                Err(e) => {
                    tracing::error!("Failed to process {}: {}", message.id, e);
                    // Message will become available again after lock expires
                }
            }
        }
    }
}
```

### Consumer with Graceful Shutdown

```rust
use tokio::signal;
use tokio::sync::watch;

async fn run_consumer(consumer: Consumer) -> Result<()> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn shutdown handler
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        shutdown_tx.send(true).ok();
    });

    loop {
        // Check for shutdown
        if *shutdown_rx.borrow() {
            tracing::info!("Shutting down consumer...");
            break;
        }

        let messages = consumer.dequeue().await?;

        for message in messages {
            // Check shutdown between messages
            if *shutdown_rx.borrow() {
                break;
            }

            process_and_archive(&consumer, &message).await?;
        }

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // Graceful shutdown
    consumer.suspend().await?;
    consumer.shutdown().await?;

    Ok(())
}
```

### Dead Letter Queue Pattern

```rust
async fn process_with_dlq(
    consumer: &Consumer,
    dlq_producer: &Producer,
    max_retries: i32,
) -> Result<()> {
    let messages = consumer.dequeue().await?;

    for message in messages {
        if message.read_ct > max_retries {
            // Move to dead letter queue
            let dlq_payload = json!({
                "original_message": message.payload,
                "original_id": message.id,
                "read_count": message.read_ct,
                "moved_at": chrono::Utc::now().to_rfc3339()
            });

            dlq_producer.enqueue(&dlq_payload).await?;
            consumer.delete(message.id).await?;

            tracing::warn!("Moved message {} to DLQ after {} attempts",
                message.id, message.read_ct);
        } else {
            // Normal processing
            match process_message(&message).await {
                Ok(_) => consumer.archive(message.id).await?,
                Err(e) => {
                    tracing::warn!("Attempt {} failed for {}: {}",
                        message.read_ct, message.id, e);
                    // Let lock expire for retry
                }
            };
        }
    }

    Ok(())
}
```

### Parallel Processing

```rust
use futures::future::join_all;

async fn parallel_process(consumer: &Consumer) -> Result<()> {
    let messages = consumer.dequeue_many_with_delay(100, 60).await?;

    // Process all messages in parallel
    let futures: Vec<_> = messages
        .iter()
        .map(|m| async move {
            let result = process_message(m).await;
            (m.id, result)
        })
        .collect();

    let results = join_all(futures).await;

    // Archive successful, leave failed for retry
    let successful: Vec<i64> = results
        .iter()
        .filter(|(_, r)| r.is_ok())
        .map(|(id, _)| *id)
        .collect();

    if !successful.is_empty() {
        consumer.archive_many(successful).await?;
    }

    Ok(())
}
```

### Consumer with Heartbeat

```rust
use tokio::time::{interval, Duration};

async fn consume_with_heartbeat(consumer: Consumer) -> Result<()> {
    let mut heartbeat = interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                consumer.heartbeat().await?;
            }
            result = consumer.dequeue() => {
                let messages = result?;
                for message in messages {
                    process_and_archive(&consumer, &message).await?;
                }
            }
        }
    }
}
```

## Full Example

```rust
use pgqrs::{Admin, Consumer, Config, Worker};
use std::time::Duration;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Setup
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?;
    let queue = admin.get_queue("tasks").await?;

    // Create consumer
    let consumer = Consumer::new(
        admin.pool.clone(),
        &queue,
        &hostname::get()?.to_string_lossy(),
        3001,
        &config,
    ).await?;

    tracing::info!("Consumer started with worker ID: {}", consumer.worker_id());

    // Main processing loop
    let processing = async {
        loop {
            match consumer.dequeue_many_with_delay(10, 30).await {
                Ok(messages) if messages.is_empty() => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(messages) => {
                    tracing::info!("Processing {} messages", messages.len());

                    for message in messages {
                        tracing::debug!("Processing message {}", message.id);

                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        // Archive on success
                        consumer.archive(message.id).await?;
                    }
                }
                Err(e) => {
                    tracing::error!("Dequeue error: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), pgqrs::Error>(())
    };

    // Run until Ctrl+C
    tokio::select! {
        result = processing => {
            result?;
        }
        _ = signal::ctrl_c() => {
            tracing::info!("Shutting down...");
        }
    }

    // Graceful shutdown
    consumer.suspend().await?;
    consumer.shutdown().await?;
    tracing::info!("Consumer shut down gracefully");

    Ok(())
}
```

## See Also

- [Producer API](producer.md) - Creating messages
- [Admin API](admin.md) - Queue management
- [Message Lifecycle](../concepts/message-lifecycle.md) - Understanding message states
- [Worker Management Guide](../guides/worker-management.md) - Running multiple consumers
