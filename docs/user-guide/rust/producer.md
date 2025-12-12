# Producer API

The `Producer` is responsible for creating and enqueueing messages to a queue.

## Creating a Producer

```rust
use pgqrs::{Admin, Producer, Config};

let config = Config::from_dsn("postgresql://localhost/mydb");
let admin = Admin::new(&config).await?;
let queue = admin.get_queue("tasks").await?;

let producer = Producer::new(
    admin.pool.clone(),  // Database connection pool
    &queue,              // Queue to produce to
    "my-service",        // Hostname for worker identification
    3000,                // Port for worker identification
    &config,             // Configuration
).await?;
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pool` | `PgPool` | SQLx PostgreSQL connection pool |
| `queue` | `&QueueInfo` | Queue to produce messages to |
| `hostname` | `&str` | Hostname for worker identification |
| `port` | `i32` | Port for worker identification |
| `config` | `&Config` | pgqrs configuration |

The hostname and port are used to identify this producer as a worker in the database.

## Methods

### enqueue

Enqueue a single message to the queue.

```rust
use serde_json::json;

let payload = json!({
    "action": "send_email",
    "to": "user@example.com",
    "subject": "Welcome!"
});

let message = producer.enqueue(&payload).await?;

println!("Message ID: {}", message.id);
println!("Enqueued at: {}", message.enqueued_at);
```

**Returns:** `Result<QueueMessage>` - The created message with its ID and metadata.

### batch_enqueue

Enqueue multiple messages in a single transaction.

```rust
let payloads = vec![
    json!({"user_id": 1, "action": "welcome_email"}),
    json!({"user_id": 2, "action": "welcome_email"}),
    json!({"user_id": 3, "action": "welcome_email"}),
];

let messages = producer.batch_enqueue(&payloads).await?;

println!("Enqueued {} messages", messages.len());
for msg in &messages {
    println!("  ID: {}", msg.id);
}
```

**Returns:** `Result<Vec<QueueMessage>>` - All created messages.

!!! tip "Performance"
    Batch enqueue is more efficient than multiple single enqueues because it uses a single database transaction.

### enqueue_delayed

Enqueue a message that won't be visible until after a delay.

```rust
let payload = json!({
    "reminder": "Follow up with customer",
    "customer_id": 12345
});

// Message available after 5 minutes (300 seconds)
let message = producer.enqueue_delayed(&payload, 300).await?;

println!("Message {} will be available at {}", message.id, message.vt);
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `payload` | `&Value` | JSON payload |
| `delay_seconds` | `i64` | Seconds until message is visible |

**Returns:** `Result<QueueMessage>` - The created message with future visibility timeout.

### extend_visibility

Extend the lock on a message being processed.

```rust
// Consumer dequeued message with ID 42
// Processing is taking longer than expected...

// Extend lock by 30 more seconds
let extended = producer.extend_visibility(42, 30).await?;

if extended {
    println!("Lock extended, continue processing...");
} else {
    println!("Failed to extend - message may have been released");
}
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `message_id` | `i64` | ID of the message |
| `duration_seconds` | `i64` | Additional seconds to lock |

**Returns:** `Result<bool>` - `true` if extended, `false` if message not found or already released.

!!! warning
    You can only extend visibility on messages that are currently locked (being processed).

## Worker Methods

`Producer` implements the `Worker` trait, giving access to worker lifecycle methods:

```rust
use pgqrs::Worker;

// Get worker ID
let id = producer.worker_id();

// Check status
let status = producer.status().await?;

// Send heartbeat
producer.heartbeat().await?;

// Check if healthy (last heartbeat within 5 minutes)
let healthy = producer.is_healthy(chrono::Duration::minutes(5)).await?;

// Lifecycle operations
producer.suspend().await?;
producer.resume().await?;
producer.shutdown().await?;
```

See [Workers](../concepts/workers.md) for details on worker lifecycle.

## Patterns

### Async Producer Service

```rust
use std::sync::Arc;

struct EmailService {
    producer: Arc<Producer>,
}

impl EmailService {
    async fn new(admin: &Admin, queue: &QueueInfo) -> Result<Self> {
        let producer = Producer::new(
            admin.pool.clone(),
            queue,
            "email-service",
            3000,
            &admin.config,
        ).await?;

        Ok(Self {
            producer: Arc::new(producer),
        })
    }

    async fn send_welcome(&self, user_id: i64) -> Result<i64> {
        let payload = json!({
            "type": "welcome",
            "user_id": user_id
        });
        let msg = self.producer.enqueue(&payload).await?;
        Ok(msg.id)
    }

    async fn send_batch(&self, user_ids: Vec<i64>) -> Result<Vec<i64>> {
        let payloads: Vec<_> = user_ids
            .iter()
            .map(|id| json!({"type": "welcome", "user_id": id}))
            .collect();

        let messages = self.producer.batch_enqueue(&payloads).await?;
        Ok(messages.iter().map(|m| m.id).collect())
    }
}
```

### Rate-Limited Producer

```rust
use tokio::time::{sleep, Duration};

async fn enqueue_with_rate_limit(
    producer: &Producer,
    payloads: Vec<Value>,
    rate_per_second: usize,
) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    let delay = Duration::from_secs(1) / rate_per_second as u32;

    for payload in payloads {
        let msg = producer.enqueue(&payload).await?;
        ids.push(msg.id);
        sleep(delay).await;
    }

    Ok(ids)
}
```

### Producer with Retry

```rust
use tokio::time::{sleep, Duration};

async fn enqueue_with_retry(
    producer: &Producer,
    payload: &Value,
    max_retries: u32,
) -> Result<QueueMessage> {
    let mut attempts = 0;

    loop {
        match producer.enqueue(payload).await {
            Ok(msg) => return Ok(msg),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                tracing::warn!("Enqueue failed, retry {}/{}: {}", attempts, max_retries, e);
                sleep(Duration::from_millis(100 * 2_u64.pow(attempts))).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Full Example

```rust
use pgqrs::{Admin, Producer, Config, Worker};
use serde_json::json;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?;
    let queue = admin.create_queue("notifications").await?;

    // Create producer
    let producer = Producer::new(
        admin.pool.clone(),
        &queue,
        "notification-service",
        3000,
        &config,
    ).await?;

    println!("Producer worker ID: {}", producer.worker_id());

    // Enqueue immediate messages
    let msg1 = producer.enqueue(&json!({
        "type": "email",
        "to": "user@example.com"
    })).await?;
    println!("Sent immediate message: {}", msg1.id);

    // Enqueue delayed message
    let msg2 = producer.enqueue_delayed(&json!({
        "type": "reminder",
        "to": "user@example.com"
    }), 3600).await?;
    println!("Sent delayed message: {} (available in 1 hour)", msg2.id);

    // Batch enqueue
    let batch = producer.batch_enqueue(&vec![
        json!({"type": "sms", "to": "+1234567890"}),
        json!({"type": "sms", "to": "+0987654321"}),
    ]).await?;
    println!("Sent batch of {} messages", batch.len());

    // Graceful shutdown on Ctrl+C
    signal::ctrl_c().await?;
    producer.suspend().await?;
    producer.shutdown().await?;
    println!("Producer shut down gracefully");

    Ok(())
}
```

## See Also

- [Consumer API](consumer.md) - Processing messages
- [Admin API](admin.md) - Queue management
- [Batch Processing Guide](../guides/batch-processing.md) - High-throughput patterns
- [Delayed Messages Guide](../guides/delayed-messages.md) - Scheduling messages
