# Delayed Messages Guide

This guide covers scheduling messages for future processing using pgqrs's delayed message feature.

## What Are Delayed Messages?

Delayed messages are messages that won't be visible to consumers until a specified time has passed. Use them for:

- Scheduled tasks (send email in 1 hour)
- Reminders (follow up in 3 days)
- Rate limiting (retry after 5 minutes)
- Deferred processing (process at off-peak hours)

## Basic Usage

=== "Rust"

    ```rust
    use pgqrs::{Admin, Producer, Config};
    use serde_json::json;

    async fn schedule_message() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;
        let queue = admin.get_queue("reminders").await?;

        let producer = Producer::new(
            admin.pool.clone(), &queue, "scheduler", 3000, &config
        ).await?;

        // Schedule a reminder for 1 hour from now
        let payload = json!({
            "type": "reminder",
            "message": "Follow up with customer",
            "customer_id": 12345
        });

        let msg = producer.enqueue_delayed(
            &payload,
            3600  // 3600 seconds = 1 hour
        ).await?;

        println!("Scheduled reminder {} for: {}", msg.id, msg.vt);

        Ok(())
    }
    ```

=== "Python"

    ```python
    # Note: Delayed messages require the Rust Producer API
    # In Python, you can simulate with custom payload + filtering

    import asyncio
    from datetime import datetime, timedelta
    from pgqrs import Producer

    async def schedule_with_custom_field():
        producer = Producer(
            "postgresql://localhost/mydb",
            "reminders",
            "scheduler",
            3000,
        )

        # Add scheduled time to payload
        scheduled_for = datetime.utcnow() + timedelta(hours=1)

        msg_id = await producer.enqueue({
            "type": "reminder",
            "message": "Follow up with customer",
            "customer_id": 12345,
            "scheduled_for": scheduled_for.isoformat()
        })

        print(f"Created reminder {msg_id} for: {scheduled_for}")

    asyncio.run(schedule_with_custom_field())
    ```

## Common Delay Patterns

### Fixed Delays

```rust
// 5 minutes
producer.enqueue_delayed(&payload, 300).await?;

// 1 hour
producer.enqueue_delayed(&payload, 3600).await?;

// 24 hours
producer.enqueue_delayed(&payload, 86400).await?;

// 7 days
producer.enqueue_delayed(&payload, 604800).await?;
```

### Dynamic Delays

```rust
use chrono::{Utc, Duration, Timelike};

// Delay until next hour
fn seconds_until_next_hour() -> i64 {
    let now = Utc::now();
    let next_hour = (now + Duration::hours(1))
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap();
    (next_hour - now).num_seconds()
}

// Delay until specific time today
fn seconds_until_time(hour: u32, minute: u32) -> i64 {
    let now = Utc::now();
    let target = now
        .with_hour(hour).unwrap()
        .with_minute(minute).unwrap()
        .with_second(0).unwrap();

    let target = if target <= now {
        target + Duration::days(1)  // Tomorrow if time has passed
    } else {
        target
    };

    (target - now).num_seconds()
}

// Usage
let delay = seconds_until_time(9, 0);  // Next 9:00 AM
producer.enqueue_delayed(&payload, delay).await?;
```

## Use Cases

### Scheduled Email

```rust
async fn schedule_welcome_email(
    producer: &Producer,
    user_id: i64,
    email: &str,
) -> Result<i64> {
    let payload = json!({
        "type": "welcome_email",
        "user_id": user_id,
        "email": email,
        "template": "welcome_series_1"
    });

    // Send 10 minutes after signup
    let msg = producer.enqueue_delayed(&payload, 600).await?;
    Ok(msg.id)
}

async fn schedule_follow_up_series(
    producer: &Producer,
    user_id: i64,
) -> Result<Vec<i64>> {
    let delays = vec![
        (1, 86400),      // Day 1
        (3, 259200),     // Day 3
        (7, 604800),     // Day 7
        (14, 1209600),   // Day 14
    ];

    let mut message_ids = Vec::new();

    for (day, delay) in delays {
        let payload = json!({
            "type": "follow_up_email",
            "user_id": user_id,
            "series_day": day
        });

        let msg = producer.enqueue_delayed(&payload, delay).await?;
        message_ids.push(msg.id);
    }

    Ok(message_ids)
}
```

### Retry with Backoff

```rust
async fn schedule_retry(
    producer: &Producer,
    original_payload: &Value,
    attempt: u32,
) -> Result<Option<i64>> {
    const MAX_ATTEMPTS: u32 = 5;

    if attempt >= MAX_ATTEMPTS {
        return Ok(None);  // Give up
    }

    // Exponential backoff: 1min, 4min, 16min, 64min
    let delay = 60 * 4_i64.pow(attempt);

    let retry_payload = json!({
        "original": original_payload,
        "attempt": attempt + 1,
        "scheduled_retry": true
    });

    let msg = producer.enqueue_delayed(&retry_payload, delay).await?;
    Ok(Some(msg.id))
}
```

### Rate-Limited Processing

```rust
async fn rate_limited_enqueue(
    producer: &Producer,
    payloads: Vec<Value>,
    rate_per_minute: usize,
) -> Result<Vec<i64>> {
    let interval_seconds = 60 / rate_per_minute;
    let mut message_ids = Vec::new();

    for (i, payload) in payloads.into_iter().enumerate() {
        let delay = (i * interval_seconds) as i64;

        let msg = if delay == 0 {
            producer.enqueue(&payload).await?
        } else {
            producer.enqueue_delayed(&payload, delay).await?
        };

        message_ids.push(msg.id);
    }

    Ok(message_ids)
}
```

### Reminder System

```rust
struct ReminderScheduler {
    producer: Producer,
}

impl ReminderScheduler {
    async fn schedule_reminder(
        &self,
        user_id: i64,
        message: &str,
        remind_at: DateTime<Utc>,
    ) -> Result<i64> {
        let now = Utc::now();
        let delay = (remind_at - now).num_seconds().max(0);

        let payload = json!({
            "type": "reminder",
            "user_id": user_id,
            "message": message,
            "remind_at": remind_at.to_rfc3339()
        });

        let msg = self.producer.enqueue_delayed(&payload, delay).await?;
        Ok(msg.id)
    }

    async fn schedule_recurring(
        &self,
        user_id: i64,
        message: &str,
        interval_hours: i64,
        occurrences: usize,
    ) -> Result<Vec<i64>> {
        let mut message_ids = Vec::new();

        for i in 0..occurrences {
            let delay = interval_hours * 3600 * (i as i64 + 1);

            let payload = json!({
                "type": "recurring_reminder",
                "user_id": user_id,
                "message": message,
                "occurrence": i + 1,
                "total": occurrences
            });

            let msg = self.producer.enqueue_delayed(&payload, delay).await?;
            message_ids.push(msg.id);
        }

        Ok(message_ids)
    }
}
```

## Processing Delayed Messages

Consumers automatically handle delayed messages—they simply won't see them until the delay expires.

```rust
async fn process_scheduled_tasks(consumer: &Consumer) {
    loop {
        // dequeue() only returns messages where visibility timeout has passed
        let messages = consumer.dequeue().await?;

        for message in messages {
            let msg_type = message.payload.get("type").and_then(|v| v.as_str());

            match msg_type {
                Some("reminder") => handle_reminder(&message).await?,
                Some("scheduled_email") => handle_email(&message).await?,
                Some("retry") => handle_retry(&message).await?,
                _ => tracing::warn!("Unknown message type: {:?}", msg_type),
            }

            consumer.archive(message.id).await?;
        }

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
```

## Canceling Scheduled Messages

To cancel a scheduled message, delete it before it becomes visible:

```rust
// Store the message ID when scheduling
let msg = producer.enqueue_delayed(&payload, 3600).await?;
let scheduled_id = msg.id;

// Later, to cancel:
consumer.delete(scheduled_id).await?;
```

## Monitoring Scheduled Messages

Use CLI to check scheduled messages:

```bash
# List messages (shows visibility timeout)
pgqrs message list --queue reminders

# Check queue metrics
pgqrs queue metrics reminders
```

## Best Practices

1. **Store message IDs** - Keep track of scheduled message IDs for cancellation
2. **Use meaningful payloads** - Include scheduled time in payload for debugging
3. **Handle timezone carefully** - Always use UTC for delays
4. **Set reasonable maximums** - Don't schedule too far in advance
5. **Monitor the queue** - Watch for growing backlog of scheduled messages

## Complete Example

```rust
use pgqrs::{Admin, Producer, Consumer, Config};
use serde_json::json;
use chrono::{Utc, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?;

    admin.install().await?;
    let queue = admin.create_queue("scheduled").await?;

    let producer = Producer::new(
        admin.pool.clone(), &queue, "scheduler", 3000, &config
    ).await?;

    // Schedule messages with different delays
    println!("Scheduling messages...");

    producer.enqueue_delayed(&json!({"msg": "5 seconds"}), 5).await?;
    producer.enqueue_delayed(&json!({"msg": "10 seconds"}), 10).await?;
    producer.enqueue_delayed(&json!({"msg": "15 seconds"}), 15).await?;

    println!("Scheduled 3 messages. Waiting...\n");

    // Consume as they become available
    let consumer = Consumer::new(
        admin.pool.clone(), &queue, "consumer", 3001, &config
    ).await?;

    let mut processed = 0;
    let start = Utc::now();

    while processed < 3 {
        let messages = consumer.dequeue().await?;

        for message in messages {
            let elapsed = (Utc::now() - start).num_seconds();
            println!("[{:2}s] Received: {:?}", elapsed, message.payload);
            consumer.archive(message.id).await?;
            processed += 1;
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    println!("\nAll scheduled messages processed!");

    Ok(())
}
```

## What's Next?

- [Worker Management](worker-management.md) - Scale your processing
- [Basic Workflow](basic-workflow.md) - Review the fundamentals
