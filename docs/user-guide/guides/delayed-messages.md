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
    import asyncio
    import pgqrs
    from datetime import datetime, timedelta

    async def schedule_message():
        admin = pgqrs.Admin("postgresql://localhost/mydb")
        await admin.install()
        queue_name = "reminders"
        await admin.create_queue(queue_name)

        producer = pgqrs.Producer(admin, queue_name, "scheduler", 3000)

        # Schedule a reminder for 1 hour from now
        payload = {
            "type": "reminder",
            "message": "Follow up with customer",
            "customer_id": 12345
        }

        # enqueue_delayed takes delay in seconds (3600 = 1 hour)
        msg_id = await producer.enqueue_delayed(
            payload,
            delay_seconds=3600
        )

        print(f"Scheduled reminder {msg_id} for 1 hour from now")

    # Run the async function
    asyncio.run(schedule_message())
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

=== "Rust"

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

=== "Python"

    ```python
    import asyncio
    from datetime import datetime, timedelta
    import pgqrs

    def seconds_until_next_hour() -> int:
        """Calculate delay until the next hour."""
        now = datetime.utcnow()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return int((next_hour - now).total_seconds())

    def seconds_until_time(hour: int, minute: int = 0) -> int:
        """Calculate delay until specific time today (or tomorrow if passed)."""
        now = datetime.utcnow()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # If target time has passed today, schedule for tomorrow
        if target <= now:
            target += timedelta(days=1)

        return int((target - now).total_seconds())

    async def schedule_for_specific_times(producer):
        payload = {"task": "daily_report", "type": "scheduled"}

        # Schedule for next hour
        delay = seconds_until_next_hour()
        await producer.enqueue_delayed(payload, delay_seconds=delay)

        # Schedule for 9:00 AM
        delay = seconds_until_time(9, 0)
        await producer.enqueue_delayed(payload, delay_seconds=delay)

        print(f"Messages scheduled successfully")
    ```

## Use Cases

### Scheduled Email

=== "Rust"

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

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def schedule_welcome_email(
        producer: pgqrs.Producer,
        user_id: int,
        email: str
    ) -> int:
        """Schedule a welcome email 10 minutes after signup."""
        payload = {
            "type": "welcome_email",
            "user_id": user_id,
            "email": email,
            "template": "welcome_series_1"
        }

        # Send 10 minutes after signup (600 seconds)
        msg_id = await producer.enqueue_delayed(payload, delay_seconds=600)
        return msg_id

    async def schedule_follow_up_series(
        producer: pgqrs.Producer,
        user_id: int
    ) -> list[int]:
        """Schedule a follow-up email series over 14 days."""
        delays = [
            (1, 86400),      # Day 1 (24 hours)
            (3, 259200),     # Day 3 (72 hours)
            (7, 604800),     # Day 7 (1 week)
            (14, 1209600),   # Day 14 (2 weeks)
        ]

        message_ids = []

        for day, delay_seconds in delays:
            payload = {
                "type": "follow_up_email",
                "user_id": user_id,
                "series_day": day
            }

            msg_id = await producer.enqueue_delayed(payload, delay_seconds=delay_seconds)
            message_ids.append(msg_id)

            print(f"Scheduled follow-up email for day {day} (message {msg_id})")

        return message_ids

    # Usage example
    async def setup_user_emails(admin, user_id: int, email: str):
        producer = pgqrs.Producer(admin, "emails", "email-scheduler", 8080)

        # Welcome email
        welcome_id = await schedule_welcome_email(producer, user_id, email)
        print(f"Welcome email scheduled: {welcome_id}")

        # Follow-up series
        follow_up_ids = await schedule_follow_up_series(producer, user_id)
        print(f"Follow-up series scheduled: {follow_up_ids}")
    ```

### Retry with Backoff

=== "Rust"

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

=== "Python"

    ```python
    import asyncio
    import pgqrs
    from typing import Optional

    async def schedule_retry(
        producer: pgqrs.Producer,
        original_payload: dict,
        attempt: int
    ) -> Optional[int]:
        """Schedule a retry with exponential backoff."""
        MAX_ATTEMPTS = 5

        if attempt >= MAX_ATTEMPTS:
            print(f"Max attempts ({MAX_ATTEMPTS}) reached, giving up")
            return None

        # Exponential backoff: 1min, 4min, 16min, 64min
        delay_seconds = 60 * (4 ** attempt)

        retry_payload = {
            "original": original_payload,
            "attempt": attempt + 1,
            "scheduled_retry": True,
            "delay_applied": delay_seconds
        }

        msg_id = await producer.enqueue_delayed(retry_payload, delay_seconds=delay_seconds)

        print(f"Scheduled retry attempt {attempt + 1} in {delay_seconds}s (message {msg_id})")
        return msg_id

    async def handle_failed_task(
        producer: pgqrs.Producer,
        failed_payload: dict,
        error: str
    ):
        """Handle a failed task by scheduling retries."""
        attempt = failed_payload.get("attempt", 0)

        print(f"Task failed (attempt {attempt}): {error}")

        retry_id = await schedule_retry(producer, failed_payload, attempt)

        if retry_id is None:
            # Send to dead letter queue or alert
            await producer.enqueue({
                "type": "failed_permanently",
                "original_payload": failed_payload,
                "final_error": error,
                "total_attempts": attempt + 1
            })
            print("Task failed permanently, sent to dead letter processing")
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

=== "Rust"

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

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def process_scheduled_tasks(consumer: pgqrs.Consumer):
        """Process delayed messages as they become available."""
        while True:
            # dequeue() only returns messages where visibility timeout has passed
            messages = await consumer.dequeue()

            for message in messages:
                msg_type = message.payload.get("type")

                try:
                    if msg_type == "reminder":
                        await handle_reminder(message)
                    elif msg_type == "scheduled_email":
                        await handle_email(message)
                    elif msg_type == "follow_up_email":
                        await handle_follow_up_email(message)
                    elif msg_type == "retry" or message.payload.get("scheduled_retry"):
                        await handle_retry(message)
                    else:
                        print(f"⚠️ Unknown message type: {msg_type}")

                    # Archive successfully processed message
                    await consumer.archive(message.id)
                    print(f"✅ Processed {msg_type} message {message.id}")

                except Exception as e:
                    print(f"❌ Error processing message {message.id}: {e}")
                    # Consider extending visibility or moving to retry
                    await consumer.extend_visibility(message.id, 300)  # 5 min delay

            if not messages:
                await asyncio.sleep(1)  # Brief pause when no messages

    async def handle_reminder(message):
        """Process reminder messages."""
        payload = message.payload
        print(f"🔔 Reminder: {payload.get('message')}")
        print(f"   User ID: {payload.get('user_id')}")

    async def handle_email(message):
        """Process email messages."""
        payload = message.payload
        print(f"📧 Sending email to {payload.get('email')}")
        print(f"   Template: {payload.get('template')}")

    async def handle_follow_up_email(message):
        """Process follow-up email messages."""
        payload = message.payload
        print(f"📨 Sending follow-up email (day {payload.get('series_day')})")
        print(f"   User ID: {payload.get('user_id')}")

    async def handle_retry(message):
        """Process retry messages."""
        payload = message.payload
        attempt = payload.get('attempt', 1)
        print(f"🔄 Processing retry attempt {attempt}")
        print(f"   Original task: {payload.get('original', {}).get('type', 'unknown')}")
    ```

## Canceling Scheduled Messages

To cancel a scheduled message, delete it before it becomes visible:

=== "Rust"

    ```rust
    // Store the message ID when scheduling
    let msg = producer.enqueue_delayed(&payload, 3600).await?;
    let scheduled_id = msg.id;

    // Later, to cancel:
    consumer.delete(scheduled_id).await?;
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def schedule_and_cancel_example():
        admin = pgqrs.Admin("postgresql://localhost/mydb")
        producer = pgqrs.Producer(admin, "tasks", "scheduler", 8080)
        consumer = pgqrs.Consumer(admin, "tasks", "canceller", 8081)

        # Store the message ID when scheduling
        payload = {"task": "send_email", "user_id": 123}
        msg_id = await producer.enqueue_delayed(payload, delay_seconds=3600)
        print(f"Scheduled message {msg_id} for 1 hour from now")

        # Store the ID for later cancellation
        scheduled_ids = [msg_id]

        # Later, to cancel before it becomes visible:
        for msg_id in scheduled_ids:
            try:
                result = await consumer.delete(msg_id)
                if result:
                    print(f"✅ Cancelled scheduled message {msg_id}")
                else:
                    print(f"⚠️ Message {msg_id} not found (may have been processed)")
            except Exception as e:
                print(f"❌ Failed to cancel message {msg_id}: {e}")

    async def cancel_user_reminders(admin, user_id: int):
        """Cancel all scheduled reminders for a user."""
        # Note: This requires tracking message IDs per user
        # You might store these in your application database

        messages = await admin.get_messages()
        user_messages = await messages.filter_by_payload({"user_id": user_id})

        consumer = pgqrs.Consumer(admin, "reminders", "canceller", 8082)

        cancelled_count = 0
        for message in user_messages:
            try:
                await consumer.delete(message.id)
                cancelled_count += 1
            except Exception as e:
                print(f"Failed to cancel message {message.id}: {e}")

        print(f"Cancelled {cancelled_count} scheduled reminders for user {user_id}")
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

=== "Rust"

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

=== "Python"

    ```python
    import asyncio
    import pgqrs
    from datetime import datetime

    async def delayed_messages_demo():
        \"\"\"Complete example of scheduling and processing delayed messages.\"\"\"

        # Setup
        admin = pgqrs.Admin("postgresql://localhost/mydb")
        await admin.install()

        queue_name = "scheduled"
        await admin.create_queue(queue_name)

        producer = pgqrs.Producer(admin, queue_name, "scheduler", 3000)
        consumer = pgqrs.Consumer(admin, queue_name, "consumer", 3001)

        print("Scheduling messages...")

        # Schedule messages with different delays
        msg1 = await producer.enqueue_delayed({"msg": "5 seconds"}, delay_seconds=5)
        msg2 = await producer.enqueue_delayed({"msg": "10 seconds"}, delay_seconds=10)
        msg3 = await producer.enqueue_delayed({"msg": "15 seconds"}, delay_seconds=15)

        print(f"Scheduled 3 messages: {msg1}, {msg2}, {msg3}")
        print("Waiting for messages to become available...\\n")

        # Consume as they become available
        processed = 0
        start_time = datetime.utcnow()

        while processed < 3:
            messages = await consumer.dequeue()

            for message in messages:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                print(f"[{elapsed:4.1f}s] Received: {message.payload}")

                await consumer.archive(message.id)
                processed += 1

            if not messages:
                await asyncio.sleep(0.5)  # Brief pause when no messages

        print("\\nAll scheduled messages processed!")

    async def advanced_scheduling_demo():
        \"\"\"Demonstrate advanced scheduling patterns.\"\"\"

        admin = pgqrs.Admin("postgresql://localhost/mydb")
        queue_name = "advanced_scheduling"
        await admin.create_queue(queue_name)

        producer = pgqrs.Producer(admin, queue_name, "advanced-scheduler", 3100)

        print("Advanced scheduling demo...")

        # Schedule welcome email series
        user_id = 12345
        email_ids = await schedule_follow_up_series(producer, user_id)
        print(f"Scheduled follow-up series: {email_ids}")

        # Schedule with retry logic
        original_task = {"task": "process_payment", "amount": 99.99}
        retry_id = await schedule_retry(producer, original_task, attempt=0)
        print(f"Scheduled retry task: {retry_id}")

        # Schedule for specific time (next minute)
        from datetime import timedelta
        next_minute = datetime.utcnow() + timedelta(minutes=1)
        delay = int((next_minute - datetime.utcnow()).total_seconds())

        reminder_id = await producer.enqueue_delayed(
            {"type": "reminder", "message": "Time-based reminder"},
            delay_seconds=delay
        )
        print(f"Scheduled time-based reminder: {reminder_id}")

    if __name__ == "__main__":
        # Run the basic demo
        asyncio.run(delayed_messages_demo())

        # Uncomment to run advanced demo
        # asyncio.run(advanced_scheduling_demo())
    ```

## What's Next?

- [Worker Management](worker-management.md) - Scale your processing
- [Basic Workflow](basic-workflow.md) - Review the fundamentals
