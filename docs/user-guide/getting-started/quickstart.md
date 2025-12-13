# Quickstart

This guide will walk you through creating your first queue, sending messages, and processing them.

## Prerequisites

- pgqrs installed ([Installation Guide](installation.md))
- A running PostgreSQL database
- pgqrs schema installed (`pgqrs install`)

## Step 1: Create a Queue

=== "Rust"

    ```rust
    use pgqrs::{Admin, Config};

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;

        // Create a queue named "tasks"
        let queue = admin.create_queue("tasks").await?;
        println!("Created queue: {} (id: {})", queue.queue_name, queue.id);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Admin

    async def main():
        admin = Admin("postgresql://localhost/mydb")

        # Create a queue named "tasks"
        queue = await admin.create_queue("tasks")
        print(f"Created queue: {queue.queue_name} (id: {queue.id})")

    asyncio.run(main())
    ```

=== "CLI"

    ```bash
    pgqrs queue create tasks
    ```

## Step 2: Send a Message

=== "Rust"

    ```rust
    use pgqrs::{Admin, Producer, Config};
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;

        // Get the queue
        let queue = admin.get_queue("tasks").await?;

        // Create a producer
        let producer = Producer::new(
            admin.pool.clone(),
            &queue,
            "localhost",  // hostname for worker identification
            3000,         // port for worker identification
            &config,
        ).await?;

        // Send a message
        let payload = json!({
            "task_type": "send_email",
            "to": "user@example.com",
            "subject": "Welcome!"
        });

        let message = producer.enqueue(&payload).await?;
        println!("Sent message with ID: {}", message.id);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Admin, Producer

    async def main():
        admin = Admin("postgresql://localhost/mydb")

        # Create a producer for the "tasks" queue
        producer = Producer(
            "postgresql://localhost/mydb",
            "tasks",
            "localhost",  # hostname
            3000,         # port
        )

        # Send a message
        payload = {
            "task_type": "send_email",
            "to": "user@example.com",
            "subject": "Welcome!"
        }

        message_id = await producer.enqueue(payload)
        print(f"Sent message with ID: {message_id}")

    asyncio.run(main())
    ```

=== "CLI"

    ```bash
    pgqrs message enqueue --queue tasks --payload '{"task_type": "send_email", "to": "user@example.com"}'
    ```

## Step 3: Consume Messages

=== "Rust"

    ```rust
    use pgqrs::{Admin, Consumer, Config};
    use std::time::Duration;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;

        let queue = admin.get_queue("tasks").await?;

        // Create a consumer
        let consumer = Consumer::new(
            admin.pool.clone(),
            &queue,
            "localhost",
            3001,
            &config,
        ).await?;

        // Poll for messages
        loop {
            let messages = consumer.dequeue().await?;

            if messages.is_empty() {
                println!("No messages, waiting...");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            for message in messages {
                println!("Processing message {}: {:?}", message.id, message.payload);

                // Process the message...

                // Archive after successful processing
                consumer.archive(message.id).await?;
                println!("Archived message {}", message.id);
            }
        }
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Admin, Consumer

    async def main():
        admin = Admin("postgresql://localhost/mydb")

        # Create a consumer for the "tasks" queue
        consumer = Consumer(
            "postgresql://localhost/mydb",
            "tasks",
            "localhost",
            3001,
        )

        # Poll for messages
        while True:
            messages = await consumer.dequeue()

            if not messages:
                print("No messages, waiting...")
                await asyncio.sleep(2)
                continue

            for message in messages:
                print(f"Processing message {message.id}: {message.payload}")

                # Process the message...

                # Archive after successful processing
                await consumer.archive(message.id)
                print(f"Archived message {message.id}")

    asyncio.run(main())
    ```

=== "CLI"

    ```bash
    # Dequeue a single message
    pgqrs message dequeue --queue tasks --worker 1
    ```

## Step 4: Monitor Your Queue

=== "Rust"

    ```rust
    let metrics = admin.queue_metrics("tasks").await?;
    println!("Queue: {}", metrics.name);
    println!("  Pending: {}", metrics.pending_messages);
    println!("  Locked: {}", metrics.locked_messages);
    println!("  Archived: {}", metrics.archived_messages);
    ```

=== "CLI"

    ```bash
    pgqrs queue metrics tasks
    ```

    Output:
    ```
    ┌────────┬───────┬─────────┬────────┬──────────┐
    │ Queue  │ Total │ Pending │ Locked │ Archived │
    ├────────┼───────┼─────────┼────────┼──────────┤
    │ tasks  │ 100   │ 45      │ 5      │ 50       │
    └────────┴───────┴─────────┴────────┴──────────┘
    ```

## Complete Example

Here's a complete example showing a producer and consumer working together:

=== "Rust"

    ```rust
    use pgqrs::{Admin, Producer, Consumer, Config};
    use serde_json::json;
    use std::time::Duration;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;

        // Setup
        admin.install().await?;
        let queue = admin.create_queue("demo").await?;

        // Producer
        let producer = Producer::new(
            admin.pool.clone(), &queue, "localhost", 3000, &config
        ).await?;

        // Send messages
        for i in 0..5 {
            let msg = producer.enqueue(&json!({"task": i})).await?;
            println!("Sent task {}: message id {}", i, msg.id);
        }

        // Consumer
        let consumer = Consumer::new(
            admin.pool.clone(), &queue, "localhost", 3001, &config
        ).await?;

        // Process messages
        let messages = consumer.dequeue_many_with_delay(10, 5).await?;
        for message in messages {
            println!("Processing: {:?}", message.payload);
            consumer.archive(message.id).await?;
        }

        // Show metrics
        let metrics = admin.queue_metrics("demo").await?;
        println!("Archived: {}", metrics.archived_messages);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Admin, Producer, Consumer

    async def main():
        admin = Admin("postgresql://localhost/mydb")

        # Setup
        await admin.install()
        queue = await admin.create_queue("demo")

        # Producer
        producer = Producer("postgresql://localhost/mydb", "demo", "localhost", 3000)

        # Send messages
        for i in range(5):
            msg_id = await producer.enqueue({"task": i})
            print(f"Sent task {i}: message id {msg_id}")

        # Consumer
        consumer = Consumer("postgresql://localhost/mydb", "demo", "localhost", 3001)

        # Process messages
        messages = await consumer.dequeue()
        for message in messages:
            print(f"Processing: {message.payload}")
            await consumer.archive(message.id)

    asyncio.run(main())
    ```

## What's Next?

- [Architecture](../concepts/architecture.md) - Understand how pgqrs works internally
- [Producer API](../rust/producer.md) - Learn about batch operations and delayed messages
- [Consumer API](../rust/consumer.md) - Learn about batch processing and visibility timeouts
- [Worker Management](../guides/worker-management.md) - Scale your workers
