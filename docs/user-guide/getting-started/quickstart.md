# Quickstart

This guide will walk you through creating your first queue, sending messages, and processing them.

## Prerequisites

- pgqrs installed ([Configuration](../api/configuration.md))
- A running PostgreSQL database
- pgqrs schema installed (`pgqrs install`)

## Step 1: Create a Queue

=== "Rust"

    ```rust
    use pgqrs;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Create a queue named "tasks"
        let queue = store.queue("tasks").await?;
        println!("Created queue: {} (id: {})", queue.queue_name, queue.id);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        admin = pgqrs.admin(store)

        # Create a queue named "tasks"
        queue = await store.queue("tasks")
        print(f"Created queue: {queue.queue_name} (id: {queue.id})")

    asyncio.run(main())
    ```

## Step 2: Send a Message

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Send a message using the high-level API
        let payload = json!({
            "task_type": "send_email",
            "to": "user@example.com",
            "subject": "Welcome!"
        });

        let ids = pgqrs::enqueue()
            .message(&payload)
            .to("tasks")
            .execute(&store)
            .await?;

        println!("Sent message with ID: {:?}", ids);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Send a message using the high-level API
        payload = {
            "task_type": "send_email",
            "to": "user@example.com",
            "subject": "Welcome!"
        }

        message_id = await pgqrs.produce(store, "tasks", payload)
        print(f"Sent message with ID: {message_id}")

    asyncio.run(main())
    ```

## Step 3: Consume Messages

=== "Rust"

    ```rust
    use pgqrs;
    use std::time::Duration;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Process messages with automatic lifecycle management
        loop {
            let result = pgqrs::dequeue()
                .from("tasks")
                .handle(|msg| async move {
                    println!("Processing message {}: {:?}", msg.id, msg.payload);
                    // Your processing logic here
                    Ok(())
                })
                .execute(&store)
                .await;

            if result.is_err() {
                println!("No messages, waiting...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Process messages with automatic lifecycle management
        async def handler(msg):
            print(f"Processing message {msg.id}: {msg.payload}")
            # Your processing logic here
            return True

        while True:
            try:
                await pgqrs.consume(store, "tasks", handler)
            except:
                print("No messages, waiting...")
                await asyncio.sleep(2)

    asyncio.run(main())
    ```

## Step 4: Monitor Your Queue

=== "Rust"

    ```rust
    let metrics = pgqrs::admin(&store).all_queues_metrics().await?;
    for m in metrics {
        if m.name == "tasks" {
            println!("Queue: {}", m.name);
            println!("  Pending: {}", m.pending_messages);
            println!("  Locked: {}", m.locked_messages);
            println!("  Archived: {}", m.archived_messages);
        }
    }
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
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Setup
        pgqrs::admin(&store).install().await?;
        store.queue("demo").await?;

        // Producer - send messages
        for i in 0..5 {
            pgqrs::enqueue()
                .message(&json!({"task": i}))
                .to("demo")
                .execute(&store)
                .await?;
            println!("Sent task {}", i);
        }

        // Consumer - process messages
        loop {
            let result = pgqrs::dequeue()
                .from("demo")
                .handle(|msg| async move {
                    println!("Processing: {:?}", msg.payload);
                    Ok(())
                })
                .execute(&store)
                .await;

            if result.is_err() {
                break;
            }
        }

        // Show metrics
        let metrics = pgqrs::admin(&store).all_queues_metrics().await?;
        for m in metrics {
            if m.name == "demo" {
                println!("Archived: {}", m.archived_messages);
            }
        }

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        admin = pgqrs.admin(store)

        # Setup
        await admin.install()
        await store.queue("demo")

        # Producer - send messages
        for i in range(5):
            await pgqrs.produce(store, "demo", {"task": i})
            print(f"Sent task {i}")

        # Consumer - process messages
        processed = 0
        while processed < 5:
            try:
                await pgqrs.consume(store, "demo", lambda msg: print(f"Processing: {msg.payload}") or True)
                processed += 1
            except:
                break

        # Show metrics
        queues = await admin.get_queues()
        metrics = await queues.list_metrics()
        for m in metrics:
            if m["name"] == "demo":
                print(f"Archived: {m['archived_messages']}")

    asyncio.run(main())
    ```

## What's Next?

- [Workflow API](../api/workflows.md): Detailed API reference
- [Producer API](../api/producer.md) - Learn about batch operations and delayed messages
- [Consumer API](../api/consumer.md) - Learn about batch processing and visibility timeouts
- [Worker Management](../guides/worker-management.md) - Scale your workers
