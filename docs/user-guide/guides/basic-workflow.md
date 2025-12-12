# Basic Workflow Guide

This guide walks you through setting up a complete producer-consumer workflow with pgqrs.

## What You'll Build

A simple task queue system where:

1. A producer sends tasks to a queue
2. A consumer processes tasks and archives them
3. You monitor the queue status

## Prerequisites

- pgqrs installed
- PostgreSQL running
- Database connection string (DSN)

## Step 1: Set Up the Environment

First, install the schema and create a queue.

=== "Rust"

    ```rust
    use pgqrs::{Admin, Config};

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;

        // Install schema (idempotent)
        admin.install().await?;
        println!("✓ Schema installed");

        // Create queue
        let queue = admin.create_queue("tasks").await?;
        println!("✓ Created queue: {} (id: {})", queue.queue_name, queue.id);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Admin

    async def setup():
        admin = Admin("postgresql://localhost/mydb")

        # Install schema (idempotent)
        await admin.install()
        print("✓ Schema installed")

        # Create queue
        queue = await admin.create_queue("tasks")
        print(f"✓ Created queue: {queue.queue_name} (id: {queue.id})")

    asyncio.run(setup())
    ```

=== "CLI"

    ```bash
    # Set connection string
    export PGQRS_DSN="postgresql://localhost/mydb"

    # Install schema
    pgqrs install
    pgqrs verify

    # Create queue
    pgqrs queue create tasks
    ```

## Step 2: Create the Producer

The producer sends messages to the queue.

=== "Rust"

    ```rust
    // producer.rs
    use pgqrs::{Admin, Producer, Config};
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;
        let queue = admin.get_queue("tasks").await?;

        // Create producer
        let producer = Producer::new(
            admin.pool.clone(),
            &queue,
            "producer-host",
            3000,
            &config,
        ).await?;

        // Send tasks
        for i in 1..=10 {
            let payload = json!({
                "task_id": i,
                "type": "process_data",
                "data": {"value": i * 10}
            });

            let msg = producer.enqueue(&payload).await?;
            println!("Sent task {}: message id {}", i, msg.id);
        }

        println!("\n✓ Sent 10 tasks to queue");
        Ok(())
    }
    ```

=== "Python"

    ```python
    # producer.py
    import asyncio
    from pgqrs import Admin, Producer

    async def main():
        admin = Admin("postgresql://localhost/mydb")

        # Create producer
        producer = Producer(
            "postgresql://localhost/mydb",
            "tasks",
            "producer-host",
            3000,
        )

        # Send tasks
        for i in range(1, 11):
            payload = {
                "task_id": i,
                "type": "process_data",
                "data": {"value": i * 10}
            }

            msg_id = await producer.enqueue(payload)
            print(f"Sent task {i}: message id {msg_id}")

        print("\n✓ Sent 10 tasks to queue")

    asyncio.run(main())
    ```

## Step 3: Create the Consumer

The consumer processes messages and archives them.

=== "Rust"

    ```rust
    // consumer.rs
    use pgqrs::{Admin, Consumer, Config};
    use std::time::Duration;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;
        let queue = admin.get_queue("tasks").await?;

        // Create consumer
        let consumer = Consumer::new(
            admin.pool.clone(),
            &queue,
            "consumer-host",
            3001,
            &config,
        ).await?;

        println!("Consumer started. Waiting for messages...\n");

        let mut processed = 0;

        loop {
            // Fetch messages
            let messages = consumer.dequeue().await?;

            if messages.is_empty() {
                if processed > 0 {
                    println!("\nNo more messages. Processed {} tasks.", processed);
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Process each message
            for message in messages {
                let task_id = message.payload.get("task_id");
                let data = message.payload.get("data");

                println!("Processing task {:?}", task_id);
                println!("  Data: {:?}", data);

                // Simulate work
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Archive the message
                consumer.archive(message.id).await?;
                println!("  ✓ Archived");

                processed += 1;
            }
        }

        Ok(())
    }
    ```

=== "Python"

    ```python
    # consumer.py
    import asyncio
    from pgqrs import Admin, Consumer

    async def main():
        admin = Admin("postgresql://localhost/mydb")

        # Create consumer
        consumer = Consumer(
            "postgresql://localhost/mydb",
            "tasks",
            "consumer-host",
            3001,
        )

        print("Consumer started. Waiting for messages...\n")

        processed = 0

        while True:
            # Fetch messages
            messages = await consumer.dequeue()

            if not messages:
                if processed > 0:
                    print(f"\nNo more messages. Processed {processed} tasks.")
                    break
                await asyncio.sleep(1)
                continue

            # Process each message
            for message in messages:
                task_id = message.payload.get("task_id")
                data = message.payload.get("data")

                print(f"Processing task {task_id}")
                print(f"  Data: {data}")

                # Simulate work
                await asyncio.sleep(0.1)

                # Archive the message
                await consumer.archive(message.id)
                print("  ✓ Archived")

                processed += 1

    asyncio.run(main())
    ```

## Step 4: Monitor the Queue

Check the queue status during and after processing.

=== "Rust"

    ```rust
    // monitor.rs
    use pgqrs::{Admin, Config};

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;

        let metrics = admin.queue_metrics("tasks").await?;

        println!("Queue: {}", metrics.name);
        println!("  Total messages:  {}", metrics.total_messages);
        println!("  Pending:         {}", metrics.pending_messages);
        println!("  Locked:          {}", metrics.locked_messages);
        println!("  Archived:        {}", metrics.archived_messages);

        Ok(())
    }
    ```

=== "CLI"

    ```bash
    pgqrs queue metrics tasks
    ```

## Running the Complete Example

1. **Terminal 1 - Setup:**
   ```bash
   # Run setup (once)
   cargo run --example setup
   # or: python setup.py
   ```

2. **Terminal 2 - Start Consumer:**
   ```bash
   cargo run --example consumer
   # or: python consumer.py
   ```

3. **Terminal 3 - Run Producer:**
   ```bash
   cargo run --example producer
   # or: python producer.py
   ```

4. **Terminal 4 - Monitor:**
   ```bash
   pgqrs queue metrics tasks
   ```

## Complete Single-File Example

Here's everything in one file for easy testing:

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
        println!("✓ Setup complete\n");

        // Producer
        let producer = Producer::new(
            admin.pool.clone(), &queue, "localhost", 3000, &config
        ).await?;

        for i in 1..=5 {
            producer.enqueue(&json!({"task": i})).await?;
            println!("Produced task {}", i);
        }
        println!();

        // Consumer
        let consumer = Consumer::new(
            admin.pool.clone(), &queue, "localhost", 3001, &config
        ).await?;

        loop {
            let messages = consumer.dequeue().await?;
            if messages.is_empty() { break; }

            for msg in messages {
                println!("Consumed: {:?}", msg.payload);
                consumer.archive(msg.id).await?;
            }
        }

        // Metrics
        println!();
        let m = admin.queue_metrics("demo").await?;
        println!("Final: {} pending, {} archived",
            m.pending_messages, m.archived_messages);

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
        await admin.create_queue("demo")
        print("✓ Setup complete\n")

        # Producer
        producer = Producer(
            "postgresql://localhost/mydb", "demo", "localhost", 3000
        )

        for i in range(1, 6):
            await producer.enqueue({"task": i})
            print(f"Produced task {i}")
        print()

        # Consumer
        consumer = Consumer(
            "postgresql://localhost/mydb", "demo", "localhost", 3001
        )

        while True:
            messages = await consumer.dequeue()
            if not messages:
                break

            for msg in messages:
                print(f"Consumed: {msg.payload}")
                await consumer.archive(msg.id)

        # Stats
        print()
        messages = await admin.get_messages()
        archive = await admin.get_archive()
        print(f"Final: {await messages.count()} pending, {await archive.count()} archived")

    asyncio.run(main())
    ```

## What's Next?

- [Batch Processing](batch-processing.md) - Process messages efficiently at scale
- [Delayed Messages](delayed-messages.md) - Schedule tasks for later
- [Worker Management](worker-management.md) - Run multiple consumers
