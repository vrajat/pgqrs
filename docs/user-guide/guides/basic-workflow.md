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
    use pgqrs;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Install schema (idempotent)
        pgqrs::admin(&store).install().await?;
        println!("✓ Schema installed");

        // Create queue
        let queue = pgqrs::admin(&store).create_queue("tasks").await?;
        println!("✓ Created queue: {} (id: {})", queue.queue_name, queue.id);

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def setup():
        store = await pgqrs.connect("postgresql://localhost/mydb")
        admin = pgqrs.admin(store)

        # Install schema (idempotent)
        await admin.install()
        print("✓ Schema installed")

        # Create queue
        queue = await admin.create_queue("tasks")
        print(f"✓ Created queue: {queue.queue_name} (id: {queue.id})")

    asyncio.run(setup())
    ```


## Step 2: Create the Producer

The producer sends messages to the queue.

=== "Rust"

    ```rust
    // producer.rs
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Create producer
        let producer = pgqrs::producer()
            .queue("tasks")
            .hostname("producer-app")
            .create(&store)
            .await?;

        // Send tasks
        for i in 1..=10 {
            let payload = json!({
                "task_id": i,
                "type": "process_data",
                "data": {"value": i * 10}
            });

            // Use producer to enqueue (ensures worker attribution)
            let ids = producer.enqueue(&payload).await?;
            println!("Sent task {}: message id {:?}", i, ids);
        }

        println!("\n✓ Sent 10 tasks to queue");
        Ok(())
    }
    ```

=== "Python"

    ```python
    # producer.py
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Create managed producer
        producer = await store.producer("tasks")

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
    use pgqrs;
    use std::time::Duration;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Create consumer
        let consumer = pgqrs::consumer()
            .queue("tasks")
            .hostname("consumer-app")
            .create(&store)
            .await?;

        println!("Consumer started. Waiting for messages...\n");

        let mut processed = 0;

        loop {
            // Fetch and process messages
            let result = consumer.dequeue()
                .batch(10)
                .handle_batch(|messages| async move {
                    for message in &messages {
                        let task_id = message.payload.get("task_id");
                        let data = message.payload.get("data");

                        println!("Processing task {:?}", task_id);
                        println!("  Data: {:?}", data);

                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        println!("  ✓ Processed");
                    }
                    Ok(())
                })
                .await;

            match result {
                Ok(_) => processed += 1,
                Err(_) => {
                    if processed > 0 {
                        println!("\nNo more messages. Processed {} batches.", processed);
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(())
    }
    ```

=== "Python"

    ```python
    # consumer.py
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Create managed consumer
        consumer = await store.consumer("tasks")

        print("Consumer started. Waiting for messages...\n")

        processed = 0

        while True:
            # Process messages with handler
            try:
                # Use consumer object directly
                await consumer.consume_batch(batch_size=10, handler=process_batch)
                processed += 1
            except:
                if processed > 0:
                    print(f"\nNo more messages. Processed {processed} batches.")
                    break
                await asyncio.sleep(1)

    async def process_batch(messages):
        for message in messages:
            task_id = message.payload.get("task_id")
            data = message.payload.get("data")

            print(f"Processing task {task_id}")
            print(f"  Data: {data}")

            # Simulate work
            await asyncio.sleep(0.1)
            print("  ✓ Processed")
        return True

    asyncio.run(main())
    ```

## Step 4: Monitor the Queue

Check the queue status during and after processing.

=== "Rust"

    ```rust
    // monitor.rs
    use pgqrs;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        let metrics = pgqrs::admin(&store).all_queues_metrics().await?;

        for m in metrics {
            if m.name == "tasks" {
                println!("Queue: {}", m.name);
                println!("  Total messages:  {}", m.total_messages);
                println!("  Pending:         {}", m.pending_messages);
                println!("  Locked:          {}", m.locked_messages);
                println!("  Archived:        {}", m.archived_messages);
            }
        }

        Ok(())
    }
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
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Setup
        pgqrs::admin(&store).install().await?;
        pgqrs::admin(&store).create_queue("demo").await?;
        println!("✓ Setup complete\n");

        // Create producer
        let producer = pgqrs::producer()
            .queue("demo")
            .hostname("demo-producer")
            .create(&store)
            .await?;

        // Producer - send 5 tasks
        for i in 1..=5 {
            producer.enqueue(&json!({"task": i})).await?;
            println!("Produced task {}", i);
        }
        println!();

        // Create consumer
        let consumer = pgqrs::consumer()
            .queue("demo")
            .hostname("demo-consumer")
            .create(&store)
            .await?;

        // Consumer - process all tasks
        loop {
            let result = consumer.dequeue()
                .handle(|msg| async move {
                    println!("Consumed: {:?}", msg.payload);
                    Ok(())
                })
                .await;

            if result.is_err() {
                break;
            }
        }

        // Metrics
        println!();
        let metrics = pgqrs::admin(&store).all_queues_metrics().await?;
        for m in metrics {
            if m.name == "demo" {
                println!("Final: {} pending, {} archived",
                    m.pending_messages, m.archived_messages);
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
        await admin.create_queue("demo")
        print("✓ Setup complete\n")

        # Create managed workers
        producer = await store.producer("demo")
        consumer = await store.consumer("demo")

        # Producer - send 5 tasks
        for i in range(1, 6):
            await producer.enqueue({"task": i})
            print(f"Produced task {i}")
        print()

        # Consumer - process all tasks
        processed = 0
        while True:
            try:
                # Use lambda or regular function handler
                await consumer.consume(handler=lambda msg: print(f"Consumed: {msg.payload}") or True)
                processed += 1
            except:
                break

        # Stats
        print()
        queues = await admin.get_queues()
        metrics = await queues.list_metrics()
        for m in metrics:
            if m["name"] == "demo":
                print(f"Final: {m['pending_messages']} pending, {m['archived_messages']} archived")

    asyncio.run(main())
    ```

## What's Next?

- [Batch Processing](batch-processing.md) - Process messages efficiently at scale
- [Delayed Messages](delayed-messages.md) - Schedule tasks for later
- [Worker Management](worker-management.md) - Run multiple consumers
