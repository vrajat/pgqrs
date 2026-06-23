# pgqrs

**pgqrs is a postgres-native, library-only durable execution engine.**

Written in Rust with Python bindings. Built for PostgreSQL.

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses.
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Postgres-only runtime:** one supported persistence model across Rust and Python
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

## Why PostgreSQL

| Scenario | Why PostgreSQL fits |
|----------|---------------------|
| Production with multiple workers | Full concurrency, no single-writer lock bottleneck |
| Durable workflows | Queue and workflow state share one transactional store |
| Local development | Same operational model as production |
| High write throughput | Better scaling characteristics under concurrent workers |

### Benchmark Highlights

Current queue benchmark baselines show:

- **PostgreSQL is the supported benchmark baseline**
- **Higher consumer counts generally improve throughput** until another bottleneck dominates
- **The published benchmarks match the supported runtime**

[:octicons-arrow-right-24: See benchmark methodology and scenario writeups](benchmarks/index.md)

## Job Queue

Simple, reliable message queue for background processing:

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;
        pgqrs::admin(&store).install().await?;
        store.queue("tasks").await?;

        let ids = pgqrs::enqueue()
            .message(&json!({"task": "send_email", "to": "user@example.com"}))
            .to("tasks")
            .execute(&store)
            .await?;
        println!("Enqueued: {:?}", ids);

        pgqrs::dequeue()
            .from("tasks")
            .handle(|msg| async move {
                println!("Processing: {:?}", msg.payload);
                Ok(())
            })
            .execute(&store)
            .await?;

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
        await admin.install()
        await store.queue("tasks")

        producer = await store.producer("tasks")
        msg_id = await producer.enqueue({
            "task": "send_email",
            "to": "user@example.com"
        })
        print(f"Enqueued: {msg_id}")

        consumer = await store.consumer("tasks")
        messages = await consumer.dequeue(batch_size=1)
        for msg in messages:
            print(f"Processing: {msg.payload}")
            await consumer.archive(msg.id)

    asyncio.run(main())
    ```

[:octicons-arrow-right-24: Learn more about Producer & Consumer](user-guide/concepts/producer-consumer.md)
