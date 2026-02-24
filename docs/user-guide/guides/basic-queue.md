# Basic Queue (Producer + Consumer)

This guide shows the smallest end-to-end setup: enqueue JSON work from a producer, process it with a consumer, and shut down cleanly.

It is intentionally "low level" (queue primitives), and complements the workflow-focused guide.

## Prerequisites

- pgqrs installed
- A database backend selected (examples use SQLite for simplicity)
- Schema installed (`admin.install()`)

!!! note

    The code below uses the fluent builder APIs (`enqueue()` / `dequeue()`). These match the Rust API and keep the docs symmetric.

## Step 1: Create a Queue

=== "Rust"

    ```rust
    use pgqrs;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("sqlite::memory:").await?;
        pgqrs::admin(&store).install().await?;

        let queue = "emails";
        pgqrs::admin(&store).create_queue(queue).await?;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("sqlite::memory:")
        await pgqrs.admin(store).install()

        queue = "emails"
        await pgqrs.admin(store).create_queue(queue)

    asyncio.run(main())
    ```

## Step 2: Create a Producer and Enqueue Work

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("sqlite::memory:").await?;
        pgqrs::admin(&store).install().await?;

        let queue = "emails";
        pgqrs::admin(&store).create_queue(queue).await?;

        let producer = pgqrs::producer("app", 9001, queue)
            .create(&store)
            .await?;

        let payload = json!({"to": "user@example.com", "template": "welcome"});

        let msg_ids = pgqrs::enqueue()
            .worker(&producer)
            .message(&payload)
            .execute(&store)
            .await?;

        println!("enqueued message id: {}", msg_ids[0]);
        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        store = await pgqrs.connect("sqlite::memory:")
        await pgqrs.admin(store).install()

        queue = "emails"
        await pgqrs.admin(store).create_queue(queue)

        producer = await store.producer(queue)

        msg_ids = await pgqrs.enqueue_batch(producer, [{"to": "user@example.com", "template": "welcome"}])
        print("enqueued message id:", msg_ids[0])

    asyncio.run(main())
    ```

!!! tip

    For bulk work, prefer batch enqueue to reduce round trips.

## Step 3: Create a Consumer and Poll

The consumer runs a poll loop that:

- dequeues up to `batch_size` messages
- calls your handler
- archives messages on success
- releases messages back to the queue on handler error

=== "Rust"

    ```rust
    use pgqrs;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("sqlite::memory:").await?;
        pgqrs::admin(&store).install().await?;

        let queue = "emails";
        pgqrs::admin(&store).create_queue(queue).await?;

        let consumer = pgqrs::consumer("worker", 9101, queue)
            .create(&store)
            .await?;

        // Poll forever until interrupted.
        // (In an application you would run this on a background task.)
        let res = pgqrs::dequeue()
            .worker(&consumer)
            .batch(10)
            .handle_batch(|msgs| {
                Box::pin(async move {
                    for msg in msgs {
                        // ... do work with msg.payload ...
                        let _ = msg;
                    }
                    Ok(())
                })
            })
            .poll(&store)
            .await;

        println!("poll stopped: {res:?}");
        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def handle_batch(msgs):
        for msg in msgs:
            # ... do work with msg.payload ...
            _ = msg.payload
        return True

    async def main():
        store = await pgqrs.connect("sqlite::memory:")
        await pgqrs.admin(store).install()

        queue = "emails"
        await pgqrs.admin(store).create_queue(queue)

        consumer = await store.consumer(queue)

        task = asyncio.create_task(
            pgqrs.dequeue()
            .worker(consumer)
            .batch(10)
            .handle_batch(handle_batch)
            .poll(store)
        )

        # In real code, interrupt on shutdown signals.
        await asyncio.sleep(1)
        await consumer.interrupt()

        try:
            await asyncio.wait_for(task, timeout=5)
        except Exception:
            pass

        assert await consumer.status() == "SUSPENDED"

    asyncio.run(main())
    ```

## Next Steps

- If you need retries/backoff, see `docs/user-guide/guides/durable-workflows.md`
- If you need visibility timeouts and scheduling, see `docs/user-guide/guides/delayed-messages.md`
- For production worker patterns, see `docs/user-guide/guides/worker-management.md`
