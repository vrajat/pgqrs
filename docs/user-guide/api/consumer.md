# Consumer API

The Consumer API allows you to process messages from queues. `pgqrs` provides both high-level (ephemeral) and low-level (managed) APIs for consuming messages.

## Quick Start

Process messages with automatic lifecycle management (dequeue -> process -> archive).

=== "Rust"

    ```rust
    // Ephemeral consumer with handler
    pgqrs::dequeue()
        .from("tasks")
        .handle(|msg| async move {
            println!("Processing: {:?}", msg.payload);
            Ok(())
        })
        .execute(&store)
        .await?;
    ```

=== "Python"

    ```python
    # High-level consume helper
    async def handler(msg):
        print(f"Processing: {msg.payload}")
        return True # Returns True to archive

    await pgqrs.consume(store, "tasks", handler)
    ```

## Managed Consumers

For long-running workers that need tracking and monitoring.

=== "Rust"

    ```rust
    // Create managed worker
    let consumer = pgqrs::consumer("worker-1", 3001, "tasks")
        .create(&store)
        .await?;

    // Use managed worker to fetch messages
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(10)
        .fetch_all(&store)
        .await?;
    ```

=== "Python"

    ```python
    # Create managed consumer
    consumer = await store.consumer("tasks")

    # Fetch messages
    messages = await pgqrs.dequeue(consumer, batch_size=10)
    ```

## Dequeue Options

### Batch Fetch

=== "Rust"

    ```rust
    let messages = pgqrs::dequeue()
        .from("tasks")
        .batch(10)
        .fetch_all(&store)
        .await?;
    ```

=== "Python"

    ```python
    messages = await pgqrs.dequeue(consumer, batch_size=10)
    ```

### Visibility Timeout

Set custom lock duration for retrieved messages.

=== "Rust"

    ```rust
    let messages = pgqrs::dequeue()
        .from("tasks")
        .vt_offset(30) // 30 seconds
        .fetch_all(&store)
        .await?;
    ```

=== "Python"

    !!! warning "Limitation"
        Python `dequeue()` currently uses the default system visibility timeout. You cannot set a custom timeout during dequeue.
        Use `extend_vt()` immediately after dequeue as a workaround.

## Message Lifecycle

### Archive (Success)

Mark message as successfully processed.

=== "Rust"

    ```rust
    consumer.archive(msg.id).await?;
    ```

=== "Python"

    ```python
    await pgqrs.archive(consumer, msg)
    # OR
    await consumer.archive(msg.id)
    ```

### Delete (Permanent Removal)

Permanently remove message (not recommended for most cases).

=== "Rust"

    ```rust
    consumer.delete(msg.id).await?;
    ```

=== "Python"

    ```python
    await pgqrs.delete(consumer, msg)
    ```

### Extend Visibility

Extend the lock on a message processing takes longer than expected.

=== "Rust"

    ```rust
    consumer.extend_visibility(msg.id, 30).await?;
    ```

=== "Python"

    ```python
    # Method is named 'extend_vt' in Python
    await consumer.extend_vt(msg.id, 30)
    # OR helper
    await pgqrs.extend_vt(consumer, msg, 30)
    ```

## Worker Lifecycle

Managed consumers implement the `Worker` trait (Rust) for lifecycle management.

=== "Rust"

    ```rust
    // Lifecycle operations
    consumer.heartbeat().await?;
    consumer.suspend().await?;
    consumer.resume().await?;
    consumer.shutdown().await?;
    ```

=== "Python"

    !!! warning "Not Supported"
        Explicit lifecycle management (`heartbeat`, `suspend`, `shutdown`) is not exposed on Python consumer objects.
        Cleanup happens automatically when the object is garbage collected or the script exits.

## Iterator Pattern

Python provides a convenient async iterator pattern not directly mirrored in the Rust high-level API.

=== "Rust"

    ```rust
    // Rust uses the .handle() callback pattern for continuous processing
    pgqrs::dequeue()
        .from("tasks")
        .handle(|msg| async move { ... })
        .execute(&store)
        .await?;
    ```

=== "Python"

    ```python
    async with store.consume_iter("tasks") as iterator:
        async for msg in iterator:
            print(msg.payload)
            await iterator.archive(msg.id)
    ```

## API Reference

### Constructor / Builder

=== "Rust"

    `pgqrs::consumer(hostname, port, queue)` -> `ConsumerBuilder`

=== "Python"

    `store.consumer(queue)` -> `Consumer`

### Methods

| Method | Rust | Python | Description |
|--------|------|--------|-------------|
| Dequeue | `.fetch_all()` | `dequeue(batch_size)` | Fetch messages |
| Archive | `.archive(id)` | `archive(msg)` | Success/Done |
| Delete | `.delete(id)` | `delete(msg)` | Remove/Discard |
| Extend | `.extend_visibility()` | `extend_vt()` | Extend lock |
