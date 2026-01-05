# Producer API

The Producer API allows you to send messages to queues. `pgqrs` provides both high-level (ephemeral) and low-level (managed) APIs for producing messages.

## Quick Start (Ephemeral Producer)

Send a single message without creating a managed worker.

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    let store = pgqrs::connect("postgresql://localhost/mydb").await?;

    // Builder API for simple one-off messages
    let ids = pgqrs::enqueue()
        .message(&json!({"task": "send_email", "to": "user@example.com"}))
        .to("tasks")
        .execute(&store)
        .await?;

    println!("Enqueued message ID: {}", ids[0]);
    ```

=== "Python"

    ```python
    # Python bindings currently require a managed producer object
    import pgqrs

    store = await pgqrs.connect("postgresql://localhost/mydb")

    # Create a simple producer instance
    producer = await store.producer("tasks")

    # Enqueue message
    msg_id = await producer.enqueue({
        "task": "send_email",
        "to": "user@example.com"
    })
    print(f"Enqueued message ID: {msg_id}")
    ```

## Managed Producers

For long-running services, creating a managed producer allows for worker tracking and monitoring.

=== "Rust"

    ```rust
    // Create a managed producer worker
    let producer = pgqrs::producer("my-service", 3000, "tasks")
        .create(&store)
        .await?;

    // Enqueue using the managed worker
    let ids = pgqrs::enqueue()
        .message(&json!({"task": "process_order"}))
        .worker(&*producer)
        .execute(&store)
        .await?;
    ```

=== "Python"

    ```python
    # Create managed producer with worker tracking details
    producer = await store.producer(
        queue="tasks",
        hostname="my-service",
        port=3000
    )

    msg_id = await producer.enqueue({"task": "process_order"})
    ```

## Enqueue Options

### Batch Messages

Send multiple messages in a single transaction.

=== "Rust"

    ```rust
    let messages = vec![
        json!({"user_id": 1}),
        json!({"user_id": 2}),
    ];

    let ids = pgqrs::enqueue()
        .messages(&messages)
        .to("emails")
        .execute(&store)
        .await?;
    ```

=== "Python"

    ```python
    # Batch enqueue requires a loop or helper (if available)
    messages = [
        {"user_id": 1},
        {"user_id": 2}
    ]

    # Currently executed sequentially in Python unless batch helper exists
    for msg in messages:
        await producer.enqueue(msg)
    ```

### Delayed Messages

Schedule a message for future processing.

=== "Rust"

    ```rust
    let ids = pgqrs::enqueue()
        .message(&json!({"reminder": "Follow up"}))
        .to("tasks")
        .delay(300)  // 5 minutes
        .execute(&store)
        .await?;
    ```

=== "Python"

    ```python
    # Use enqueue_delayed specific method
    await producer.enqueue_delayed(
        {"reminder": "Follow up"},
        delay_seconds=300
    )
    ```

## Worker Lifecycle

Managed producers implement the `Worker` trait (Rust) for lifecycle management.

=== "Rust"

    ```rust
    // Lifecycle operations
    producer.heartbeat().await?;
    producer.suspend().await?;
    producer.resume().await?;
    producer.shutdown().await?;
    ```

=== "Python"

    !!! warning "Not Supported"
        Explicit lifecycle management (`heartbeat`, `suspend`, `shutdown`) is not exposed on Python producer objects. They are managed automatically.

## API Reference

### Constructor / Builder

=== "Rust"

    `pgqrs::producer(hostname, port, queue)` -> `ProducerBuilder`

=== "Python"

    `store.producer(queue, hostname="localhost", port=0)` -> `Producer`

### Methods

| Method | Rust | Python | Description |
|--------|------|--------|-------------|
| Enqueue | `.enqueue()...execute()` | `enqueue(payload)` | Send single message |
| Batch | `.messages(&vec)...` | Loop or batch helper | Send multiple messages |
| Delay | `.delay(seconds)...` | `enqueue_delayed(payload, seconds)` | Schedule message |
| Cleanup | `shutdown()` | Automatic | Stop worker |
