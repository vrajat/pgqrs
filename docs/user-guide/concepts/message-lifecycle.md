# Message Lifecycle

This page explains the complete lifecycle of a message in pgqrs, from creation to archival.

## Overview

Messages in pgqrs go through several states as they move through the system:

```mermaid
stateDiagram-v2
    [*] --> Pending: enqueue()
    Pending --> Locked: dequeue()
    Locked --> Pending: timeout expires
    Locked --> Archived: archive()
    Locked --> [*]: delete()
    Archived --> [*]: purge
```

## Message States

### Pending

A message is **pending** when it's in the queue and available for processing.

**Characteristics:**
- Visible to consumers
- `vt` (visibility timeout) is in the past or NULL
- Can be dequeued

**How to get here:**
- `producer.enqueue()` - immediate availability
- `producer.enqueue_delayed()` - after delay expires
- Lock timeout expires on a locked message

### Locked

A message is **locked** when a consumer has dequeued it and is processing it.

**Characteristics:**
- Not visible to other consumers
- `vt` (visibility timeout) is in the future
- `read_ct` incremented
- Associated with a specific worker

**How to get here:**
- `consumer.dequeue()`

**Transitions:**
- → **Archived**: `consumer.archive()`
- → **Deleted**: `consumer.delete()`
- → **Pending**: Visibility timeout expires

### Archived

A message is **archived** when processing completed successfully and the message was moved to the archive table.

**Characteristics:**
- Stored in `pgqrs_archive` table
- Includes original payload and metadata
- Records `archived_at` timestamp
- Useful for audit trails

**How to get here:**
- `consumer.archive()`

### Deleted

A message is **deleted** when it's removed from the system entirely.

**Characteristics:**
- No longer exists in any table
- Cannot be recovered
- No audit trail

**How to get here:**
- `consumer.delete()`

## Message Flow

### Normal Processing Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant Q as Messages Table
    participant C as Consumer
    participant A as Archive Table

    P->>Q: enqueue(payload)
    Note over Q: Message created<br/>status: pending

    C->>Q: dequeue()
    Note over Q: Message locked<br/>vt = now + lock_time

    C->>C: Process message

    C->>A: archive(id)
    Note over A: Message archived<br/>with timestamp

    C->>Q: DELETE
    Note over Q: Message removed
```

### Timeout and Retry Flow

```mermaid
sequenceDiagram
    participant Q as Messages Table
    participant C1 as Consumer 1
    participant C2 as Consumer 2

    C1->>Q: dequeue()
    Note over Q: Message locked<br/>vt = now + 5s

    C1->>C1: Processing fails<br/>or crashes

    Note over Q: Time passes...<br/>vt expires

    Note over Q: Message visible<br/>again (pending)

    C2->>Q: dequeue()
    Note over Q: Message locked<br/>by Consumer 2

    C2->>C2: Process message
    C2->>Q: archive(id)
```

## Message Fields

### In Messages Table

| Field | Type | Description |
|-------|------|-------------|
| `id` | `BIGINT` | Unique identifier |
| `queue_id` | `BIGINT` | Reference to queue |
| `payload` | `JSONB` | Your message data |
| `enqueued_at` | `TIMESTAMPTZ` | When message was created |
| `vt` | `TIMESTAMPTZ` | Visibility timeout (lock expiry) |
| `read_ct` | `INT` | Number of dequeue attempts |

### In Archive Table

| Field | Type | Description |
|-------|------|-------------|
| `id` | `BIGINT` | Original message ID |
| `queue_id` | `BIGINT` | Reference to queue |
| `payload` | `JSONB` | Original message data |
| `enqueued_at` | `TIMESTAMPTZ` | Original creation time |
| `archived_at` | `TIMESTAMPTZ` | When archived |
| `read_ct` | `INT` | Final read count |

## Delayed Messages

Delayed messages have a `vt` set in the future at creation time:

=== "Rust"

    ```rust
    // Message won't be visible for 5 minutes
    let message = producer.enqueue_delayed(&payload, 300).await?;
    ```

=== "Python"

    ```python
    import pgqrs
    import asyncio

    admin = pgqrs.admin("postgresql://localhost/mydb")
    producer = pgqrs.producer(admin, "tasks", "scheduler", 8080)

    # Send delayed message (available in 300 seconds = 5 minutes)
    message_id = await producer.enqueue_delayed(
        {"task": "send_reminder", "user_id": 123},
        delay_seconds=300
    )

    print(f"Scheduled message {message_id} for future processing")
    ```

```mermaid
sequenceDiagram
    participant P as Producer
    participant Q as Messages Table
    participant C as Consumer

    P->>Q: enqueue_delayed(payload, 300)
    Note over Q: Message created<br/>vt = now + 300s

    C->>Q: dequeue()
    Note over Q: Message not returned<br/>(vt in future)

    Note over Q: 5 minutes pass...

    C->>Q: dequeue()
    Note over Q: Message returned<br/>(vt expired)
```

## Visibility Extension

Extend the lock on a message if processing takes longer:

=== "Rust"

    ```rust
        // Processing taking longer than expected...
    consumer.extend_visibility(message.id, 30).await?;
    ```

=== "Python"

    ```python
    import pgqrs
    import asyncio

    admin = pgqrs.Admin("postgresql://localhost/mydb")
    consumer = pgqrs.Consumer(admin, "tasks", "worker1", 8080)

    # Dequeue and extend processing time
    messages = await consumer.dequeue()
    if messages:
        msg = messages[0]

        # Extend visibility by 5 minutes (300 seconds)
        await consumer.extend_vt(msg.id, 300)

        # Continue processing with extended timeout
        result = await long_running_task(msg.payload)
        await consumer.archive(msg.id)
    ```

```mermaid
sequenceDiagram
    participant C as Consumer
    participant Q as Messages Table

    C->>Q: dequeue()
    Note over Q: vt = now + 5s

    C->>C: Processing...

    C->>Q: extend_visibility(id, 30)
    Note over Q: vt = now + 30s

    C->>C: Continue processing...

    C->>Q: archive(id)
```


## Read Count & Dead Letter Queue (DLQ)

The `read_ct` field tracks how many times a message has been dequeued:

| read_ct | Meaning |
|---------|---------|
| 1 | First attempt |
| 2 | Second attempt (first retry) |
| > 3 | **Poison Message** (Hidden from dequeue) |

By default, messages with `read_ct > 3` are widely considered "poison messages" and will **NOT** be returned by `dequeue()`. This prevents a single bad message from crashing consumers repeatedly forever.

To handle these messages, you must use administrative tools to move them to the archive (Dead Letter Queue processing).

### Handling Poison Messages

=== "Rust"

    ```rust
    // 1. Identify high read_ct messages (they are stuck in the queue)
    let messages = admin.messages().list(queue_id).await?;
    for msg in messages {
        if msg.read_ct > 3 {
             println!("Poison message found: {}", msg.id);
        }
    }

    // 2. Move all poison messages (read_ct > 3) to archive
    // This cleans up the queue and preserves the messages for inspection
    let moved_ids = admin.dlq().await?;
    println!("Moved {} messages to DLQ archive", moved_ids.len());
    ```

=== "Python"

    !!! warning "Not Supported"
        Python bindings currently do not support the administrative APIs required to inspect `read_ct` of pending messages or trigger the `dlq()` cleanup command.

        **Workaround:** Use the CLI to manage poison messages:
        ```bash
        # Move all poison messages to archive
        pgqrs queue archive-dlq
        ```

## Archive vs Delete

Choose based on your requirements:

| Operation | Use When |
|-----------|----------|
| **Archive** | Need audit trail, compliance requirements, debugging |
| **Delete** | No retention needed, minimize storage, ephemeral data |

### Archive

=== "Rust"

    ```rust
    consumer.archive(message.id).await?;
    // Message moved to pgqrs_archive, audit trail preserved
    ```

=== "Python"

    ```python
    await consumer.archive(message.id)
    # Message moved to pgqrs_archive, audit trail preserved
    ```

### Delete

=== "Rust"

    ```rust
    consumer.delete(message.id).await?;
    // Message gone forever, no audit trail
    ```

=== "Python"

    ```python
    await consumer.archive(message.id)
    ```

## Best Practices

### 1. Set Appropriate Lock Times

=== "Rust"

    ```rust
    // Short tasks: 5-10 seconds
    let messages = consumer.dequeue_many_with_delay(100, 10).await?;

    // Long tasks: extend as needed
    let messages = consumer.dequeue_many_with_delay(10, 60).await?;
    for message in messages {
        if might_take_long(&message) {
            consumer.extend_visibility(message.id, 300).await?;
        }
        // Process...
    }
    ```

=== "Python"

    !!! info
        Python bindings currently use the system default visibility timeout.
        Custom dequeue lock times are not supported during dequeue.

    ```python
    messages = await consumer.dequeue()

    for message in messages:
        # Workaround: Extend visibility immediately if task is known to be long
        # Note: Method is named 'extend_vt' in Python
        await consumer.extend_vt(message.id, 300)

        await process(message)
        await consumer.archive(message.id)
    ```

### 2. Archive for Observability

Even if you don't need long-term retention, archives help debugging:

=== "Rust"

    ```rust
    // Always archive in development
    consumer.archive(message.id).await?;
    ```

=== "Python"

    ```python
    # Archive the message to remove it from the queue
    await consumer.archive(message.id)
    ```

## What's Next?

- [Basic Workflow Guide](../guides/basic-workflow.md) - Complete working examples
- [Batch Processing](../guides/batch-processing.md) - High-throughput patterns
