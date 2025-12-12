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

    !!! warning "Not Yet Implemented"
        `enqueue_delayed` is not yet available in Python bindings.
        See [GitHub Issue](https://github.com/vrajat/pgqrs/issues) for status.

    As a workaround, you can include a scheduled time in your payload and filter in the consumer:

    ```python
    from datetime import datetime, timedelta

    # Include scheduled time in payload
    scheduled_at = datetime.utcnow() + timedelta(minutes=5)
    await producer.enqueue({
        "task": "send_email",
        "scheduled_at": scheduled_at.isoformat()
    })
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
    // Extend lock by 30 more seconds
    producer.extend_visibility(message.id, 30).await?;
    ```

=== "Python"

    !!! warning "Not Yet Implemented"
        `extend_visibility` is not yet available in Python bindings.

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

## Read Count

The `read_ct` field tracks how many times a message has been dequeued:

| read_ct | Meaning |
|---------|---------|
| 1 | First attempt |
| 2 | Second attempt (first retry) |
| 3+ | Multiple retries |

Use this for dead-letter queue logic:

=== "Rust"

    ```rust
    let messages = consumer.dequeue().await?;
    for message in messages {
        if message.read_ct > 3 {
            // Move to dead-letter queue
            move_to_dlq(&message).await?;
            consumer.delete(message.id).await?;
        } else {
            // Normal processing
            process(&message).await?;
            consumer.archive(message.id).await?;
        }
    }
    ```

=== "Python"

    ```python
    messages = await consumer.dequeue()
    for message in messages:
        # Note: read_ct field access depends on QueueMessage implementation
        # Currently archive is available, delete is not yet exposed
        try:
            await process(message)
            await consumer.archive(message.id)
        except Exception as e:
            print(f"Failed to process message {message.id}: {e}")
            # Message will become available again after lock expires
    ```

    !!! note
        The `delete` method and `read_ct` field access may not be fully exposed in Python bindings yet.

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

    !!! warning "Not Yet Implemented"
        `delete` method is not yet available in Python bindings.
        Use `archive` for all completed messages.

## Archive Management

### Query Archived Messages

```bash
pgqrs archive list tasks
pgqrs archive count tasks
```

### Purge Old Archives

```bash
# Delete archived messages older than 30 days
pgqrs archive delete tasks --older-than 30d
```

### Programmatic Archive Access

=== "Rust"

    ```rust
    use pgqrs::Archive;

    let archive = Archive::new(pool.clone());

    // Count archived messages for a queue
    let count = archive.count_for_fk(queue_id, &mut tx).await?;

    // List archived messages
    let messages = archive.filter_by_fk(queue_id, &mut tx).await?;
    ```

=== "Python"

    ```python
    admin = Admin("postgresql://localhost/mydb")
    archive = await admin.get_archive()

    # Count archived messages
    count = await archive.count()
    print(f"Total archived: {count}")
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
            producer.extend_visibility(message.id, 300).await?;
        }
        // Process...
    }
    ```

=== "Python"

    ```python
    # Currently Python uses default lock times
    messages = await consumer.dequeue()

    # Process quickly to avoid lock expiration
    for message in messages:
        await process(message)
        await consumer.archive(message.id)
    ```

    !!! note
        `dequeue_many_with_delay` and `extend_visibility` are not yet available in Python.

### 2. Handle Retries Gracefully

=== "Rust"

    ```rust
    for message in messages {
        match process(&message).await {
            Ok(_) => consumer.archive(message.id).await?,
            Err(e) if message.read_ct < 3 => {
                // Let it retry (don't archive/delete)
                tracing::warn!("Will retry message {}: {}", message.id, e);
            }
            Err(e) => {
                // Too many retries, move to DLQ
                move_to_dlq(&message, e).await?;
                consumer.delete(message.id).await?;
            }
        }
    }
    ```

=== "Python"

    ```python
    for message in messages:
        try:
            await process(message)
            await consumer.archive(message.id)
        except Exception as e:
            print(f"Error processing {message.id}: {e}")
            # Message will retry after lock expires
            # Consider logging for manual intervention
    ```

### 3. Archive for Observability

Even if you don't need long-term retention, archives help debugging:

=== "Rust"

    ```rust
    // Always archive in development
    consumer.archive(message.id).await?;

    // In production, archive or delete based on message type
    if message.payload.get("debug").is_some() {
        consumer.archive(message.id).await?;
    } else {
        consumer.delete(message.id).await?;
    }
    ```

=== "Python"

    ```python
    # Always archive (delete not yet available in Python)
    await consumer.archive(message.id)
    ```

## What's Next?

- [Basic Workflow Guide](../guides/basic-workflow.md) - Complete working examples
- [Batch Processing](../guides/batch-processing.md) - High-throughput patterns
