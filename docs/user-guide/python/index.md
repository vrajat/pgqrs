# Python API

This section provides comprehensive documentation for the pgqrs Python bindings.

## Overview

pgqrs provides Python bindings via [PyO3](https://pyo3.rs/), giving you access to the same high-performance Rust implementation from Python with an async-native API.

## Installation

```bash
pip install pgqrs
```

Or with your preferred package manager:

```bash
uv add pgqrs
poetry add pgqrs
```

## Core Classes

| Class | Description |
|-------|-------------|
| [`Admin`](admin.md) | Queue and schema management |
| [`Producer`](producer.md) | Message creation and enqueueing |
| [`Consumer`](consumer.md) | Message consumption and processing |
| `QueueInfo` | Queue metadata |
| `QueueMessage` | Message with payload and metadata |

### Table Classes

| Class | Description |
|-------|-------------|
| `Queues` | Queue table operations |
| `Workers` | Worker table operations |
| `Messages` | Message table operations |
| `Archive` | Archive table operations |

## Quick Reference

### Setup

```python
import asyncio
from pgqrs import Admin, Producer, Consumer

async def main():
    # Create admin for schema/queue management
    admin = Admin("postgresql://localhost/mydb")

    # Install schema (first time only)
    await admin.install()

    # Create a queue
    queue = await admin.create_queue("tasks")
    print(f"Created queue: {queue.queue_name}")

asyncio.run(main())
```

### Producing Messages

```python
async def produce_messages():
    producer = Producer(
        "postgresql://localhost/mydb",
        "tasks",      # queue name
        "localhost",  # hostname
        3000,         # port
    )

    # Single message
    msg_id = await producer.enqueue({"task": "process", "data": 123})
    print(f"Sent message: {msg_id}")
```

### Consuming Messages

```python
async def consume_messages():
    consumer = Consumer(
        "postgresql://localhost/mydb",
        "tasks",
        "localhost",
        3001,
    )

    # Dequeue and process
    messages = await consumer.dequeue()
    for message in messages:
        print(f"Processing: {message.payload}")
        await consumer.archive(message.id)
```

## Async Support

All pgqrs operations are async and work with Python's `asyncio`:

```python
import asyncio
from pgqrs import Admin

async def main():
    admin = Admin("postgresql://localhost/mydb")
    await admin.install()
    queue = await admin.create_queue("my-queue")
    print(f"Created: {queue.queue_name}")

# Run with asyncio
asyncio.run(main())
```

### With async frameworks

pgqrs works with async web frameworks:

=== "FastAPI"

    ```python
    from fastapi import FastAPI
    from pgqrs import Producer

    app = FastAPI()
    producer = Producer("postgresql://localhost/mydb", "tasks", "api", 8000)

    @app.post("/tasks")
    async def create_task(payload: dict):
        msg_id = await producer.enqueue(payload)
        return {"message_id": msg_id}
    ```

=== "aiohttp"

    ```python
    from aiohttp import web
    from pgqrs import Producer

    producer = Producer("postgresql://localhost/mydb", "tasks", "api", 8000)

    async def create_task(request):
        payload = await request.json()
        msg_id = await producer.enqueue(payload)
        return web.json_response({"message_id": msg_id})
    ```

## Error Handling

pgqrs raises Python exceptions:

```python
from pgqrs import Admin

async def safe_create_queue():
    admin = Admin("postgresql://localhost/mydb")

    try:
        queue = await admin.create_queue("tasks")
        print(f"Created: {queue.queue_name}")
    except RuntimeError as e:
        if "already exists" in str(e):
            print("Queue already exists")
        else:
            raise
```

## Type Hints

pgqrs includes type stubs for IDE support:

```python
from pgqrs import Producer, QueueMessage

async def process(consumer) -> None:
    messages: list[QueueMessage] = await consumer.dequeue()
    for msg in messages:
        id: int = msg.id
        payload: dict = msg.payload
        await consumer.archive(id)
```

## API Reference

<div class="grid cards" markdown>

-   :material-arrow-up-bold:{ .lg .middle } **Producer**

    ---

    Creating and sending messages.

    [:octicons-arrow-right-24: Producer API](producer.md)

-   :material-arrow-down-bold:{ .lg .middle } **Consumer**

    ---

    Fetching and processing messages.

    [:octicons-arrow-right-24: Consumer API](consumer.md)

-   :material-shield-account:{ .lg .middle } **Admin**

    ---

    Queue and schema management.

    [:octicons-arrow-right-24: Admin API](admin.md)

</div>

## Complete Example

```python
import asyncio
from pgqrs import Admin, Producer, Consumer

async def main():
    # Setup
    admin = Admin("postgresql://localhost/mydb")
    await admin.install()
    queue = await admin.create_queue("demo")

    # Produce
    producer = Producer("postgresql://localhost/mydb", "demo", "localhost", 3000)
    for i in range(5):
        msg_id = await producer.enqueue({"task": i})
        print(f"Sent task {i}: {msg_id}")

    # Consume
    consumer = Consumer("postgresql://localhost/mydb", "demo", "localhost", 3001)
    messages = await consumer.dequeue()

    for msg in messages:
        print(f"Processing: {msg.payload}")
        await consumer.archive(msg.id)

    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())
```

## See Also

- [Rust API](../rust/index.md) - Native Rust documentation
- [Basic Workflow Guide](../guides/basic-workflow.md) - Step-by-step tutorial
