# Consumer (Python)

The `Consumer` class is responsible for fetching and processing messages from a queue.

## Creating a Consumer

```python
from pgqrs import Consumer

consumer = Consumer(
    dsn="postgresql://localhost/mydb",
    queue="tasks",
    hostname="worker-1",
    port=3001,
)
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dsn` | `str` | PostgreSQL connection string |
| `queue` | `str` | Name of the queue to consume from |
| `hostname` | `str` | Hostname for worker identification |
| `port` | `int` | Port for worker identification |

## Methods

### dequeue

Fetch available messages from the queue.

```python
messages = await consumer.dequeue()

for message in messages:
    print(f"ID: {message.id}")
    print(f"Payload: {message.payload}")
```

**Returns:** `list[QueueMessage]` - Available messages (empty list if none).

Messages are automatically locked with a visibility timeout. If not archived/deleted before the timeout, they become available again.

### archive

Archive a processed message for audit trail retention.

```python
await consumer.archive(message_id)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `message_id` | `int` | ID of the message to archive |

Archiving moves the message to the `pgqrs_archive` table with a timestamp.

## QueueMessage

The `QueueMessage` object returned by `dequeue()`:

| Property | Type | Description |
|----------|------|-------------|
| `id` | `int` | Unique message ID |
| `queue_id` | `int` | Queue this message belongs to |
| `payload` | `dict` | Message data (automatically parsed from JSON) |

## Usage Examples

### Basic Consumer Loop

```python
import asyncio
from pgqrs import Consumer

async def consume_loop():
    consumer = Consumer(
        "postgresql://localhost/mydb",
        "tasks",
        "worker-1",
        3001,
    )

    while True:
        messages = await consumer.dequeue()

        if not messages:
            print("No messages, waiting...")
            await asyncio.sleep(1)
            continue

        for message in messages:
            print(f"Processing message {message.id}")
            print(f"Payload: {message.payload}")

            # Process the message...

            # Archive after successful processing
            await consumer.archive(message.id)
            print(f"Archived message {message.id}")

asyncio.run(consume_loop())
```

### Consumer with Error Handling

```python
async def robust_consumer(consumer):
    """Consumer with error handling and retries."""
    while True:
        try:
            messages = await consumer.dequeue()

            if not messages:
                await asyncio.sleep(1)
                continue

            for message in messages:
                try:
                    await process_message(message)
                    await consumer.archive(message.id)
                except Exception as e:
                    print(f"Failed to process {message.id}: {e}")
                    # Message will become available again after timeout

        except RuntimeError as e:
            print(f"Dequeue error: {e}")
            await asyncio.sleep(5)

async def process_message(message):
    """Your message processing logic."""
    payload = message.payload
    # ... process payload ...
```

### Consumer with Graceful Shutdown

```python
import asyncio
import signal
from pgqrs import Consumer

shutdown = False

def handle_shutdown(signum, frame):
    global shutdown
    print("\nShutdown requested...")
    shutdown = True

async def consumer_with_shutdown():
    global shutdown

    # Setup signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    consumer = Consumer(
        "postgresql://localhost/mydb",
        "tasks",
        "worker-1",
        3001,
    )

    print("Consumer started. Press Ctrl+C to stop.")

    while not shutdown:
        messages = await consumer.dequeue()

        for message in messages:
            if shutdown:
                print("Shutdown: not processing more messages")
                break

            print(f"Processing: {message.payload}")
            await consumer.archive(message.id)

        if not messages and not shutdown:
            await asyncio.sleep(1)

    print("Consumer stopped gracefully")

asyncio.run(consumer_with_shutdown())
```

## Patterns

### Worker Service Class

```python
class TaskWorker:
    def __init__(self, dsn: str, queue: str, worker_id: str):
        self.consumer = Consumer(dsn, queue, f"worker-{worker_id}", 3001)
        self.running = False

    async def start(self):
        self.running = True
        print(f"Worker started")

        while self.running:
            messages = await self.consumer.dequeue()

            for message in messages:
                await self.handle_message(message)

            if not messages:
                await asyncio.sleep(1)

    async def handle_message(self, message):
        """Override in subclass for custom handling."""
        payload = message.payload
        task_type = payload.get("type")

        if task_type == "email":
            await self.send_email(payload)
        elif task_type == "notification":
            await self.send_notification(payload)
        else:
            print(f"Unknown task type: {task_type}")

        await self.consumer.archive(message.id)

    async def send_email(self, payload):
        print(f"Sending email to {payload.get('to')}")

    async def send_notification(self, payload):
        print(f"Sending notification: {payload.get('message')}")

    def stop(self):
        self.running = False

# Usage
async def main():
    worker = TaskWorker(
        "postgresql://localhost/mydb",
        "tasks",
        "001"
    )
    await worker.start()

asyncio.run(main())
```

### Multiple Consumers

```python
async def run_consumers(dsn: str, queue: str, num_workers: int):
    """Run multiple consumers concurrently."""

    async def consumer_task(worker_id: int):
        consumer = Consumer(dsn, queue, f"worker-{worker_id}", 3000 + worker_id)

        while True:
            messages = await consumer.dequeue()
            for message in messages:
                print(f"Worker {worker_id} processing: {message.id}")
                await asyncio.sleep(0.1)  # Simulate work
                await consumer.archive(message.id)

            if not messages:
                await asyncio.sleep(1)

    # Start all consumers
    tasks = [
        asyncio.create_task(consumer_task(i))
        for i in range(num_workers)
    ]

    await asyncio.gather(*tasks)

# Run 4 consumers
asyncio.run(run_consumers("postgresql://localhost/mydb", "tasks", 4))
```

### Dead Letter Queue Pattern

```python
async def consumer_with_dlq(consumer, dlq_producer, max_retries=3):
    """Process messages with dead letter queue for failures."""

    while True:
        messages = await consumer.dequeue()

        for message in messages:
            read_count = message.payload.get("_read_count", 1)

            try:
                await process_message(message)
                await consumer.archive(message.id)

            except Exception as e:
                if read_count >= max_retries:
                    # Move to dead letter queue
                    await dlq_producer.enqueue({
                        "original_payload": message.payload,
                        "original_id": message.id,
                        "error": str(e),
                        "read_count": read_count
                    })
                    await consumer.archive(message.id)  # Remove from main queue
                    print(f"Moved {message.id} to DLQ after {read_count} attempts")
                else:
                    print(f"Will retry {message.id} (attempt {read_count})")
                    # Let lock expire for retry

        if not messages:
            await asyncio.sleep(1)
```

## Full Example

```python
import asyncio
from pgqrs import Admin, Producer, Consumer

async def main():
    dsn = "postgresql://localhost/mydb"

    # Setup
    admin = Admin(dsn)
    await admin.install()
    await admin.create_queue("demo")

    # Send some messages
    producer = Producer(dsn, "demo", "producer", 3000)
    for i in range(5):
        msg_id = await producer.enqueue({"task": f"task-{i}", "value": i})
        print(f"Sent: {msg_id}")

    print("\n--- Processing ---\n")

    # Consume messages
    consumer = Consumer(dsn, "demo", "consumer", 3001)

    processed = 0
    while processed < 5:
        messages = await consumer.dequeue()

        for message in messages:
            print(f"Processing message {message.id}:")
            print(f"  Payload: {message.payload}")

            # Archive after processing
            await consumer.archive(message.id)
            print(f"  Archived!")
            processed += 1

        if not messages:
            await asyncio.sleep(0.5)

    print(f"\nProcessed {processed} messages")

if __name__ == "__main__":
    asyncio.run(main())
```

## See Also

- [Producer (Python)](producer.md) - Creating messages
- [Admin (Python)](admin.md) - Queue management
- [Consumer (Rust)](../rust/consumer.md) - Rust API reference
- [Worker Management Guide](../guides/worker-management.md) - Scaling consumers
