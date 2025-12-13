# Producer (Python)

The `Producer` class is responsible for creating and enqueueing messages to a queue.

## Creating a Producer

```python
from pgqrs import Producer

producer = Producer(
    dsn="postgresql://localhost/mydb",
    queue="tasks",
    hostname="my-service",
    port=3000,
)
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dsn` | `str` | PostgreSQL connection string |
| `queue` | `str` | Name of the queue to produce to |
| `hostname` | `str` | Hostname for worker identification |
| `port` | `int` | Port for worker identification |

!!! note
    The queue must already exist. Create it first using `Admin.create_queue()`.

## Methods

### enqueue

Enqueue a single message to the queue.

```python
payload = {
    "action": "send_email",
    "to": "user@example.com",
    "subject": "Welcome!"
}

message_id = await producer.enqueue(payload)
print(f"Message ID: {message_id}")
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `payload` | `dict` | JSON-serializable message data |

**Returns:** `int` - The created message's ID.

### Usage Examples

#### Basic Enqueue

```python
import asyncio
from pgqrs import Admin, Producer

async def main():
    # Setup
    admin = Admin("postgresql://localhost/mydb")
    await admin.create_queue("notifications")

    # Create producer
    producer = Producer(
        "postgresql://localhost/mydb",
        "notifications",
        "notification-service",
        3000,
    )

    # Send message
    msg_id = await producer.enqueue({
        "type": "email",
        "to": "user@example.com",
        "subject": "Hello!"
    })

    print(f"Sent notification: {msg_id}")

asyncio.run(main())
```

#### Sending Multiple Messages

```python
async def send_batch_emails(producer, emails):
    """Send multiple messages one at a time."""
    message_ids = []

    for email in emails:
        msg_id = await producer.enqueue({
            "type": "email",
            "to": email["to"],
            "subject": email["subject"],
            "body": email["body"]
        })
        message_ids.append(msg_id)

    return message_ids

# Usage
emails = [
    {"to": "user1@example.com", "subject": "Hello", "body": "..."},
    {"to": "user2@example.com", "subject": "Hello", "body": "..."},
]
ids = await send_batch_emails(producer, emails)
print(f"Sent {len(ids)} emails")
```

## Patterns

### FastAPI Integration

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pgqrs import Producer

app = FastAPI()

# Initialize producer at startup
producer: Producer | None = None

@app.on_event("startup")
async def startup():
    global producer
    producer = Producer(
        "postgresql://localhost/mydb",
        "tasks",
        "api-server",
        8000,
    )

class TaskRequest(BaseModel):
    task_type: str
    data: dict

@app.post("/tasks")
async def create_task(request: TaskRequest):
    if not producer:
        raise HTTPException(status_code=503, detail="Producer not ready")

    msg_id = await producer.enqueue({
        "task_type": request.task_type,
        "data": request.data
    })

    return {"message_id": msg_id, "status": "queued"}
```

### Error Handling

```python
async def safe_enqueue(producer, payload, max_retries=3):
    """Enqueue with retry logic."""
    for attempt in range(max_retries):
        try:
            return await producer.enqueue(payload)
        except RuntimeError as e:
            if attempt == max_retries - 1:
                raise
            print(f"Attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
```

### Producer Service Class

```python
class NotificationService:
    def __init__(self, dsn: str):
        self.producer = Producer(dsn, "notifications", "notification-svc", 3000)

    async def send_welcome_email(self, user_id: int, email: str):
        return await self.producer.enqueue({
            "type": "welcome_email",
            "user_id": user_id,
            "email": email
        })

    async def send_password_reset(self, user_id: int, token: str):
        return await self.producer.enqueue({
            "type": "password_reset",
            "user_id": user_id,
            "token": token
        })

    async def send_notification(self, user_id: int, message: str):
        return await self.producer.enqueue({
            "type": "push_notification",
            "user_id": user_id,
            "message": message
        })

# Usage
async def main():
    notifications = NotificationService("postgresql://localhost/mydb")

    # Send various notifications
    await notifications.send_welcome_email(123, "user@example.com")
    await notifications.send_notification(123, "Your order shipped!")
```

## Full Example

```python
import asyncio
from pgqrs import Admin, Producer

async def main():
    dsn = "postgresql://localhost/mydb"

    # Setup
    admin = Admin(dsn)
    await admin.install()
    await admin.create_queue("tasks")

    # Create producer
    producer = Producer(dsn, "tasks", "producer-1", 3000)

    # Send immediate messages
    for i in range(5):
        msg_id = await producer.enqueue({
            "task_id": i,
            "action": "process",
            "data": {"value": i * 10}
        })
        print(f"Sent task {i}: message ID {msg_id}")

    print("\nAll messages sent!")

if __name__ == "__main__":
    asyncio.run(main())
```

## See Also

- [Consumer (Python)](consumer.md) - Processing messages
- [Admin (Python)](admin.md) - Queue management
- [Producer (Rust)](../rust/producer.md) - Rust API reference
