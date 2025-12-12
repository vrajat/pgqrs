# Python Configuration

This page covers configuration options for pgqrs Python bindings.

## Basic Configuration

The Python bindings use a connection string (DSN) for configuration:

```python
from pgqrs import Admin, Producer, Consumer

# Basic DSN
dsn = "postgresql://user:password@localhost:5432/database"

# Create instances
admin = Admin(dsn)
producer = Producer(dsn, "queue_name", "hostname", 3000)
consumer = Consumer(dsn, "queue_name", "hostname", 3001)
```

## Connection String Format

The DSN follows the standard PostgreSQL connection string format:

```
postgresql://[user[:password]@][host][:port][/database][?param=value&...]
```

### Examples

```python
# Local development
dsn = "postgresql://localhost/mydb"

# With credentials
dsn = "postgresql://user:password@localhost:5432/mydb"

# Remote server with SSL
dsn = "postgresql://user:password@db.example.com:5432/mydb?sslmode=require"

# Unix socket
dsn = "postgresql:///mydb?host=/var/run/postgresql"
```

## Environment Variables

Use environment variables for secure configuration:

```python
import os
from pgqrs import Admin

dsn = os.environ.get("PGQRS_DSN", "postgresql://localhost/mydb")
admin = Admin(dsn)
```

Common environment variables:

| Variable | Description |
|----------|-------------|
| `PGQRS_DSN` | Full connection string |
| `PGHOST` | PostgreSQL host |
| `PGPORT` | PostgreSQL port |
| `PGDATABASE` | Database name |
| `PGUSER` | Username |
| `PGPASSWORD` | Password |

## Configuration Patterns

### Using python-dotenv

```python
from dotenv import load_dotenv
import os
from pgqrs import Admin

load_dotenv()

dsn = os.environ["PGQRS_DSN"]
admin = Admin(dsn)
```

### Using Pydantic Settings

```python
from pydantic_settings import BaseSettings
from pgqrs import Admin, Producer, Consumer

class Settings(BaseSettings):
    database_url: str
    queue_name: str = "default"
    worker_hostname: str = "localhost"
    worker_port: int = 3000

    class Config:
        env_prefix = "PGQRS_"

settings = Settings()

admin = Admin(settings.database_url)
producer = Producer(
    settings.database_url,
    settings.queue_name,
    settings.worker_hostname,
    settings.worker_port,
)
```

### FastAPI Integration

```python
from fastapi import FastAPI, Depends
from functools import lru_cache
from pgqrs import Producer

class Config:
    dsn: str = "postgresql://localhost/mydb"
    queue_name: str = "tasks"

@lru_cache
def get_config():
    return Config()

def get_producer(config: Config = Depends(get_config)):
    return Producer(
        config.dsn,
        config.queue_name,
        "api-server",
        8000,
    )

app = FastAPI()

@app.post("/tasks")
async def create_task(
    task: dict,
    producer: Producer = Depends(get_producer)
):
    msg_id = await producer.enqueue(task)
    return {"id": msg_id}
```

## Current Limitations

!!! warning "Limited Configuration Options"
    The Python bindings currently have limited configuration compared to Rust:

    - **No custom schema support** - Always uses `public` schema
    - **No connection pool settings** - Uses defaults
    - **No timeout configuration** - Uses defaults
    - **No rate limiting configuration** - Not exposed

### Missing Features

The following Rust configuration options are not yet available in Python:

| Feature | Rust | Python |
|---------|------|--------|
| Custom schema | ✅ `Config::from_dsn_with_schema()` | ❌ Not available |
| Max connections | ✅ `config.max_connections` | ❌ Not available |
| Connection timeout | ✅ `config.connection_timeout_seconds` | ❌ Not available |
| Default lock time | ✅ `config.default_lock_time_seconds` | ❌ Not available |
| Batch size | ✅ `config.default_max_batch_size` | ❌ Not available |

See [GitHub Issues](https://github.com/vrajat/pgqrs/issues) for feature requests.

## Worker Identification

When creating Producer or Consumer instances, provide meaningful identification:

```python
import socket
import os

# Get meaningful hostname
hostname = socket.gethostname()
# Or from environment
hostname = os.environ.get("HOSTNAME", "unknown")

# Use unique ports for different workers
worker_id = int(os.environ.get("WORKER_ID", "0"))
port = 3000 + worker_id

producer = Producer(dsn, "tasks", hostname, port)
```

## SSL/TLS Configuration

For secure connections, include SSL parameters in the DSN:

```python
# Require SSL
dsn = "postgresql://user:pass@host/db?sslmode=require"

# Verify server certificate
dsn = "postgresql://user:pass@host/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt"

# Client certificate authentication
dsn = "postgresql://user@host/db?sslmode=verify-full&sslcert=/path/to/client.crt&sslkey=/path/to/client.key"
```

SSL modes:

| Mode | Description |
|------|-------------|
| `disable` | No SSL |
| `allow` | Try non-SSL first |
| `prefer` | Try SSL first (default) |
| `require` | Require SSL |
| `verify-ca` | Verify server cert |
| `verify-full` | Verify server cert and hostname |

## Best Practices

1. **Use environment variables** for credentials
2. **Use connection pooling** at the application level if needed
3. **Provide meaningful worker identifiers** for debugging
4. **Use SSL in production** for secure connections

## What's Next?

- [Producer](producer.md) - Sending messages
- [Consumer](consumer.md) - Processing messages
- [Rust Configuration](../rust/configuration.md) - Full configuration options
