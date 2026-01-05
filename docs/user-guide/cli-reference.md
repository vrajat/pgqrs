# CLI Reference

Complete reference for the `pgqrs` command-line interface.

## Installation

The CLI is included when you install the pgqrs Rust crate:

```bash
cargo install pgqrs
```

## Global Options

All commands accept these global options:

| Option | Short | Description |
|--------|-------|-------------|
| `--dsn <DSN>` | `-d` | Database URL (overrides all config) |
| `--schema <SCHEMA>` | `-s` | Schema name (default: public) |
| `--config <FILE>` | `-c` | Config file path |
| `--log-dest <DEST>` | | stderr \| file |
| `--log-level <LEVEL>` | | error \| warn \| info \| debug \| trace |
| `--format <FORMAT>` | | json \| table (default: table) |
| `--out <OUT>` | | stdout \| file |

## Configuration

pgqrs loads configuration in priority order:

1. **Command-line arguments** (highest priority)
2. **Environment variables**
3. **Configuration file**
4. **Defaults**

### Environment Variables

```bash
# Required
export PGQRS_DSN="postgresql://user:pass@localhost/db"

# Optional
export PGQRS_SCHEMA="pgqrs"
export PGQRS_MAX_CONNECTIONS=32
export PGQRS_CONNECTION_TIMEOUT=60
export PGQRS_DEFAULT_LOCK_TIME=10
export PGQRS_DEFAULT_BATCH_SIZE=200
export PGQRS_CONFIG_FILE="path/to/config.yaml"
```

### Configuration File

Create `pgqrs.yaml` or `pgqrs.yml`:

```yaml
dsn: "postgresql://user:pass@localhost/db"
schema: "pgqrs"
max_connections: 16
connection_timeout_seconds: 30
default_lock_time_seconds: 5
default_max_batch_size: 100
```

---

## Schema Commands

### install

Install the pgqrs schema (schema must already exist).

```bash
pgqrs install
```

### uninstall

Remove the pgqrs schema and all data.

```bash
pgqrs uninstall
```

!!! warning
    This permanently deletes all queues, messages, and workers.

### verify

Verify the pgqrs installation.

```bash
pgqrs verify
```

---

## Queue Commands

### queue create

Create a new queue.

```bash
pgqrs queue create <name>
```

**Example:**

```bash
pgqrs queue create emails
pgqrs queue create --format json emails
```

### queue list

List all queues.

```bash
pgqrs queue list
pgqrs queue list --format json
```

### queue get

Show details for a queue.

```bash
pgqrs queue get <name>
```

### queue delete

Delete a queue.

```bash
pgqrs queue delete <name>
```

!!! warning
    This deletes the queue and all its messages.

### queue purge

Remove all messages from a queue without deleting the queue.

```bash
pgqrs queue purge <name>
```

### queue metrics

Show metrics for one or all queues.

```bash
# All queues
pgqrs queue metrics

# Specific queue
pgqrs queue metrics <name>
```

**Output:**

| Field | Description |
|-------|-------------|
| `pending` | Messages waiting to be processed |
| `locked` | Messages currently being processed |
| `archived` | Successfully processed messages |
| `total` | Total messages |

---

## Message Commands

### message enqueue

Add a message to a queue.

```bash
pgqrs message enqueue --queue <queue> --payload <json> [--delay <seconds>]
```

**Options:**

| Option | Required | Description |
|--------|----------|-------------|
| `--queue` | Yes | Queue name |
| `--payload` | Yes | JSON payload |
| `--delay` | No | Delay in seconds |

**Examples:**

```bash
# Immediate message
pgqrs message enqueue --queue emails --payload '{"to": "user@example.com"}'

# Delayed message (1 hour)
pgqrs message enqueue --queue reminders --payload '{"type": "follow_up"}' --delay 3600
```

### message dequeue

Fetch a message for processing.

```bash
pgqrs message dequeue --queue <queue> --worker <id> [--lock-time <seconds>]
```

**Options:**

| Option | Required | Description |
|--------|----------|-------------|
| `--queue` | Yes | Queue name |
| `--worker` | Yes | Worker ID |
| `--lock-time` | No | Lock duration in seconds |

### message archive

Archive a processed message.

```bash
pgqrs message archive --queue <queue> --id <id>
```

### message delete

Delete a message without archiving.

```bash
pgqrs message delete --queue <queue> --id <id>
```

### message get

Show message details.

```bash
pgqrs message get --queue <queue> --id <id>
```

### message count

Count pending messages in a queue.

```bash
pgqrs message count --queue <queue>
```

---

## Worker Commands

### worker create

Register a new worker.

```bash
pgqrs worker create --queue <queue> --host <host> --port <port>
```

**Example:**

```bash
pgqrs worker create --queue emails --host worker1.example.com --port 3000
```

### worker list

List all workers.

```bash
pgqrs worker list [--queue <queue>]
```

### worker get

Show worker details.

```bash
pgqrs worker get --id <id>
```

### worker stats

Show worker statistics for a queue.

```bash
pgqrs worker stats --queue <queue>
```

### worker stop

Mark a worker as stopped.

```bash
pgqrs worker stop --id <id>
```

### worker messages

Show messages assigned to a worker.

```bash
pgqrs worker messages --id <id>
```

### worker release

Release all messages from a worker.

```bash
pgqrs worker release --id <id>
```

### worker delete

Delete a worker (must have no messages).

```bash
pgqrs worker delete --id <id>
```

### worker purge

Remove old stopped workers.

```bash
pgqrs worker purge [--older-than <duration>]
```

**Duration format:** `7d`, `30d`, `24h`, etc.

**Example:**

```bash
# Remove workers stopped more than 30 days ago
pgqrs worker purge --older-than 30d
```

### worker health

Check worker health.

```bash
pgqrs worker health --queue <queue> [--max-age <seconds>]
```

**Example:**

```bash
# Alert if no heartbeat in 10 minutes
pgqrs worker health --queue emails --max-age 600
```

---

## Archive Commands

### archive list

List archived messages.

```bash
pgqrs archive list <queue> [--worker <id>]
```

### archive count

Count archived messages.

```bash
pgqrs archive count <queue> [--worker <id>]
```

### archive delete

Delete archived messages.

```bash
pgqrs archive delete <queue> [--worker <id>]
```

!!! tip "Archive Best Practices"
    - Regularly purge old archived messages
    - Monitor archive growth
    - Set retention policies

---

## Output Formats

### Table Format (Default)

```bash
pgqrs queue list
```

```
┌────┬────────────┬─────────────────────┐
│ ID │ Name       │ Created At          │
├────┼────────────┼─────────────────────┤
│ 1  │ emails     │ 2024-01-15 10:30:00 │
│ 2  │ reminders  │ 2024-01-15 10:31:00 │
└────┴────────────┴─────────────────────┘
```

### JSON Format

```bash
pgqrs queue list --format json
```

```json
[
  {"id": 1, "name": "emails", "created_at": "2024-01-15T10:30:00Z"},
  {"id": 2, "name": "reminders", "created_at": "2024-01-15T10:31:00Z"}
]
```

---

## Common Workflows

### Setup

```bash
# Set connection
export PGQRS_DSN="postgresql://localhost/mydb"

# Install schema
pgqrs install
pgqrs verify

# Create queue
pgqrs queue create tasks
```

### Send and Process Messages

```bash
# Send message
pgqrs message enqueue --queue tasks --payload '{"job": "process_order", "order_id": 123}'

# Process (in worker)
pgqrs message dequeue --queue tasks --worker 1 --lock-time 60

# Archive after processing
pgqrs message archive --queue tasks --id 1
```

### Monitor

```bash
# Queue status
pgqrs queue metrics tasks

# Worker health
pgqrs worker health --queue tasks

# Check backlog
pgqrs message count --queue tasks
```

### Cleanup

```bash
# Purge old workers
pgqrs worker purge --older-than 30d

# Clean archive
pgqrs archive delete tasks --older-than 90d

# Empty queue
pgqrs queue purge tasks
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Database connection error |
| 4 | Command not found |

---

## See Also

- [Installation](getting-started/installation.md)
- [Configuration](api/configuration.md)
- [Concepts](concepts/architecture.md)
