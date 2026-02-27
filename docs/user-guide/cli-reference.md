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
| `--dsn <DSN>` | `-d` | Database URL (highest priority, overrides all config sources) |
| `--schema <SCHEMA>` | `-s` | Schema name (default: public, must exist before install) |
| `--config <CONFIG>` | `-c` | Config file path (overrides environment variables and defaults) |
| `--log-dest <LOG_DEST>` | | Log destination: stderr or file path [default: stderr] |
| `--log-level <LOG_LEVEL>` | | Log level: error, warn, info, debug, trace [default: info] |
| `--format <FORMAT>` | | Output format: json, table [default: table] |
| `--out <OUT>` | | Output destination: stdout or file path [default: stdout] |

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

## Admin Commands

Administrative operations for managing the pgqrs installation and system-wide state.

### install

Install the pgqrs schema in your database.

!!! warning
    The target schema (e.g., `public` or your custom schema) must already exist.

```bash
pgqrs admin install
```

### verify

Verify the pgqrs installation by checking that all required tables and indexes exist.

```bash
pgqrs admin verify
```

### stats

Get system-wide statistics across all queues and workers.

```bash
pgqrs admin stats
```

### reclaim

Reclaim messages from zombie workers (workers that have stopped sending heartbeats).

```bash
pgqrs admin reclaim
```

---

## Queue Commands

Commands for managing queues and viewing queue-level metrics.

### create

Create a new queue.

```bash
pgqrs queue create <name>
```

**Example:**

```bash
pgqrs queue create emails
pgqrs queue create --format json emails
```

### list

List all queues in the database.

```bash
pgqrs queue list
pgqrs queue list --format json
```

### get

Show details for a specific queue.

```bash
pgqrs queue get <name>
```

### messages

List messages currently in the queue.

```bash
pgqrs queue messages <name>
```

### archive-dlq

Move dead letter queue messages to the archive.

```bash
pgqrs queue archive-dlq <name>
```

### delete

Delete a queue.

!!! warning
    This deletes the queue and **all** its messages.

```bash
pgqrs queue delete <name>
```

### purge

Remove all messages from a queue without deleting the queue itself.

```bash
pgqrs queue purge <name>
```

### metrics

Show detailed metrics for one or all queues.

```bash
# All queues
pgqrs queue metrics

# Specific queue
pgqrs queue metrics <name>
```

**Output attributes include:**
- Pending and Locked message counts
- Archived metrics
- Error rates

---

## Worker Commands

Commands for managing, monitoring, and cleaning up workers.

### list

List all active workers.

```bash
pgqrs worker list
```

### get

Get details for a specific worker by its ID.

```bash
pgqrs worker get <id>
```

### messages

Get messages currently assigned to a specific worker.

```bash
pgqrs worker messages <id>
```

### release-messages

Force-release all messages currently held by a worker back to the queue.

```bash
pgqrs worker release-messages <id>
```

### suspend

Suspend a worker (`Ready` -> `Suspended`). It will stop processing new messages but finish current ones.

```bash
pgqrs worker suspend <id>
```

### resume

Resume a suspended worker (`Suspended` -> `Ready`).

```bash
pgqrs worker resume <id>
```

### shutdown

Shutdown a worker. The worker must be suspended first.

```bash
pgqrs worker shutdown <id>
```

### purge

Purge old stopped workers from the database to save space.

```bash
pgqrs worker purge
```

### delete

Delete a specific worker record.

```bash
pgqrs worker delete <id>
```

### heartbeat

Manually update a worker's heartbeat.

```bash
pgqrs worker heartbeat <id>
```

### stats

Get statistics for a specific worker.

```bash
pgqrs worker stats <id>
```

### health

Check the health status of a specific worker or workers.

```bash
pgqrs worker health
```

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
