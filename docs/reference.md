# Reference

## Configuration

pgqrs uses a prioritized configuration system. Configuration is loaded in the following order (highest priority first):

### 1. Command Line Arguments (Highest Priority)
```bash
# Override DSN via command line
pgqrs --dsn "postgresql://user:pass@localhost/db" verify

# Override config file location
pgqrs --config "custom-config.yaml" verify
```

### 2. Environment Variables
```bash
# Required: Database connection string
export PGQRS_DSN="postgresql://user:pass@localhost/db"

# Optional: Schema name for pgqrs tables (default: public)
export PGQRS_SCHEMA="pgqrs"

# Optional: Connection pool settings
export PGQRS_MAX_CONNECTIONS=32
export PGQRS_CONNECTION_TIMEOUT=60

# Optional: Default job settings
export PGQRS_DEFAULT_LOCK_TIME=10
export PGQRS_DEFAULT_BATCH_SIZE=200

# Optional: Config file location
export PGQRS_CONFIG_FILE="path/to/config.yaml"
```

### 3. Configuration File
Create a YAML configuration file (default locations: `pgqrs.yaml`, `pgqrs.yml`):

```yaml
# Required: Database connection string
dsn: "postgresql://user:pass@localhost/db"

# Optional: Schema name for pgqrs tables (default: public)
schema: "pgqrs"

# Optional: Connection pool settings (defaults shown)
max_connections: 16
connection_timeout_seconds: 30

# Optional: Default job settings (defaults shown)
default_lock_time_seconds: 5
default_max_batch_size: 100
```

### 4. Programmatic Configuration
```rust
use pgqrs::config::Config;

// Create from explicit DSN (uses 'public' schema by default)
let config = Config::from_dsn("postgresql://user:pass@localhost/db");

// Create with custom schema
let config = Config::from_dsn_with_schema(
    "postgresql://user:pass@localhost/db",
    "my_schema"
)?;

// Load from environment variables
let config = Config::from_env()?;

// Load from specific file
let config = Config::from_file("config.yaml")?;

// Load automatically with priority order
let config = Config::load()?;

// Load with explicit overrides (for CLI tools)
let config = Config::load_with_options(
    Some("postgresql://explicit:dsn@localhost/db"), // DSN override
    Some("custom-config.yaml")                      // Config file override
)?;
```

### Configuration Reference

| Field | Environment Variable | Description | Default |
|-------|---------------------|-------------|---------|
| `dsn` | `PGQRS_DSN` | PostgreSQL connection string | **Required** |
| `schema` | `PGQRS_SCHEMA` | Schema name for pgqrs tables | `public` |
| `max_connections` | `PGQRS_MAX_CONNECTIONS` | Maximum database connections | 16 |
| `connection_timeout_seconds` | `PGQRS_CONNECTION_TIMEOUT` | Connection timeout in seconds | 30 |
| `default_lock_time_seconds` | `PGQRS_DEFAULT_LOCK_TIME` | Default job lock time | 5 |
| `default_max_batch_size` | `PGQRS_DEFAULT_BATCH_SIZE` | Default batch size for operations | 100 |

## CLI Reference

The `pgqrs` CLI provides commands for installing, managing, and inspecting your queue system.

---

### Global Options

- `-d, --dsn <DSN>`: Database URL (overrides all other config sources)
- `-s, --schema <SCHEMA>`: Schema name for pgqrs tables (default: public)
- `-c, --config <CONFIG>`: Config file path
- `--log-dest <stderr|file>`: Log destination (default: stderr)
- `--log-level <error|warn|info|debug|trace>`: Log level (default: info)
- `--format <json|table>`: Output format (default: table)
- `--out <stdout|file>`: Output destination (default: stdout)

All commands accept these global options. Example:

```sh
pgqrs --dsn postgresql://postgres:postgres@localhost:5432/postgres install
```

---

## Top-level Commands

### install
Install the pgqrs schema (schema must exist).

**Example:**
```sh
pgqrs install
```

### uninstall
Uninstall the pgqrs schema.

**Example:**
```sh
pgqrs uninstall
```

### verify
Verify the pgqrs installation.

**Example:**
```sh
pgqrs verify
```

### queue <SUBCOMMAND>
Queue management commands.

#### queue create <name>
Create a new queue.

**Example:**
```sh
pgqrs queue create jobs
```

#### queue list
List all queues.

**Example:**
```sh
pgqrs queue list
```

#### queue get <name>
Show details for a queue.

**Example:**
```sh
pgqrs queue get jobs
```

#### queue delete <name>
Delete a queue.

**Example:**
```sh
pgqrs queue delete jobs
```

#### queue purge <name>
Purge all messages from a queue.

**Example:**
```sh
pgqrs queue purge jobs
```

#### queue metrics [<name>]
Show metrics for a queue or all queues.

**Example:**
```sh
pgqrs queue metrics jobs
pgqrs queue metrics
```

---

### message <SUBCOMMAND>
Message management commands.

#### message enqueue --queue <queue> --payload <payload> [--delay <seconds>]
Enqueue a message to a queue. Payload must be valid JSON.

**Options:**
- `--queue <queue>`: Queue name (required)
- `--payload <payload>`: JSON payload (required)
- `-d, --delay <seconds>`: Delay before message is available (optional)

**Example:**
```sh
pgqrs message enqueue --queue jobs --payload '{"task": "foo"}'
pgqrs message enqueue --queue jobs --payload '{"task": "bar"}' --delay 60
```

#### message dequeue --queue <queue> --worker <id> [--lock-time <seconds>]
Dequeue a message from a queue for a worker.

**Options:**
- `--queue <queue>`: Queue name (required)
- `--worker <id>`: Worker ID (required)
- `--lock-time <seconds>`: Lock time in seconds (optional)

**Example:**
```sh
pgqrs message dequeue --queue jobs --worker 1
```

#### message archive --queue <queue> --id <id>
Archive a message by ID.

**Example:**
```sh
pgqrs message archive --queue jobs --id 42
```

#### message delete --queue <queue> --id <id>
Delete a message by ID.

**Example:**
```sh
pgqrs message delete --queue jobs --id 42
```

#### message count --queue <queue>
Show pending message count for a queue.

**Example:**
```sh
pgqrs message count --queue jobs
```

#### message get --queue <queue> --id <id>
Show message details by ID.

**Example:**
```sh
pgqrs message get --queue jobs --id 42
```

---

### worker <SUBCOMMAND>
Worker management commands.

#### worker create --queue <queue> --host <host> --port <port>
Register a new worker for a queue.

**Example:**
```sh
pgqrs worker create --queue jobs --host worker1 --port 3000
```

#### worker list [--queue <queue>]
List all workers, optionally filtered by queue.

**Example:**
```sh
pgqrs worker list
pgqrs worker list --queue jobs
```

#### worker get --id <id>
Show details for a worker.

**Example:**
```sh
pgqrs worker get --id 1
```

#### worker stats --queue <queue>
Show worker statistics for a queue.

**Example:**
```sh
pgqrs worker stats --queue jobs
```

#### worker stop --id <id>
Stop a worker (mark as stopped).

**Example:**
```sh
pgqrs worker stop --id 1
```

#### worker messages --id <id>
Show messages assigned to a worker.

**Example:**
```sh
pgqrs worker messages --id 1
```

#### worker release --id <id>
Release messages from a worker.

**Example:**
```sh
pgqrs worker release --id 1
```

#### worker delete --id <id>
Delete a worker (only if no associated messages).

**Example:**
```sh
pgqrs worker delete --id 1
```

#### worker purge [--older-than <duration>]
Remove old stopped workers (default: 7d).

**Example:**
```sh
pgqrs worker purge --older-than 30d
```

#### worker health --queue <queue> [--max-age <seconds>]
Check worker health for a queue (default max-age: 300).

**Example:**
```sh
pgqrs worker health --queue jobs --max-age 600
```

---

### archive <SUBCOMMAND>
Archive management commands.

#### archive list <queue> [--worker <id>]
List archived messages, optionally filtered by worker.

**Example:**
```sh
pgqrs archive list jobs
pgqrs archive list jobs --worker 1
```

#### archive delete <queue> [--worker <id>]
Delete archived messages, optionally filtered by worker.

**Example:**
```sh
pgqrs archive delete jobs
pgqrs archive delete jobs --worker 1
```

#### archive count <queue> [--worker <id>]
Count archived messages, optionally filtered by worker.

**Example:**
```sh
pgqrs archive count jobs
pgqrs archive count jobs --worker 1
```

### Top-level commands

- `install` — Install pgqrs schema
- `uninstall` — Uninstall pgqrs schema
- `verify` — Verify installation
- `queue <subcommand>` — Queue management
- `message <subcommand>` — Message management

### Queue commands

- `queue create <name>` — Create a new queue
- `queue list` — List all queues
- `queue delete <name>` — Delete a queue
- `queue purge <name>` — Purge all messages from a queue
- `queue metrics [<name>]` — Show metrics for a queue or all queues

### Message commands

- `message send <queue> <payload> [--delay <seconds>]` — Send a message (payload is JSON)
- `message read <queue> [--count <n>] [--lock-time <seconds>] [--message-type <type>]` — Read messages
- `message dequeue <queue>` — Read and return one message
- `message delete <queue> <id>` — Delete a message by ID
- `message count <queue>` — Show pending message count
- `message show <queue> <id>` — Show message details by ID
- `message show <queue> <id> --archive` — Show archived message details by ID

### Archive commands

Archive functionality is accessed through existing commands with the `--archive` flag:

- `message show <queue> <id> --archive` — Show archived message details by ID
- `message count <queue> --archive` — Show archived message count
- `message read <queue> --archive` — Read/list archived messages

#### Archive Usage Examples

```bash
# Show details of an archived message
pgqrs message show email_queue 12345 --archive

# Check how many messages are archived
pgqrs message count email_queue --archive

# List archived messages (use read command with --archive flag)
pgqrs message read email_queue --archive --count 10
```

#### Archive System Overview

The archive system automatically creates archive tables (`archive_<queue_name>`) when queues are created. Archived messages retain all original data plus additional metadata:

- `archived_at` — Timestamp when the message was archived
- `processing_duration` — Time taken to process the message (in milliseconds)

#### Archive Best Practices

- **Archive after processing**: Archive messages only after successful processing
- **Regular cleanup**: Periodically purge old archived messages to manage database size
- **Monitoring**: Track archive growth as part of your queue metrics
- **Retention policy**: Establish how long to keep archived messages based on your compliance needs

### Output and Logging Options

All commands support global flags:

- `-d, --dsn <DSN>` — Database URL (highest priority, overrides all other config sources)
- `-c, --config <CONFIG>` — Config file path (overrides environment variables and defaults)
- `--log-dest <stderr|file>` — Log destination
- `--log-level <error|warn|info|debug|trace>` — Log level
- `--format <json|table>` — Output format
- `--out <stdout|file>` — Output destination
