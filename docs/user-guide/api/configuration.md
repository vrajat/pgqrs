# Configuration API

`pgqrs` supports flexible configuration via connection strings, environment variables, configuration files, and programmatic builders.

## Basic Connection

pgqrs selects the backend from the DSN scheme:

- `postgresql://...` or `postgres://...`
- `sqlite:///...`
- `turso:///...`
- `s3://bucket/key.sqlite`

=== "Rust"

    ```rust
    // Connect using DSN string
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;
    ```

=== "Python"

    ```python
    # Connect using DSN string
    store = await pgqrs.connect("postgresql://localhost/mydb")
    ```

## Advanced Configuration

For fine-grained control over connection pools, schema names, queue defaults, validation, and S3 durability behavior.

=== "Rust"

    ```rust
    use pgqrs::Config;
    use pgqrs::store::s3::DurabilityMode;

    // Load from DSN and customize
    let mut config = Config::from_dsn("s3://my-bucket/queue.sqlite")
        .with_max_connections(32);
    config.s3.mode = DurabilityMode::Durable;

    let store = pgqrs::connect_with_config(&config).await?;
    ```

=== "Python"

    ```python
    import pgqrs

    config = pgqrs.Config("postgresql://localhost/mydb", schema="pgqrs")
    config.max_connections = 32
    config.connection_timeout_seconds = 60
    config.max_payload_size_bytes = 512 * 1024
    config.max_enqueue_per_second = None

    store = await pgqrs.connect_with(config)
    ```

## Configuration Sources (Rust)

Rust applications support hierarchical configuration loading:

1. CLI Arguments
2. Environment Variables
3. Config File (`pgqrs.yaml`)
4. Defaults

```rust
// Auto-load from environment or file
let config = Config::load()?;
```

## Configuration Fields

| Field | Description | Default |
|-------|-------------|---------|
| `dsn` | Backend DSN | **Required** |
| `schema` | Database schema name | `public` |
| `max_connections` | Connection pool size | `16` |
| `connection_timeout_seconds` | Timeout for acquiring connection | `30` |
| `default_lock_time_seconds` | Default message visibility timeout | `5` |
| `default_max_batch_size` | Default dequeue batch size | `100` |
| `max_read_ct` | Dead-letter threshold | `5` |
| `heartbeat_interval` | Worker heartbeat interval | `5` |
| `poll_interval_ms` | Poll loop sleep interval | `250` |
| `sqlite.use_wal` | Enable SQLite WAL mode | `true` |
| `s3.mode` | `durable` or `local` for `s3://...` DSNs | `durable` |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PGQRS_DSN` | Connection string |
| `PGQRS_SCHEMA` | Schema name |
| `PGQRS_MAX_CONNECTIONS` | Pool size |
| `PGQRS_CONNECTION_TIMEOUT` | Connection timeout in seconds |
| `PGQRS_DEFAULT_LOCK_TIME` | Default visibility timeout |
| `PGQRS_DEFAULT_BATCH_SIZE` | Default dequeue batch size |
| `PGQRS_MAX_READ_CT` | Dead-letter threshold |
| `PGQRS_HEARTBEAT_INTERVAL` | Worker heartbeat interval |
| `PGQRS_POLL_INTERVAL_MS` | Poll loop interval |
| `PGQRS_SQLITE_USE_WAL` | SQLite WAL toggle |
| `PGQRS_S3_MODE` | S3 durability mode: `durable` or `local` |

### Object Store Environment

For `s3://...` DSNs, pgqrs also reads AWS-compatible object store settings:

- `AWS_REGION`
- `AWS_ENDPOINT_URL`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### Python Environment Usage

```python
import os
import pgqrs

dsn = os.environ.get("PGQRS_DSN", "postgresql://localhost/mydb")
store = await pgqrs.connect(dsn)
```

## Configuration File Example

```yaml
dsn: "s3://my-bucket/queue.sqlite"
max_connections: 16
default_lock_time_seconds: 5
default_max_batch_size: 100
poll_interval_ms: 250
sqlite:
  use_wal: false
s3:
  mode: durable
```

## Python Validation Settings

Python's `Config` surface exposes the same validation and enqueue-rate controls used by Rust:

- `max_payload_size_bytes`
- `max_string_length`
- `max_object_depth`
- `forbidden_keys`
- `required_keys`
- `max_enqueue_per_second`
- `max_enqueue_burst`

## SSL/TLS

Secure connections are configured via the DSN parameters.

`postgresql://user:pass@host/db?sslmode=require`

| Mode | Description |
|------|-------------|
| `disable` | No SSL |
| `prefer` | Try SSL, fall back to plain |
| `require` | Encrypt, do not verify cert |
| `verify-ca` | Verify server certificate |
| `verify-full` | Verify cert and hostname |
