# Configuration API

`pgqrs` supports flexible configuration via connection strings, environment variables, configuration files, and programmatic builders.

## Basic Connection

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

For fine-grained control over connection pools, schema names, and timeouts.

=== "Rust"

    ```rust
    use pgqrs::Config;

    // Load from DSN and customize
    let config = Config::from_dsn("postgresql://localhost/mydb")
        .with_schema("pgqrs")
        .with_max_connections(32);

    let store = pgqrs::connect_with_config(&config).await?;
    ```

=== "Python"

    ```python
    import pgqrs

    # improved Config object
    config = pgqrs.Config("postgresql://localhost/mydb", schema="pgqrs")
    config.max_connections = 32
    config.connection_timeout_seconds = 60

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
| `dsn` | PostgreSQL connection string | **Required** |
| `schema` | Database schema name | `public` |
| `max_connections` | Connection pool size | `16` |
| `connection_timeout_seconds` | Timeout for acquiring connection | `30` |
| `default_lock_time_seconds` | Default message visibility timeout | `5` |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PGQRS_DSN` | Connection string |
| `PGQRS_SCHEMA` | Schema name |
| `PGQRS_MAX_CONNECTIONS` | Pool size |

### Python Environment Usage

```python
import os
import pgqrs

dsn = os.environ.get("PGQRS_DSN", "postgresql://localhost/mydb")
store = await pgqrs.connect(dsn)
```

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
