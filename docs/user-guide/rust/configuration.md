# Configuration

pgqrs uses a flexible configuration system that supports multiple sources.

## Configuration Sources

Configuration is loaded in priority order (highest first):

1. **Command line arguments** (CLI only)
2. **Environment variables**
3. **Configuration file** (YAML)
4. **Programmatic configuration**
5. **Default values**

## Creating Configuration

### From DSN

The simplest way to create configuration:

```rust
use pgqrs::Config;

// Uses 'public' schema by default
let config = Config::from_dsn("postgresql://user:pass@localhost:5432/mydb");
```

### With Custom Schema

```rust
let config = Config::from_dsn_with_schema(
    "postgresql://user:pass@localhost:5432/mydb",
    "pgqrs"
)?;
```

### From Environment

```rust
// Reads PGQRS_DSN, PGQRS_SCHEMA, etc.
let config = Config::from_env()?;
```

### From File

```rust
// Load from specific file
let config = Config::from_file("pgqrs.yaml")?;

// Or auto-discover (looks for pgqrs.yaml, pgqrs.yml)
let config = Config::load()?;
```

### With CLI Overrides

For CLI applications that need to override file/env config:

```rust
let config = Config::load_with_options(
    Some("postgresql://explicit:dsn@localhost/db"), // DSN override
    Some("custom-config.yaml")                       // Config file override
)?;
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PGQRS_DSN` | PostgreSQL connection string | **Required** |
| `PGQRS_SCHEMA` | Schema for pgqrs tables | `public` |
| `PGQRS_MAX_CONNECTIONS` | Maximum pool connections | `16` |
| `PGQRS_CONNECTION_TIMEOUT` | Connection timeout (seconds) | `30` |
| `PGQRS_DEFAULT_LOCK_TIME` | Default message lock time (seconds) | `5` |
| `PGQRS_DEFAULT_BATCH_SIZE` | Default batch size for operations | `100` |
| `PGQRS_CONFIG_FILE` | Config file path | Auto-discover |

### Example

```bash
export PGQRS_DSN="postgresql://postgres:password@localhost:5432/myapp"
export PGQRS_SCHEMA="pgqrs"
export PGQRS_MAX_CONNECTIONS=32
export PGQRS_DEFAULT_LOCK_TIME=10
```

## Configuration File

Create a `pgqrs.yaml` or `pgqrs.yml` file:

```yaml
# Required: Database connection string
dsn: "postgresql://user:pass@localhost:5432/mydb"

# Optional: Schema name (default: public)
schema: "pgqrs"

# Optional: Connection pool settings
max_connections: 16
connection_timeout_seconds: 30

# Optional: Default operation settings
default_lock_time_seconds: 5
default_max_batch_size: 100
```

## DSN Format

The PostgreSQL connection string follows the standard format:

```
postgresql://[user[:password]@][host][:port][/database][?params]
```

### Examples

```
# Basic
postgresql://localhost/mydb

# With credentials
postgresql://user:password@localhost:5432/mydb

# With SSL
postgresql://user:password@host:5432/mydb?sslmode=require

# Cloud providers (example)
postgresql://user:password@project-id.us-east1.sql.gcloud.com:5432/mydb?sslmode=require
```

### Connection Parameters

Common query parameters:

| Parameter | Description |
|-----------|-------------|
| `sslmode` | SSL mode: `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `connect_timeout` | Connection timeout in seconds |
| `application_name` | Application name for logging |

## Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | `String` | Required | PostgreSQL connection string |
| `schema` | `String` | `"public"` | Schema for pgqrs tables |
| `max_connections` | `u32` | `16` | Max connections in pool |
| `connection_timeout_seconds` | `u64` | `30` | Connection timeout |
| `default_lock_time_seconds` | `i64` | `5` | Default visibility timeout |
| `default_max_batch_size` | `i32` | `100` | Default batch size |

## Best Practices

### Development

```yaml
# pgqrs.yaml
dsn: "postgresql://postgres:postgres@localhost:5432/dev_db"
schema: "public"
max_connections: 4
default_lock_time_seconds: 30  # Longer for debugging
```

### Production

```yaml
# pgqrs.yaml
dsn: "${PGQRS_DSN}"  # Use environment variable
schema: "pgqrs"
max_connections: 32
connection_timeout_seconds: 10
default_lock_time_seconds: 5
default_max_batch_size: 200
```

```bash
# Environment
export PGQRS_DSN="postgresql://app:secure_password@prod-db.example.com:5432/production?sslmode=require"
```

### Using with Connection Poolers

When using pgBouncer or pgcat:

```yaml
dsn: "postgresql://app:password@pgbouncer:6432/mydb"
max_connections: 50  # Can be higher than actual DB connections
connection_timeout_seconds: 60  # Longer for pool queuing
```

## Code Examples

### Application Setup

```rust
use pgqrs::{Admin, Config};

async fn setup() -> Result<Admin, Box<dyn std::error::Error>> {
    // Try loading from file/env, fall back to default
    let config = Config::load().unwrap_or_else(|_| {
        Config::from_dsn("postgresql://localhost/mydb")
    });

    let admin = Admin::new(&config).await?;
    Ok(admin)
}
```

### Environment-Aware Configuration

```rust
use pgqrs::Config;

fn get_config() -> Result<Config, Box<dyn std::error::Error>> {
    let env = std::env::var("APP_ENV").unwrap_or_else(|_| "development".into());

    match env.as_str() {
        "production" => {
            // Production: require explicit DSN
            Config::from_env()
        }
        "test" => {
            // Test: use test database
            Ok(Config::from_dsn("postgresql://postgres@localhost/test_db"))
        }
        _ => {
            // Development: use config file or defaults
            Config::from_file("pgqrs.yaml").or_else(|_| {
                Ok(Config::from_dsn("postgresql://postgres@localhost/dev_db"))
            })
        }
    }
}
```

### Custom Pool Configuration

```rust
use pgqrs::Config;
use sqlx::postgres::PgPoolOptions;

async fn custom_pool_setup() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_dsn("postgresql://localhost/mydb");

    // Create custom pool with specific settings
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            config.connection_timeout_seconds
        ))
        .connect(&config.dsn)
        .await?;

    // Use pool with Admin, Producer, Consumer...
    Ok(())
}
```

## See Also

- [Installation](../getting-started/installation.md) - Setting up pgqrs
- [Admin API](admin.md) - Schema management
- [CLI Reference](../cli-reference.md) - Command-line options
