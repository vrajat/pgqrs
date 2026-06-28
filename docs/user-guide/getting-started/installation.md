# Installation

This guide covers how to install pgqrs for both Rust and Python projects.

## Runtime Support

pgqrs supports PostgreSQL only.

| Runtime | Best For | Prerequisites |
|---------|----------|---------------|
| **PostgreSQL** | Production, local development, testing | PostgreSQL 12+ server |

See [PostgreSQL Runtime Guide](../concepts/backends.md) for deployment notes.

## Library Installation

=== "Rust"

    ### Using Cargo

    Add pgqrs to your `Cargo.toml`:

    ```toml
    [dependencies]
    pgqrs = "0.15.3"
    ```

    pgqrs is async-first and works with [Tokio](https://tokio.rs/).

    ### Verify Installation

    ```rust
    use pgqrs::Config;

    fn main() {
        let pg_config = Config::from_dsn("postgresql://localhost/mydb");
        println!("pgqrs configured successfully!");
    }
    ```

=== "Python"

    ### Using pip

    ```bash
    pip install pgqrs
    ```

    ### Using uv (recommended)

    ```bash
    uv add pgqrs
    ```

    ### Using Poetry

    ```bash
    poetry add pgqrs
    ```

    ### Verify Installation

    ```python
    import pgqrs
    print("pgqrs installed successfully!")
    ```

## PostgreSQL Setup

### Option 1: Docker (Recommended for Development)

```bash
docker run --name pgqrs-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:15
```

Connection string:

```text
postgresql://postgres:postgres@localhost:5432/postgres
```

### Option 2: Local PostgreSQL

Install PostgreSQL using your system package manager:

=== "macOS (Homebrew)"

    ```bash
    brew install postgresql@15
    brew services start postgresql@15
    ```

=== "Ubuntu/Debian"

    ```bash
    sudo apt-get update
    sudo apt-get install postgresql-15
    sudo systemctl start postgresql
    ```

### Option 3: Cloud PostgreSQL

pgqrs works with any PostgreSQL-compatible database:

- **AWS RDS**
- **Google Cloud SQL**
- **Azure Database for PostgreSQL**
- **Supabase**
- **Neon**

## Installing the pgqrs Schema

Before using pgqrs, install its schema in your database.

=== "Rust"

    ```rust
    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let dsn = "postgresql://localhost/mydb";
        let store = pgqrs::connect(dsn).await?;

        pgqrs::admin(&store).install().await?;
        pgqrs::admin(&store).verify().await?;

        println!("Schema installed successfully!");
        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def main():
        dsn = "postgresql://localhost/mydb"
        store = await pgqrs.connect(dsn)
        admin = pgqrs.admin(store)

        await admin.install()
        await admin.verify()

        print("Schema installed successfully!")

    asyncio.run(main())
    ```

## Installing the pgqrs-extension (Optional - Self-Hosted Deployments)

For self-hosted deployments, you can install the PostgreSQL extension to run the coordinator loop and built-in capabilities natively inside Postgres without running the separate `pgqrs-admin` coordinator process.

### 1. Build and Install the Extension

The extension is packaged using `pgrx`.

```bash
# Build the extension library
cargo build -p pgqrs-extension

# Install the extension into your PostgreSQL instance
cargo pgrx install -p pgqrs-extension
```

### 2. Configure PostgreSQL

Add the extension to `shared_preload_libraries` in `postgresql.conf`:

```ini
shared_preload_libraries = 'pgqrs_extension'
```

Restart your PostgreSQL server to load the extension.

### 3. Initialize the Extension

In your database, run the following SQL command to register the extension:

```sql
CREATE EXTENSION pgqrs_extension;
```

### 4. Extension Configuration Parameters (GUC)

Configure the following options in `postgresql.conf` to customize the background worker and built-in execution:

```ini
# Enable the background worker coordinator loop (defaults to true)
pgqrs.coordinator_enabled = true

# The database containing the pgqrs tables (defaults to 'postgres')
pgqrs.database = 'postgres'

# The user used to execute SPI queries (defaults to 'postgres')
pgqrs.user = 'postgres'

# Interval for scheduler sweeps and cron execution in milliseconds
pgqrs.coordinator_interval_ms = 1000

# Comma-separated list of queues to poll and execute via native built-ins
pgqrs.builtin_queues = 'sql-tasks,sys-maintenance'
```

## Custom Schema

Custom schemas apply to PostgreSQL.

By default, pgqrs creates tables in the `public` schema. To use a custom schema:

=== "Rust"

    ```rust
    let config = Config::from_dsn_with_schema(
        "postgresql://localhost/mydb",
        "pgqrs"
    )?;
    ```

=== "Python"

    ```python
    config = pgqrs.Config("postgresql://localhost/mydb", schema="pgqrs")
    store = await pgqrs.connect_with(config)
    ```

## What's Next?

- [Quickstart](quickstart.md) - Create your first queue and send messages
- [Architecture](../concepts/architecture.md) - Understand how pgqrs works
