# Installation

This guide covers how to install pgqrs for both Rust and Python projects.

## Choose Your Backend

pgqrs supports PostgreSQL, SQLite, Turso, and S3-backed queues. Choose based on your needs:

| Backend | Best For | Prerequisites |
|---------|----------|---------------|
| **PostgreSQL** | Production, multi-worker | PostgreSQL 12+ server |
| **SQLite** | Testing, local development, embedded | None (embedded) |
| **Turso** | Embedded SQLite-compatible deployments | Turso or local Turso database |
| **S3** | Remote queue durability without a database server | S3 bucket and AWS-compatible credentials |

See [Backend Selection Guide](../concepts/backends.md) for detailed comparison.

## Library Installation

=== "Rust"

    ### Using Cargo

    Add pgqrs to your `Cargo.toml`:

    ```toml
    [dependencies]
    # PostgreSQL only (default)
    pgqrs = "0.15.3"

    # SQLite only
    pgqrs = { version = "0.15.1", default-features = false, features = ["sqlite"] }

    # Turso only
    pgqrs = { version = "0.15.1", default-features = false, features = ["turso"] }

    # S3 only
    pgqrs = { version = "0.15.1", default-features = false, features = ["s3"] }

    # All backends
    pgqrs = { version = "0.15.1", features = ["full"] }
    ```

    pgqrs is async-first and works with [Tokio](https://tokio.rs/).

    ### Verify Installation

    ```rust
    use pgqrs::Config;

    fn main() {
        // PostgreSQL
        let pg_config = Config::from_dsn("postgresql://localhost/mydb");

        // SQLite (requires `sqlite` feature)
        let sqlite_config = Config::from_dsn("sqlite:///path/to/db.sqlite");

        // S3 (requires `s3` feature plus AWS environment variables)
        let s3_config = Config::from_dsn("s3://my-bucket/queue.sqlite");

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

## Backend Setup

### PostgreSQL Setup

#### Option 1: Docker (Recommended for Development)

```bash
# Start a PostgreSQL container
docker run --name pgqrs-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:15

# Connection string
# postgresql://postgres:postgres@localhost:5432/postgres
```

#### Option 2: Local PostgreSQL

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

#### Option 3: Cloud PostgreSQL

pgqrs works with any PostgreSQL-compatible database:

- **AWS RDS** - Get connection string from RDS console
- **Google Cloud SQL** - Get connection string from Cloud Console
- **Azure Database for PostgreSQL** - Get connection string from Azure portal
- **Supabase** - Get connection string from project settings
- **Neon** - Get connection string from dashboard

### SQLite and Turso Setup

SQLite and Turso do not require a separate schema server process. Point pgqrs at a file path:

```text
sqlite:///var/lib/myapp/queue.db
turso:///var/lib/myapp/queue.db
```

### S3 Setup

The S3 backend requires an object-store endpoint plus credentials in the environment.

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Optional for LocalStack or other S3-compatible endpoints
export AWS_ENDPOINT_URL=http://localhost:4566

# Optional; defaults to durable for s3:// DSNs
export PGQRS_S3_MODE=durable
```

Then use an S3 DSN such as:

```text
s3://my-bucket/queue.sqlite
```

## Installing the pgqrs Schema

Before using pgqrs, you need to install its schema in your database.

For `s3://...` DSNs, this step bootstraps the remote object if it does not exist yet.

=== "Rust"

    ```rust
    use pgqrs::Config;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let dsn = "postgresql://localhost/mydb";
        let store = pgqrs::connect(dsn).await?;

        // Install schema
        pgqrs::admin(&store).install().await?;

        // Verify installation
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
        # Connect to database
        dsn = "postgresql://localhost/mydb"
        admin = pgqrs.admin(dsn)

        # Install schema
        await admin.install()

        # Verify installation
        await admin.verify()

        print("Schema installed successfully!")

    asyncio.run(main())
    ```

## Custom Schema

Custom schemas apply to SQL backends such as PostgreSQL. SQLite, Turso, and S3-backed queues ignore the schema name.

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
    # Set schema during Admin initialization
    admin = pgqrs.Admin(dsn, schema="pgqrs")
    ```

## What's Next?

- [Quickstart](quickstart.md) - Create your first queue and send messages
- [S3 Queue Guide](../guides/s3-queue.md) - Bootstrap and operate an S3-backed queue
- [Architecture](../concepts/architecture.md) - Understand how pgqrs works
