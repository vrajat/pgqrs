# Installation

This guide covers how to install pgqrs for both Rust and Python projects.

## Prerequisites

- **PostgreSQL 12+** - pgqrs uses `SKIP LOCKED` and other features from PostgreSQL 12+
- A PostgreSQL database you have access to (local, Docker, or cloud-hosted)

## Library Installation

=== "Rust"

    ### Using Cargo

    Add pgqrs to your `Cargo.toml`:

    ```toml
    [dependencies]
    pgqrs = "0.4"
    tokio = { version = "1", features = ["full"] }
    serde_json = "1"
    ```

    pgqrs is async-first and works with [Tokio](https://tokio.rs/).

    ### Verify Installation

    ```rust
    use pgqrs::Config;

    fn main() {
        let config = Config::from_dsn("postgresql://localhost/mydb");
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
# Start a PostgreSQL container
docker run --name pgqrs-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:15

# Connection string
# postgresql://postgres:postgres@localhost:5432/postgres
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

=== "Windows"

    Download and install from [postgresql.org](https://www.postgresql.org/download/windows/)

### Option 3: Cloud PostgreSQL

pgqrs works with any PostgreSQL-compatible database:

- **AWS RDS** - Get connection string from RDS console
- **Google Cloud SQL** - Get connection string from Cloud Console
- **Azure Database for PostgreSQL** - Get connection string from Azure portal
- **Supabase** - Get connection string from project settings
- **Neon** - Get connection string from dashboard

## Installing the pgqrs Schema

Before using pgqrs, you need to install its schema in your database. You can do this via the CLI or programmatically.

### Using the CLI

```bash
# Install the CLI
cargo install pgqrs

# Set your database connection
export PGQRS_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

# Install the schema
pgqrs install

# Verify the installation
pgqrs verify
```

### Programmatically (Rust)

```rust
use pgqrs::{Admin, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?;

    // Install schema
    admin.install().await?;

    // Verify installation
    admin.verify().await?;

    println!("Schema installed successfully!");
    Ok(())
}
```

### Programmatically (Python)

```python
import asyncio
from pgqrs import Admin

async def main():
    admin = Admin("postgresql://localhost/mydb")

    # Install schema
    await admin.install()

    # Verify installation
    await admin.verify()

    print("Schema installed successfully!")

asyncio.run(main())
```

## Custom Schema

By default, pgqrs creates tables in the `public` schema. To use a custom schema:

### CLI

```bash
# Create the schema first (in psql)
# CREATE SCHEMA IF NOT EXISTS pgqrs;

# Install with custom schema
pgqrs --schema pgqrs install
```

### Rust

```rust
let config = Config::from_dsn_with_schema(
    "postgresql://localhost/mydb",
    "pgqrs"
)?;
```

## What's Next?

- [Quickstart](quickstart.md) - Create your first queue and send messages
- [Architecture](../concepts/architecture.md) - Understand how pgqrs works
