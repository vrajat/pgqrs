# Installation

This guide covers how to set up `pgqrs`. The architecture consists of:
1. **The Database:** PostgreSQL (13+) storing all queues, runs, and step cache tables.
2. **The Coordinator:** Runs the scheduler (cron schedules, worker health check, lease sweeps).
3. **The SQL Worker:** Polls the database and executes your SQL-native workflows.

---

## Step 1: Start the Core Infrastructure

To use the SQL API (and to run scheduled crons/maintenance), you need both the **coordinator** and the **SQL worker** daemons running and connected to PostgreSQL.

=== "Option A: Docker Compose (Recommended)"

    The easiest way to run a local development stack is using our pre-configured `docker-compose.yml` file. This starts a PostgreSQL database, the `pgqrs` coordinator, and the `pgqrs` SQL worker together:

    Create a `docker-compose.yml` file:
    ```yaml
    version: '3.8'

    services:
      db:
        image: postgres:15-alpine
        container_name: pgqrs-postgres
        ports:
          - "5432:5432"
        environment:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: postgres
        healthcheck:
          test: ["CMD-SHELL", "pg_isready -U postgres"]
          interval: 5s
          timeout: 5s
          retries: 5

      coordinator:
        image: vrajat/pgqrs:latest
        container_name: pgqrs-coordinator
        command: admin --dsn "postgres://postgres:postgres@db:5432/postgres"
        environment:
          - PGQRS_DSN=postgres://postgres:postgres@db:5432/postgres
        depends_on:
          db:
            condition: service_healthy

      sql-worker:
        image: vrajat/pgqrs:latest
        container_name: pgqrs-sql-worker
        command: sql-worker --queues invoice_processing,my_workflow --dsn "postgres://postgres:postgres@db:5432/postgres"
        environment:
          - PGQRS_DSN=postgres://postgres:postgres@db:5432/postgres
        depends_on:
          db:
            condition: service_healthy
    ```

    Run the stack in the background:
    ```bash
    docker compose up -d
    ```

=== "Option B: Embedded Coordinator (Self-Hosted Postgres)"

    For self-hosted databases, you can compile and install `pgqrs_extension` to run the coordinator loop natively inside the PostgreSQL server process.

    ```bash
    # Build and install the extension via pgrx
    cargo build -p pgqrs-extension
    cargo pgrx install -p pgqrs-extension
    ```

    Enable the extension in `postgresql.conf`:
    ```ini
    shared_preload_libraries = 'pgqrs_extension'
    pgqrs.coordinator_enabled = true
    ```
    *Restart PostgreSQL to apply.* Then initialize the extension in your database:
    ```sql
    CREATE EXTENSION pgqrs_extension;
    ```

    *Note: Even when using the extension for coordination, you must still run the external `pgqrs sql-worker` daemon to process SQL-native workflows.*

---

## Step 2: Install Client Libraries (Optional)

If you are using code-first application workflows instead of pure SQL, install the client library for your programming language:

=== "Rust API"

    Add the `pgqrs` crate to your application's `Cargo.toml`:

    ```toml
    [dependencies]
    pgqrs = "0.15.3"
    pgqrs-macros = "0.15.3" # for #[pgqrs_workflow] macro
    tokio = { version = "1", features = ["full"] }
    ```

=== "Python API"

    Install the `pgqrs` library using your Python package manager:

    ```bash
    # Using pip
    pip install pgqrs

    # Using uv (recommended)
    uv add pgqrs
    ```

---

## Database Schema

The coordinator automatically handles schema bootstrapping and migrations:
* The **CLI/Docker Daemon** checks and runs database migrations on startup.
* The **PostgreSQL Extension** provisions all required tables upon executing `CREATE EXTENSION`.
