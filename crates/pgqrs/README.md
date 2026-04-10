# pgqrs

**pgqrs is a postgres-native, library-only durable execution engine.**

Written in Rust. Built for Postgres. Also supports SQLite and Turso.

## What is Durable Execution?

A durable execution engine ensures workflows resume from application crashes or pauses. 
Each step executes exactly once. State persists in the database. Processes resume from the last completed step.

## Key Properties

- **Postgres-native:** Leverages SKIP LOCKED, ACID transactions
- **Library-only:** Runs in-process with your application
- **Multi-backend:** Postgres (production), SQLite/Turso (testing and embedded)
- **Type-safe:** Rust core with idiomatic Python bindings
- **Transaction-safe:** Exactly-once step execution within database transactions

## Example

### Producer

```rust
use pgqrs::Producer;
use serde_json::Value;

/// Enqueue a payload to the queue
async fn enqueue_job(producer: &Producer, payload: Value) -> Result<i64, Box<dyn std::error::Error>> {
	let message = producer.enqueue(&payload).await?;
	Ok(message.id)
}
```

### Consumer

```rust
use pgqrs::{Consumer, WorkerInfo};
use std::time::Duration;

/// Poll for jobs from the queue and print them as they arrive
async fn poll_and_print_jobs(consumer: &Consumer, worker: &WorkerInfo) -> Result<(), Box<dyn std::error::Error>> {
	loop {
		let messages = consumer.dequeue(worker).await?;
		if messages.is_empty() {
			// No job found, wait before polling again
			tokio::time::sleep(Duration::from_secs(2)).await;
		} else {
			for message in messages {
				println!("Dequeued job: {}", message.payload);
				// Optionally archive or delete the message after processing
				consumer.archive(message.id).await?;
			}
		}
	}
}
```

## Quickstart

### Add the library

```toml
[dependencies]
pgqrs = "0.15.2"
```

### Start a Postgres DB or get the DSN of an existing db.

You'll need a PostgreSQL database to use pgqrs. Here are your options:

#### Option 1: Using Docker (Recommended for development)
```bash
# Start a PostgreSQL container
docker run --name pgqrs-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15

# Your DSN will be:
# postgresql://postgres:postgres@localhost:5432/postgres
```

#### Option 2: Using an existing PostgreSQL database
Get your database connection string (DSN) in this format:
```
postgresql://username:password@hostname:port/database
```

#### Option 3: Using a cloud PostgreSQL service
- **AWS RDS**: Get the connection string from the RDS console
- **Google Cloud SQL**: Get the connection string from the Cloud Console
- **Azure Database**: Get the connection string from the Azure portal
- **Heroku Postgres**: Use the `DATABASE_URL` from your Heroku config

### Configure pgqrs

pgqrs can load configuration from environment variables or a `pgqrs.yaml` file.

Create a `pgqrs.yaml` file:
```yaml
dsn: "postgresql://postgres:postgres@localhost:5432/postgres"
```

### Install the pgqrs schema

pgqrs requires a few tables to store metadata. It creates these tables as well as
queue tables in the specified schema.

**Important**: You must create the schema before calling `admin.install()`.

### Step 1: Create the schema

Connect to your PostgreSQL database and create the schema:

```sql
-- For default 'public' schema (no action needed)
-- For custom schema:
CREATE SCHEMA IF NOT EXISTS pgqrs;
```

#### Step 2: Install pgqrs

Once you have your database configured and schema created, install and verify the pgqrs schema:

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let store = pgqrs::connect("postgresql://postgres:postgres@localhost:5432/postgres").await?;
pgqrs::admin(&store).install().await?;
pgqrs::admin(&store).verify().await?;
# Ok(()) }
```

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
