## Install the binary

```bash
cargo install pgqrs
```

## Start a Postgres DB or get the DSN of an existing db.

You'll need a PostgreSQL database to use pgqrs. Here are your options:

### Option 1: Using Docker (Recommended for development)
```bash
# Start a PostgreSQL container
docker run --name pgqrs-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15

# Your DSN will be:
# postgresql://postgres:postgres@localhost:5432/postgres
```

### Option 2: Using an existing PostgreSQL database
Get your database connection string (DSN) in this format:
```
postgresql://username:password@hostname:port/database
```

### Option 3: Using a cloud PostgreSQL service
- **AWS RDS**: Get the connection string from the RDS console
- **Google Cloud SQL**: Get the connection string from the Cloud Console
- **Azure Database**: Get the connection string from the Azure portal
- **Heroku Postgres**: Use the `DATABASE_URL` from your Heroku config

## Configure pgqrs

Set your database connection using one of these methods (in order of priority):

```bash
# Method 1: Command line argument (highest priority)
pgqrs --dsn "postgresql://postgres:postgres@localhost:5432/postgres"

# Method 2: Environment variable
export PGQRS_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
pgqrs ...
```

Create a `pgqrs.yaml` file:
```yaml
dsn: "postgresql://postgres:postgres@localhost:5432/postgres"
```

Then run:
```bash
# Method 3: Use a yaml config file.
pgqrs ...
```

## Install the pgqrs schema

pgqrs requires a few tables to store metadata. It creates these tables as well as
queue tables in the specified schema.

**Important**: You must create the schema before running `pgqrs install`.

### Step 1: Create the schema

Connect to your PostgreSQL database and create the schema:

```sql
-- For default 'public' schema (no action needed)
-- For custom schema:
CREATE SCHEMA IF NOT EXISTS pgqrs;
```

### Step 2: Install pgqrs

Once you have your database configured and schema created, install the pgqrs schema:

```bash
# Install in default 'public' schema
pgqrs install

# Install in custom schema
pgqrs --schema pgqrs install

# Verify the installation
pgqrs verify
# Or verify custom schema
pgqrs --schema pgqrs verify
```

## Test queue commands from the CLI

Items can be enqueued or dequeued using the CLI. This option is only available for testing
or experiments.

```bash
# Create a test queue
pgqrs queue create test_queue

# Send a message to the queue
pgqrs message send test_queue '{"message": "Hello, World!", "timestamp": "2023-01-01T00:00:00Z"}'

# Send a delayed message (available after 30 seconds)
pgqrs message send test_queue '{"task": "delayed_task"}' --delay 30

# Read and immediately consume one message
pgqrs message dequeue test_queue

# Delete a specific message by ID
pgqrs message delete test_queue 12345
```
