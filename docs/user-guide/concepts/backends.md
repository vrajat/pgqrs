# Backend Selection Guide

pgqrs supports multiple storage backends, giving you flexibility to choose the right database for your use case.

## Supported Backends

| Backend | DSN Format | Cargo Feature |
|---------|------------|---------------|
| PostgreSQL | `postgresql://host/db` | `postgres` (default) |
| SQLite | `sqlite:///path/to/file.db` | `sqlite` |
| Turso | `turso:///path/to/file.db` | `turso` |
| S3 | `s3://bucket/key.sqlite` | `s3` |

## Decision Matrix

| Scenario | Recommended Backend | Why |
|----------|---------------------|-----|
| Production with multiple workers | **PostgreSQL** | Full concurrency, no writer conflicts |
| Testing & prototyping | **SQLite / Turso** | Fast setup, no external dependencies |
| Embedded applications | **SQLite / Turso** | Single-file database, no server required |
| Portable remote queue state in object storage | **S3** | SQLite state replicated through a single S3 object |
| High write throughput | **PostgreSQL** | SQLite/Turso allow only 1 writer at a time |
| Distributed systems | **PostgreSQL** | Multiple processes can connect simultaneously |

## PostgreSQL

PostgreSQL is the production-ready choice for most deployments.

### Advantages

- **Unlimited concurrent workers**: Multiple producers and consumers can operate simultaneously
- **Connection pooling**: Works with pgBouncer and pgcat for connection scaling
- **SKIP LOCKED**: Efficient concurrent job fetching without conflicts
- **Proven at scale**: Handles millions of messages and large teams

### When to Use

- Production workloads
- Multiple worker processes
- Distributed systems
- High write throughput requirements

### Benchmark-backed Behavior

In the current `queue.drain_fixed_backlog` benchmark:

- PostgreSQL is the best-performing backend in the current drain benchmark
- PostgreSQL starts around `150 msg/s` with `1` consumer and scales close to linearly as more consumers are added
- latency remains comparatively flat as concurrency rises

See the curated benchmark writeup for the exact scenario and charts:

- [Queue Drain Fixed Backlog](../../benchmarks/queue-drain-fixed-backlog.md)

### DSN Examples

```
postgresql://localhost/mydb
postgresql://user:password@host:5432/database
postgres://localhost/mydb?sslmode=require
```

## SQLite

SQLite is a zero-config embedded option for simpler deployments.

### Advantages

- **No server required**: Database is a single file
- **Zero configuration**: No setup, no maintenance
- **Portable**: Database file can be copied/moved easily
- **Fast for reads**: Excellent read performance

### Limitations

!!! warning "Concurrency Constraints"
    SQLite uses database-level locks. **Only one writer can operate at a time.**

    With many concurrent writing processes, you may experience:

    - Lock contention errors (`SQLITE_BUSY`)
    - Starvation where some processes wait indefinitely
    - Reduced throughput as writers queue up

    See [SkyPilot's detailed analysis](https://blog.skypilot.co/abusing-sqlite-to-handle-concurrency/) of SQLite concurrency issues.

### pgqrs Mitigations

pgqrs automatically configures SQLite for the best possible concurrency:

- **WAL mode**: Enables concurrent reads during writes
- **5000ms busy timeout**: Retries locks instead of failing immediately
- **Foreign key enforcement**: Data integrity maintained

### When to Use

- Single-process scripts
- Testing and prototyping
- Embedded applications
- Development environments
- Desktop applications

### Benchmark-backed Behavior

In the current `queue.drain_fixed_backlog` benchmark:

- SQLite has solid single-consumer throughput in this workload
- SQLite benefits strongly from larger dequeue batch sizes
- SQLite does not scale with more consumers, and latency rises as concurrency increases

That is consistent with SQLite's single-writer concurrency model.

See the curated benchmark writeup for the exact scenario and charts:

- [Queue Drain Fixed Backlog](../../benchmarks/queue-drain-fixed-backlog.md)

### DSN Examples

```
sqlite:///path/to/database.sqlite
sqlite:///var/lib/myapp/queue.db
sqlite::memory:  # In-memory database (testing only)
```

## Turso

Turso provides SQLite-compatible local storage with enhanced features.

### Advantages

- **SQLite-compatible**: Drop-in replacement for SQLite with enhancements
- **No server required**: Database is a single file, just like SQLite
- **Zero configuration**: No setup, no maintenance
- **Portable**: Database file can be copied/moved easily
- **Fast for reads**: Excellent read performance

### Limitations

!!! warning "Concurrency Constraints"
    Turso uses database-level locks (same as SQLite). **Only one writer can operate at a time.**

    With many concurrent writing processes, you may experience:

    - Lock contention errors (`SQLITE_BUSY`)
    - Starvation where some processes wait indefinitely
    - Reduced throughput as writers queue up

    See [SkyPilot's detailed analysis](https://blog.skypilot.co/abusing-sqlite-to-handle-concurrency/) of SQLite concurrency issues.

### pgqrs Mitigations

pgqrs automatically configures Turso for the best possible concurrency:

- **WAL mode**: Enables concurrent reads during writes
- **5000ms busy timeout**: Retries locks instead of failing immediately
- **Foreign key enforcement**: Data integrity maintained

### When to Use

- Single-process scripts
- Testing and prototyping
- Embedded applications
- Development environments
- Desktop applications

### Benchmark-backed Behavior

The current Turso guidance is directional, but already useful.

In the current local-path `queue.drain_fixed_backlog` run:

- `1 consumer, batch_size = 1` reaches `173.0 msg/s`
- `1 consumer, batch_size = 50` reaches `6452.5 msg/s`
- `4 consumers, batch_size = 50` reaches `6804.3 msg/s`
- `p95 dequeue latency` rises from `4.02 ms` to `14.97 ms` between those low- and high-contention points

The important takeaway is not the exact number, but the shape:

- Turso currently behaves much more like SQLite than PostgreSQL
- larger dequeue batches help a lot
- more consumers add some latency, but do not unlock PostgreSQL-style scaling

In this repo, `turso:///...` uses local-path storage rather than a remote Turso edge deployment, so this guidance applies to the current local backend semantics.

See the benchmark writeup for the exact scenario and caveats:

- [Queue Drain Fixed Backlog](../../benchmarks/queue-drain-fixed-backlog.md)

### DSN Examples

```
turso:///path/to/database.db
turso:///var/lib/myapp/queue.db
```

## S3

S3 stores the queue as a SQLite database file backed by object storage.

### Advantages

- **Remote durable state**: Queue data lives in an S3 object instead of a local disk file
- **Portable deployment**: No PostgreSQL server is required for simple queue workloads
- **Explicit replication model**: Rust applications can choose automatic durable syncs or explicit sync boundaries
- **SQLite-compatible internals**: The backend keeps the queue API surface aligned with the other stores

### Durability Modes

pgqrs exposes two S3 durability modes:

- **`Durable`**: ordinary writes are synchronized to S3 before the call returns
- **`Local`**: writes stay in the local cache until the Rust application explicitly calls `sync()`

`Durable` is the default when `PGQRS_DSN` uses `s3://...`.

### When to Use

- Simple deployments that want remote queue durability without running PostgreSQL
- Operational workflows where queue state should live in object storage
- Rust applications that benefit from explicit `snapshot()` / `sync()` control

### Limitations

!!! warning "Not A Replacement For PostgreSQL Concurrency"
    The S3 backend is still built on a SQLite local cache. It is designed around explicit object synchronization, not PostgreSQL-style concurrent writers.

    Prefer PostgreSQL when you need high write throughput or many workers writing concurrently.

### Benchmark-backed Behavior

The current S3 guidance is directional, but it is already useful for backend selection.

In the current durable `queue.drain_fixed_backlog` baseline:

- `1 consumer, batch_size = 1` reaches `6.6 msg/s`
- `1 consumer, batch_size = 50` reaches `325.8 msg/s`
- `4 consumers, batch_size = 50` reaches `265.4 msg/s`
- `p95 dequeue latency` rises from `79.68 ms` to `304.12 ms` between those low- and high-contention points
- `p95 archive latency` rises from `80.01 ms` to `310.38 ms`

The important takeaway is the shape:

- object-store latency dominates this backend
- larger dequeue batches still help a lot
- more consumers do not improve throughput and can make it worse
- the large throughput drop versus PostgreSQL is broadly in line with the much higher per-message latency on the durable path

This baseline uses the current local benchmark harness rather than live AWS S3:

- `LocalStack + Toxiproxy`
- `durability_mode = durable`
- injected latency `60 ms`, jitter `0 ms`
- `prefill_jobs = 500`

Treat the numbers as "how S3Store behaves under an object-storage latency envelope" rather than as a cloud-provider guarantee.

See the benchmark writeup for the exact scenario and caveats:

- [Queue Drain Fixed Backlog](../../benchmarks/queue-drain-fixed-backlog.md)

### Operational Model

- `bootstrap()` creates the remote state if it does not exist and returns a conflict if it already exists
- `snapshot()` pulls remote state into the local cache
- `sync()` publishes dirty local state back to S3
- `state()` reports whether local and remote state are aligned

### Environment Requirements

The S3 backend reads its object store settings from AWS-compatible environment variables:

- `AWS_REGION`
- `AWS_ENDPOINT_URL` for LocalStack or other S3-compatible endpoints
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `PGQRS_S3_MODE` with `durable` or `local`

### DSN Examples

```
s3://my-bucket/queue.sqlite
s3://my-bucket/orders/prod.sqlite
```

## Cargo Feature Configuration

### Rust

```toml
[dependencies]
# PostgreSQL only (default)
pgqrs = "0.15.3"

# SQLite only
pgqrs = { version = "0.15.3", default-features = false, features = ["sqlite"] }

# Turso only
pgqrs = { version = "0.15.3", default-features = false, features = ["turso"] }

# S3 only
pgqrs = { version = "0.15.3", default-features = false, features = ["s3"] }

# All backends
pgqrs = { version = "0.15.3", features = ["full"] }
```

### Python

Python uses the backend support compiled into the installed wheel or local build.

## API Compatibility

All backends implement the same `Store` trait, so your application code remains unchanged:

```rust
// Works with all backends!
async fn process_jobs(store: &impl pgqrs::Store) -> Result<(), pgqrs::Error> {
    pgqrs::dequeue()
        .from("tasks")
        .handle(|msg| async move {
            println!("Processing: {:?}", msg.payload);
            Ok(())
        })
        .execute(store)
        .await
}
```

Switch backends by changing your DSN:

```rust
// PostgreSQL
let store = pgqrs::connect("postgresql://localhost/mydb").await?;

// SQLite
let store = pgqrs::connect("sqlite:///path/to/db.sqlite").await?;

// Turso
let store = pgqrs::connect("turso:///path/to/db.db").await?;

// S3
let store = pgqrs::connect("s3://my-bucket/queue.sqlite").await?;

// Same code works with any backend!
process_jobs(&store).await?;
```
