# Backend Selection Guide

pgqrs supports multiple storage backends, giving you flexibility to choose the right database for your use case.

## Supported Backends

| Backend | DSN Format | Cargo Feature |
|---------|------------|---------------|
| PostgreSQL | `postgresql://host/db` | `postgres` (default) |
| SQLite | `sqlite:///path/to/file.db` | `sqlite` |
| Turso | `turso:///path/to/file.db` | `turso` |

## Decision Matrix

| Scenario | Recommended Backend | Why |
|----------|---------------------|-----|
| Production with multiple workers | **PostgreSQL** | Full concurrency, no writer conflicts |
| CLI tools & scripts | **SQLite / Turso** | Zero-config, embedded, portable |
| Testing & prototyping | **SQLite / Turso** | Fast setup, no external dependencies |
| Embedded applications | **SQLite / Turso** | Single-file database, no server required |
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

- PostgreSQL scales with consumer count
- PostgreSQL also benefits strongly from larger dequeue batch sizes
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

- CLI tools and single-process scripts
- Testing and prototyping
- Embedded applications
- Development environments
- Desktop applications

### Benchmark-backed Behavior

In the current `queue.drain_fixed_backlog` benchmark:

- SQLite benefits strongly from larger dequeue batch sizes
- SQLite does not scale with more consumers in this workload
- latency rises sharply as concurrency increases

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

- CLI tools and single-process scripts
- Testing and prototyping
- Embedded applications
- Development environments
- Desktop applications

### Benchmark-backed Behavior

Turso benchmarks are still being stabilized for the current queue scenarios.

Treat Turso benchmark guidance as a work in progress until dedicated scenario writeups are published.

### DSN Examples

```
turso:///path/to/database.db
turso:///var/lib/myapp/queue.db
```

## Cargo Feature Configuration

### Rust

```toml
[dependencies]
# PostgreSQL only (default)
pgqrs = "0.14.0"

# SQLite only
pgqrs = { version = "0.14.0", default-features = false, features = ["sqlite"] }

# Turso only
pgqrs = { version = "0.14.0", default-features = false, features = ["turso"] }

# All backends
pgqrs = { version = "0.14.0", features = ["full"] }
```

### Python

The Python package includes both backends by default.

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

// Same code works with any backend!
process_jobs(&store).await?;
```
