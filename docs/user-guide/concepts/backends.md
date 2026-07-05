# PostgreSQL Runtime Guide

pgqrs supports PostgreSQL only. This guide describes why the project standardizes on that runtime and what that means for deployment.

## Supported Runtime

| Runtime | DSN Format | Cargo Feature |
|---------|------------|---------------|
| PostgreSQL | `postgresql://host/db` | `postgres` (default) |

## Decision Matrix

| Scenario | Why PostgreSQL fits |
|----------|---------------------|
| Production with multiple workers | Full concurrency, transactional dequeue, no file-lock writer ceiling |
| Durable workflows | Queue state and workflow state stay in one database |
| Local development | Same runtime semantics as production |
| Distributed systems | Multiple processes can connect simultaneously |
| High write throughput | Better scaling under concurrent producers and consumers |

## PostgreSQL

PostgreSQL is the production-ready choice for all supported pgqrs deployments.

### Advantages

- **Concurrent workers**: Multiple producers and consumers can operate simultaneously
- **Connection pooling**: Works with PgBouncer and similar poolers
- **`SKIP LOCKED`**: Efficient concurrent job fetching without worker conflicts
- **Transactional durability**: Queue and workflow state share one ACID store

### When to Use

- Production workloads
- Local development
- CI and integration testing
- Distributed worker systems
- High write throughput requirements

### Benchmark-backed Behavior

In the current `queue.drain_fixed_backlog` benchmark:

- PostgreSQL is the best-performing supported runtime in the current drain benchmark
- throughput scales much better with more consumers than single-writer file-backed stores
- latency remains comparatively stable as concurrency rises

See the curated benchmark writeup for the exact scenario and charts:

- [Queue Drain Fixed Backlog](../../benchmarks/queue-drain-fixed-backlog.md)

### DSN Examples

```text
postgresql://localhost/mydb
postgresql://user:password@host:5432/database
postgres://localhost/mydb?sslmode=require
```

## Cargo Feature Configuration

### Rust

```toml
[dependencies]
pgqrs = "0.15.3"
```

### Python

Python bindings target the same PostgreSQL runtime.

## API Stability

Rust and Python share the same PostgreSQL-backed runtime model:

```rust
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

The DSN stays PostgreSQL-shaped across environments:

```rust
let store = pgqrs::connect("postgresql://localhost/mydb").await?;
process_jobs(&store).await?;
```
