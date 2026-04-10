# S3 Queue Guide

This guide covers the queue-on-S3 backend introduced in `0.15.0`.

pgqrs stores queue state as a SQLite database file addressed by an `s3://bucket/key.sqlite` DSN. The Rust API exposes explicit `bootstrap()`, `snapshot()`, `sync()`, and `state()` operations on top of the normal queue APIs.

## When To Use S3

Use the S3 backend when you want:

- remote durable queue state without running PostgreSQL
- a portable queue stored as a single object
- explicit sync boundaries in Rust

Prefer PostgreSQL when you need many concurrent writers or high sustained write throughput.

## Prerequisites

- `pgqrs` built with the `s3` feature
- an S3 bucket or S3-compatible endpoint
- AWS-compatible environment variables configured

If you are using Python, the installed build also needs S3 support compiled in.

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Optional for LocalStack or other S3-compatible endpoints
export AWS_ENDPOINT_URL=http://localhost:4566

# Optional; default is durable
export PGQRS_S3_MODE=durable
```

## Step 1: Create An S3 Config

=== "Rust"

    ```rust
    use pgqrs::Config;
    use pgqrs::store::s3::DurabilityMode;

    let mut config = Config::from_dsn("s3://my-bucket/queue.sqlite");
    config.s3.mode = DurabilityMode::Durable;
    ```

=== "Python"

    ```python
    import pgqrs

    config = pgqrs.Config("s3://my-bucket/queue.sqlite")
    ```

## Step 2: Bootstrap Remote State

Before the first write, bootstrap the remote object.

=== "Rust"

    ```rust
    let store = pgqrs::connect_with_config(&config).await?;
    pgqrs::admin(&store).install().await?;
    ```

=== "Python"

    ```python
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    ```

`install()` creates the remote SQLite object if it is missing. If the object already exists, bootstrap returns a conflict instead of overwriting it.

## Step 3: Use The Queue Normally

Once bootstrapped, queue operations look the same as other backends.

=== "Rust"

    ```rust
    use serde_json::json;

    let producer = pgqrs::producer("host", 9001, "jobs").create(&store).await?;
    let consumer = pgqrs::consumer("host", 9002, "jobs").create(&store).await?;

    pgqrs::admin(&store).create_queue("jobs").await?;

    pgqrs::enqueue()
        .message(&json!({ "job": "send_email" }))
        .worker(&producer)
        .execute(&store)
        .await?;

    let messages = pgqrs::dequeue()
        .worker(&consumer)
        .fetch_all(&store)
        .await?;
    ```

=== "Python"

    ```python
    producer = await store.producer("jobs")
    consumer = await store.consumer("jobs")

    await store.queue("jobs")
    await producer.enqueue({"job": "send_email"})
    messages = await consumer.dequeue(batch_size=1)
    ```

## Durability Modes

### Durable Mode

`Durable` mode is the default.

- ordinary writes are synchronized to S3 before returning
- this is the simplest mode for applications that just want an S3-backed queue

### Local Mode

`Local` mode is Rust-only today and gives the application explicit replication control.

```rust
use pgqrs::store::s3::DurabilityMode;

let mut config = pgqrs::Config::from_dsn("s3://my-bucket/queue.sqlite");
config.s3.mode = DurabilityMode::Local;
let mut store = pgqrs::connect_with_config(&config).await?;

pgqrs::admin(&store).install().await?;
pgqrs::admin(&store).create_queue("jobs").await?;
store.sync().await?;
```

Use `Local` mode when you want to stage multiple local writes and publish them together with `sync()`, or when a follower process should explicitly refresh with `snapshot()`.

## Refreshing From Remote State

Rust applications can inspect and refresh S3 state directly:

```rust
use pgqrs::store::AnyStore;
use pgqrs::store::s3::SyncState;

let mut store = pgqrs::connect_with_config(&config).await?;

if let AnyStore::S3(s3_store) = &mut store {
    match s3_store.state().await? {
        SyncState::InSync => {}
        _ => s3_store.snapshot().await?,
    }
}
```

`snapshot()` pulls remote state into the local cache. It returns a conflict if the local cache is dirty.

## Current Python Limitation

When the installed Python build includes S3 support, Python can connect to an S3-backed queue and bootstrap it. Explicit `snapshot()` / `sync()` lifecycle controls are not currently exposed in the Python bindings, so advanced replication control still belongs in Rust.
