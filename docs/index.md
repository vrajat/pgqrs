# pgqrs

`pgqrs` is a library-only PostgreSQL-backed job queue for Rust applications.

## Features
- **Lightweight**: No servers to operate. Directly use `pgqrs` as a library in your Rust applications.
- **Compatible with Connection Poolers**: Use with [pgBouncer](https://www.pgbouncer.org) or [pgcat](https://github.com/postgresml/pgcat) to scale connections.
- **Efficient**: [Uses PostgreSQL's `SKIP LOCKED` for concurrent job fetching](https://vrajat.com/posts/postgres-queue-skip-locked-unlogged/).
- **Exactly Once Delivery**: Guarantees exactly-once delivery within a time range specified by time limit.
- **Message Archiving**: Built-in archiving system for audit trails and historical data retention.

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