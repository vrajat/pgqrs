# Batch Processing Guide

This guide covers efficient batch processing patterns for high-throughput scenarios.

## When to Use Batch Processing

- Processing thousands of messages per minute
- Reducing database round-trips
- Improving throughput with bulk operations
- ETL and data pipeline workloads

## Batch Enqueueing

Send multiple messages in a single transaction.

=== "Rust"

    ```rust
    use pgqrs::{Admin, Producer, Config};
    use serde_json::json;

    async fn batch_enqueue_example() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;
        let queue = admin.get_queue("tasks").await?;

        let producer = Producer::new(
            admin.pool.clone(), &queue, "producer", 3000, &config
        ).await?;

        // Prepare batch of messages
        let payloads: Vec<_> = (0..1000)
            .map(|i| json!({"task_id": i, "data": "process me"}))
            .collect();

        // Send all at once
        let messages = producer.batch_enqueue(&payloads).await?;

        println!("Enqueued {} messages in one transaction", messages.len());

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Producer

    async def batch_enqueue_example():
        producer = Producer(
            "postgresql://localhost/mydb",
            "tasks",
            "producer",
            3000,
        )

        # Note: Python API currently sends one at a time
        # For batching, use a loop with concurrent sends
        tasks = []
        for i in range(1000):
            task = producer.enqueue({"task_id": i, "data": "process me"})
            tasks.append(task)

        # Send concurrently (with semaphore for connection limits)
        results = await asyncio.gather(*tasks[:100])  # Batch of 100
        print(f"Enqueued {len(results)} messages")

    asyncio.run(batch_enqueue_example())
    ```

## Batch Dequeueing

Fetch multiple messages in one operation.

=== "Rust"

    ```rust
    use pgqrs::{Admin, Consumer, Config};

    async fn batch_dequeue_example() -> Result<(), Box<dyn std::error::Error>> {
        let config = Config::from_dsn("postgresql://localhost/mydb");
        let admin = Admin::new(&config).await?;
        let queue = admin.get_queue("tasks").await?;

        let consumer = Consumer::new(
            admin.pool.clone(), &queue, "consumer", 3001, &config
        ).await?;

        // Fetch up to 100 messages with 30-second lock
        let messages = consumer.dequeue_many_with_delay(100, 30).await?;

        println!("Fetched {} messages", messages.len());

        for message in &messages {
            // Process each message
            println!("Processing: {}", message.id);
        }

        // Batch archive
        let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
        let results = consumer.archive_many(ids).await?;

        let archived = results.iter().filter(|&&r| r).count();
        println!("Archived {} messages", archived);

        Ok(())
    }
    ```

## Processing Patterns

### Sequential Batch Processing

Process batches one after another with controlled throughput.

```rust
async fn sequential_batch_processing(consumer: &Consumer) -> Result<(), Error> {
    loop {
        // Fetch batch
        let messages = consumer.dequeue_many_with_delay(100, 60).await?;

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Process sequentially
        let mut successful_ids = Vec::new();

        for message in &messages {
            match process_message(message).await {
                Ok(_) => successful_ids.push(message.id),
                Err(e) => {
                    tracing::warn!("Failed to process {}: {}", message.id, e);
                    // Message will become available again after timeout
                }
            }
        }

        // Batch archive successful ones
        if !successful_ids.is_empty() {
            consumer.archive_many(successful_ids).await?;
        }
    }
}
```

### Parallel Batch Processing

Process all messages in a batch concurrently.

```rust
use futures::future::join_all;

async fn parallel_batch_processing(consumer: &Consumer) -> Result<(), Error> {
    let messages = consumer.dequeue_many_with_delay(100, 60).await?;

    // Process all in parallel
    let futures: Vec<_> = messages.iter().map(|m| async {
        let result = process_message(m).await;
        (m.id, result)
    }).collect();

    let results = join_all(futures).await;

    // Separate successes and failures
    let successful: Vec<i64> = results
        .iter()
        .filter(|(_, r)| r.is_ok())
        .map(|(id, _)| *id)
        .collect();

    let failed: Vec<i64> = results
        .iter()
        .filter(|(_, r)| r.is_err())
        .map(|(id, _)| *id)
        .collect();

    // Batch archive successful
    consumer.archive_many(successful).await?;

    // Log failures (will retry after timeout)
    for id in failed {
        tracing::warn!("Message {} failed, will retry", id);
    }

    Ok(())
}
```

### Chunked Processing

Process large batches in smaller chunks.

```rust
async fn chunked_processing(consumer: &Consumer, chunk_size: usize) -> Result<(), Error> {
    // Fetch large batch
    let messages = consumer.dequeue_many_with_delay(1000, 300).await?;

    // Process in chunks
    for chunk in messages.chunks(chunk_size) {
        let futures: Vec<_> = chunk.iter().map(process_message).collect();
        let results = join_all(futures).await;

        // Archive this chunk
        let ids: Vec<i64> = chunk.iter()
            .zip(results.iter())
            .filter(|(_, r)| r.is_ok())
            .map(|(m, _)| m.id)
            .collect();

        consumer.archive_many(ids).await?;

        // Progress logging
        tracing::info!("Processed chunk of {} messages", chunk.len());
    }

    Ok(())
}
```

## Throughput Optimization

### Tuning Batch Size

| Scenario | Recommended Batch Size |
|----------|------------------------|
| Quick tasks (< 10ms) | 100-500 |
| Medium tasks (10-100ms) | 50-100 |
| Slow tasks (> 100ms) | 10-50 |
| I/O bound tasks | 100-200 (with parallel) |

```rust
// Adaptive batch sizing
async fn adaptive_batch_consumer(consumer: &Consumer) -> Result<(), Error> {
    let mut batch_size = 50;
    let target_batch_time = Duration::from_secs(5);

    loop {
        let start = Instant::now();

        let messages = consumer.dequeue_many_with_delay(batch_size, 60).await?;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // Process batch
        for message in &messages {
            process_message(message).await?;
        }
        consumer.archive_many(messages.iter().map(|m| m.id).collect()).await?;

        let elapsed = start.elapsed();

        // Adjust batch size
        if elapsed < target_batch_time / 2 && batch_size < 500 {
            batch_size = (batch_size * 3 / 2).min(500);
        } else if elapsed > target_batch_time * 2 && batch_size > 10 {
            batch_size = (batch_size * 2 / 3).max(10);
        }

        tracing::debug!("Batch size: {}, time: {:?}", batch_size, elapsed);
    }
}
```

### Connection Pool Sizing

For batch processing, ensure adequate connection pool size:

```rust
// In your config
let config = Config {
    dsn: "postgresql://localhost/mydb".into(),
    max_connections: 20,  // Increase for parallel batch processing
    ..Default::default()
};
```

## Batch Processing with Multiple Workers

Scale horizontally with multiple consumers.

```rust
use tokio::task::JoinSet;

async fn run_batch_workers(num_workers: usize) -> Result<(), Error> {
    let config = Config::from_dsn("postgresql://localhost/mydb");
    let admin = Admin::new(&config).await?;
    let queue = admin.get_queue("tasks").await?;

    let mut workers = JoinSet::new();

    for i in 0..num_workers {
        let pool = admin.pool.clone();
        let queue = queue.clone();
        let config = config.clone();

        workers.spawn(async move {
            let consumer = Consumer::new(
                pool,
                &queue,
                &format!("worker-{}", i),
                3000 + i as i32,
                &config,
            ).await?;

            batch_consumer_loop(&consumer).await
        });
    }

    // Wait for all workers
    while let Some(result) = workers.join_next().await {
        if let Err(e) = result {
            tracing::error!("Worker error: {:?}", e);
        }
    }

    Ok(())
}

async fn batch_consumer_loop(consumer: &Consumer) -> Result<(), Error> {
    loop {
        let messages = consumer.dequeue_many_with_delay(100, 60).await?;

        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // Process and archive
        for message in &messages {
            process_message(message).await?;
        }

        let ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
        consumer.archive_many(ids).await?;
    }
}
```

## Monitoring Batch Processing

Track throughput and processing times:

```rust
use std::time::Instant;

async fn monitored_batch_processing(consumer: &Consumer) -> Result<(), Error> {
    let mut total_processed: u64 = 0;
    let start = Instant::now();

    loop {
        let batch_start = Instant::now();

        let messages = consumer.dequeue_many_with_delay(100, 60).await?;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let batch_size = messages.len();

        // Process batch
        for message in &messages {
            process_message(message).await?;
        }
        consumer.archive_many(messages.iter().map(|m| m.id).collect()).await?;

        // Update metrics
        total_processed += batch_size as u64;
        let batch_time = batch_start.elapsed();
        let total_time = start.elapsed();

        let throughput = total_processed as f64 / total_time.as_secs_f64();

        tracing::info!(
            batch_size = batch_size,
            batch_time_ms = batch_time.as_millis(),
            total_processed = total_processed,
            throughput_per_sec = throughput,
            "Batch completed"
        );
    }
}
```

## Best Practices

1. **Match batch size to processing time** - Larger batches for quick tasks
2. **Use appropriate lock times** - Lock time should cover entire batch
3. **Handle partial failures** - Archive successful, let failed retry
4. **Monitor throughput** - Track messages per second
5. **Scale with workers** - Add consumers for more throughput

## What's Next?

- [Delayed Messages](delayed-messages.md) - Schedule future tasks
- [Worker Management](worker-management.md) - Scale your workers
