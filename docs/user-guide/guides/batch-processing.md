# Batch Processing Guide

This guide covers efficient batch processing patterns for high-throughput scenarios.

## When to Use Batch Processing

- Processing thousands of messages per minute
- Reducing database round-trips
- Improving throughput with bulk operations
- ETL and data pipeline workloads

## Batch Enqueueing

Send multiple messages efficiently.

=== "Rust"

    ```rust
    use pgqrs;
    use serde_json::json;

    async fn batch_enqueue_example() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Create managed producer
        let producer = pgqrs::producer()
            .queue("tasks")
            .hostname("batch-producer")
            .create(&store)
            .await?;

        // Prepare batch of messages
        let payloads: Vec<_> = (0..1000)
            .map(|i| json!({"task_id": i, "data": "process me"}))
            .collect();

        // Send batch
        for payload in &payloads {
            producer.enqueue(payload).await?;
        }

        println!("Enqueued {} messages", payloads.len());

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def batch_enqueue_example():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Create managed producer
        producer = await store.producer("tasks")

        # Prepare batch
        payloads = [{"task_id": i, "data": "process me"} for i in range(1000)]

        # Send using producer
        msg_ids = await producer.enqueue_batch(payloads)
        print(f"Enqueued {len(msg_ids)} messages")

    asyncio.run(batch_enqueue_example())
    ```

## Batch Dequeueing

Fetch and process multiple messages in one operation.

=== "Rust"

    ```rust
    use pgqrs;

    async fn batch_dequeue_example() -> Result<(), Box<dyn std::error::Error>> {
        let store = pgqrs::connect("postgresql://localhost/mydb").await?;

        // Create managed consumer
        let consumer = pgqrs::consumer()
            .queue("tasks")
            .hostname("batch-consumer")
            .create(&store)
            .await?;

        // Fetch and process batch with handler
        consumer.dequeue()
            .batch(100)
            .handle_batch(|messages| async move {
                println!("Processing {} messages", messages.len());

                for message in &messages {
                    // Process each message
                    println!("Processing: {}", message.id);
                }

                Ok(())
            })
            .await?;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import pgqrs

    async def batch_dequeue_example():
        store = await pgqrs.connect("postgresql://localhost/mydb")

        # Create managed consumer
        consumer = await store.consumer("tasks")

        # Process batch with handler
        async def process_batch(messages):
            print(f"Processing {len(messages)} messages")
            for msg in messages:
                print(f"Processing: {msg.id}")
            return True

        await consumer.consume_batch(batch_size=100, handler=process_batch)

    asyncio.run(batch_dequeue_example())
    ```

## Processing Patterns

### Sequential Batch Processing

Process batches one after another with controlled throughput.

```rust
use pgqrs;
use std::time::Duration;

async fn sequential_batch_processing(consumer: &dyn pgqrs::store::Consumer) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let result = consumer.dequeue()
            .batch(100)
            .handle_batch(|messages| async move {
                if messages.is_empty() {
                    return Err("No messages".into());
                }

                // Process sequentially
                for message in &messages {
                    match process_message(message).await {
                        Ok(_) => println!("Processed {}", message.id),
                        Err(e) => {
                            tracing::warn!("Failed to process {}: {}", message.id, e);
                        }
                    }
                }
                Ok(())
            })
            .await;

        if result.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn process_message(msg: &pgqrs::QueueMessage) -> Result<(), Box<dyn std::error::Error>> {
    // Your processing logic
    Ok(())
}
```

### Parallel Batch Processing

Process all messages in a batch concurrently.

```rust
use pgqrs;
use futures::future::join_all;

async fn parallel_batch_processing(consumer: &dyn pgqrs::store::Consumer) -> Result<(), Box<dyn std::error::Error>> {
    consumer.dequeue()
        .batch(100)
        .handle_batch(|messages| async move {
            // Process all in parallel
            let futures: Vec<_> = messages.iter()
                .map(|m| async move {
                    process_message(m).await
                })
                .collect();

            let results = join_all(futures).await;

            // Log results
            let successful = results.iter().filter(|r| r.is_ok()).count();
            let failed = results.iter().filter(|r| r.is_err()).count();

            println!("Processed: {} successful, {} failed", successful, failed);

            Ok(())
        })
        .await?;

    Ok(())
}
```

### Chunked Processing

Process large batches in smaller chunks.

```rust
use pgqrs;

async fn chunked_processing(consumer: &dyn pgqrs::store::Consumer, chunk_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    consumer.dequeue()
        .batch(1000)
        .handle_batch(|messages| async move {
            // Process in chunks
            for chunk in messages.chunks(chunk_size) {
                for message in chunk {
                    process_message(message).await?;
                }
                tracing::info!("Processed chunk of {} messages", chunk.len());
            }
            Ok(())
        })
        .await?;

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

### Connection Pool Sizing

For batch processing, ensure adequate connection pool size:

```rust
use pgqrs::Config;

let config = Config {
    dsn: "postgresql://localhost/mydb".into(),
    max_connections: 20,  // Increase for parallel batch processing
    ..Default::default()
};

let store = pgqrs::connect_with_config(&config).await?;
```

## Batch Processing with Multiple Workers

Scale horizontally with multiple consumers using managed workers.

```rust
use pgqrs;
use tokio::task::JoinSet;

async fn run_batch_workers(num_workers: usize) -> Result<(), Box<dyn std::error::Error>> {
    let store = pgqrs::connect("postgresql://localhost/mydb").await?;

    let mut workers = JoinSet::new();

    for i in 0..num_workers {
        let store_clone = store.clone();

        workers.spawn(async move {
            // Create managed consumer for this worker
            // Note: In a real app, hostname/port should be unique/discoverable
            let consumer = pgqrs::consumer()
                .queue("tasks")
                .hostname(&format!("worker-{}", i))
                .port(3000 + i as i32)
                .create(&store_clone)
                .await?;

            batch_consumer_loop(&store_clone, &*consumer).await
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

async fn batch_consumer_loop(
    store: &pgqrs::store::AnyStore,
    consumer: &dyn pgqrs::store::Consumer
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let result = pgqrs::dequeue()
            .from("tasks")
            .batch(100)
            .worker(consumer)
            .handle_batch(|messages| async move {
                if messages.is_empty() {
                    return Err("No messages".into());
                }

                for message in &messages {
                    process_message(message).await?;
                }
                Ok(())
            })
            .execute(store)
            .await;

        if result.is_err() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}
```

## Monitoring Batch Processing

Track throughput and processing times:

```rust
use std::time::Instant;
use pgqrs;

async fn monitored_batch_processing(store: &pgqrs::store::AnyStore) -> Result<(), Box<dyn std::error::Error>> {
    let mut total_processed: u64 = 0;
    let start = Instant::now();

    loop {
        let batch_start = Instant::now();

        let result = pgqrs::dequeue()
            .from("tasks")
            .batch(100)
            .handle_batch(|messages| async move {
                let batch_size = messages.len();

                for message in &messages {
                    process_message(message).await?;
                }

                Ok(batch_size)
            })
            .execute(store)
            .await;

        match result {
            Ok(batch_size) => {
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
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}
```

## Best Practices

1. **Match batch size to processing time** - Larger batches for quick tasks
2. **Use appropriate visibility timeouts** - Timeout should cover entire batch processing
3. **Handle partial failures** - Archive successful, let failed retry
4. **Monitor throughput** - Track messages per second
5. **Scale with workers** - Add consumers for more throughput
6. **Use managed workers** - Let pgqrs handle worker lifecycle

## What's Next?

- [Delayed Messages](delayed-messages.md) - Schedule future tasks
- [Worker Management](worker-management.md) - Scale your workers
