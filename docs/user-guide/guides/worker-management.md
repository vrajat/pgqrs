# Worker Management Guide

This guide covers running and managing multiple workers in production environments.

## Understanding Workers

Every Producer and Consumer in pgqrs registers as a **worker**. Workers have:

- Unique ID
- Associated queue
- Status (Ready, Suspended, Stopped)
- Heartbeat timestamp
- Hostname and port for identification

## Running Multiple Workers

### Multiple Consumers per Queue

Run several consumers to increase throughput:

=== "Rust"

    ```rust
    use pgqrs::{Admin, Consumer, Config};
    use tokio::task::JoinSet;

    async fn run_workers(num_workers: usize) -> Result<(), Box<dyn std::error::Error>> {
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

                consumer_loop(&consumer, i).await
            });
        }

        println!("Started {} workers", num_workers);

        // Wait for all workers
        while let Some(result) = workers.join_next().await {
            if let Err(e) = result {
                eprintln!("Worker error: {:?}", e);
            }
        }

        Ok(())
    }

    async fn consumer_loop(consumer: &Consumer, id: usize) -> Result<(), pgqrs::Error> {
        loop {
            let messages = consumer.dequeue().await?;

            for message in messages {
                println!("[Worker {}] Processing message {}", id, message.id);
                // Process...
                consumer.archive(message.id).await?;
            }

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Consumer

    async def run_workers(num_workers: int):
        tasks = []

        for i in range(num_workers):
            consumer = Consumer(
                "postgresql://localhost/mydb",
                "tasks",
                f"worker-{i}",
                3000 + i,
            )
            task = asyncio.create_task(consumer_loop(consumer, i))
            tasks.append(task)

        print(f"Started {num_workers} workers")

        await asyncio.gather(*tasks)

    async def consumer_loop(consumer, worker_id: int):
        while True:
            messages = await consumer.dequeue()

            for message in messages:
                print(f"[Worker {worker_id}] Processing {message.id}")
                # Process...
                await consumer.archive(message.id)

            if not messages:
                await asyncio.sleep(1)

    asyncio.run(run_workers(4))
    ```

### Workers Across Multiple Processes

For production, run workers as separate processes:

```bash
# Start multiple worker processes
for i in {1..4}; do
    WORKER_ID=$i cargo run --release --bin worker &
done
```

```rust
// worker binary
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id: i32 = env::var("WORKER_ID")
        .unwrap_or_else(|_| "0".into())
        .parse()?;

    let hostname = hostname::get()?.to_string_lossy().to_string();

    let config = Config::from_env()?;
    let admin = Admin::new(&config).await?;
    let queue = admin.get_queue("tasks").await?;

    let consumer = Consumer::new(
        admin.pool.clone(),
        &queue,
        &hostname,
        3000 + worker_id,
        &config,
    ).await?;

    println!("Worker {} started on {}:{}", worker_id, hostname, 3000 + worker_id);

    run_consumer_loop(&consumer).await
}
```

## Worker Lifecycle

### Heartbeats

Send periodic heartbeats to indicate worker health:

=== "Rust"

    ```rust
    use pgqrs::Worker;
    use tokio::time::{interval, Duration};

    async fn worker_with_heartbeat(consumer: Consumer) -> Result<(), Error> {
        let mut heartbeat_interval = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    if let Err(e) = consumer.heartbeat().await {
                        tracing::warn!("Heartbeat failed: {}", e);
                    }
                }
                result = consumer.dequeue() => {
                    let messages = result?;
                    for message in messages {
                        process_message(&message).await?;
                        consumer.archive(message.id).await?;
                    }
                }
            }
        }
    }
    ```

=== "Python"

    ```python
    import pgqrs
    import asyncio

    admin = pgqrs.Admin("postgresql://localhost/mydb")

    # Batch producer
    producer = pgqrs.Producer(admin, "tasks", "batch-producer", 8080)

    # Enqueue multiple messages at once
    payloads = [
        {"task": "process_user", "user_id": i}
        for i in range(100, 200)
    ]

    messages = await producer.enqueue_batch(payloads)
    print(f"Enqueued {len(messages)} messages")

    # Batch consumer
    consumer = pgqrs.Consumer(admin, "tasks", "batch-consumer", 8081)

    # Process messages in batches
    batch = await consumer.dequeue_batch(limit=50)
    if batch:
        # Process all messages in the batch
        results = await asyncio.gather(*[
            process_message(msg.payload) for msg in batch
        ])

        # Archive the whole batch
        message_ids = [msg.id for msg in batch]
        await consumer.archive_batch(message_ids)

        print(f"Processed {len(batch)} messages")
    ```### Graceful Shutdown

Handle shutdown signals properly:

=== "Rust"

    ```rust
    use pgqrs::Worker;
    use tokio::signal;
    use tokio::sync::watch;

    async fn run_with_graceful_shutdown(consumer: Consumer) -> Result<(), Error> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        // Spawn shutdown handler
        tokio::spawn(async move {
            signal::ctrl_c().await.ok();
            println!("\nShutdown signal received");
            shutdown_tx.send(true).ok();
        });

        // Processing loop
        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            let messages = consumer.dequeue().await?;

            for message in messages {
                if *shutdown_rx.borrow() {
                    // Don't start new work during shutdown
                    break;
                }

                process_message(&message).await?;
                consumer.archive(message.id).await?;
            }

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Graceful shutdown
        println!("Suspending worker...");
        consumer.suspend().await?;
        consumer.shutdown().await?;
        println!("Worker shut down gracefully");

        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import signal
    from pgqrs import Consumer

    async def run_with_graceful_shutdown(consumer):
        stop = asyncio.Event()

        def handle_signal():
            print("\nShutdown signal received")
            stop.set()

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_signal)

        while not stop.is_set():
            messages = await consumer.dequeue()

            for message in messages:
                if stop.is_set():
                    break
                await process_message(message)
                await consumer.archive(message.id)

            if not messages:
                await asyncio.sleep(0.1)

        print("Consumer stopped gracefully")
        # Use signal handlers for graceful shutdown as shown above
    ```

## Monitoring Workers

### Via CLI

```bash
# List all workers
pgqrs worker list

# List workers for a queue
pgqrs worker list --queue tasks

# Check worker health
pgqrs worker health --queue tasks --max-age 300

# Get worker stats
pgqrs worker stats --queue tasks
```

### Via Code

=== "Rust"

    ```rust
    // List workers
    let workers = admin.workers.list().await?;
    for worker in workers {
        println!("Worker {}: {:?} on queue {}",
            worker.id, worker.status, worker.queue_id);
    }

    // Check health
    use pgqrs::Worker;
    let healthy = consumer.is_healthy(chrono::Duration::minutes(5)).await?;
    ```

=== "Python"

    ```python
    # Access workers table
    admin = Admin("postgresql://localhost/mydb")
    workers = await admin.get_workers()

    # Count workers
    count = await workers.count()
    print(f"Total workers: {count}")
    ```

    !!! note
        Detailed worker listing and health check methods not yet fully exposed in Python.

## Worker Health Monitoring

Build a health monitoring system:

=== "Rust"

    ```rust
    use std::collections::HashMap;

    struct WorkerMonitor {
        admin: Admin,
        max_heartbeat_age: chrono::Duration,
    }

    impl WorkerMonitor {
        async fn check_health(&self) -> Result<Vec<UnhealthyWorker>> {
            let workers = self.admin.workers.list().await?;
            let now = chrono::Utc::now();

            let unhealthy: Vec<_> = workers
                .into_iter()
                .filter(|w| {
                    w.status == WorkerStatus::Ready &&
                    (now - w.last_heartbeat) > self.max_heartbeat_age
                })
                .map(|w| UnhealthyWorker {
                    id: w.id,
                    last_seen: w.last_heartbeat,
                    queue_id: w.queue_id,
                })
                .collect();

            Ok(unhealthy)
        }

        async fn alert_unhealthy(&self) -> Result<()> {
            let unhealthy = self.check_health().await?;

            for worker in unhealthy {
                tracing::error!(
                    worker_id = worker.id,
                    last_seen = %worker.last_seen,
                    "Worker appears unhealthy"
                );

                // Send alert via your preferred method
                // send_alert(&worker).await?;
            }

            Ok(())
        }
    }
    ```

=== "Python"

    For Python, use the CLI for health monitoring until full API support is available:

    ```python
    import subprocess
    import json

    def check_worker_health(queue: str, max_age: int = 300) -> dict:
        """Check worker health using CLI."""
        result = subprocess.run(
            ["pgqrs", "worker", "health", "--queue", queue,
             "--max-age", str(max_age), "--format", "json"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        return {"error": result.stderr}

    # Or implement simple monitoring with available Python APIs
    async def monitor_workers(admin):
        workers = await admin.get_workers()
        count = await workers.count()
        print(f"Active workers: {count}")
        # Additional monitoring via CLI or database queries
    ```

## Scaling Strategies

### Auto-Scaling Based on Queue Depth

=== "Rust"

    ```rust
    async fn auto_scale_workers(
        admin: &Admin,
        queue_name: &str,
        min_workers: usize,
        max_workers: usize,
        messages_per_worker: i64,
    ) -> Result<usize> {
        let metrics = admin.queue_metrics(queue_name).await?;
        let pending = metrics.pending_messages;

        // Calculate desired workers
        let desired = ((pending / messages_per_worker) as usize)
            .clamp(min_workers, max_workers);

        tracing::info!(
            pending = pending,
            desired_workers = desired,
            "Auto-scale calculation"
        );

        Ok(desired)
    }
    ```

=== "Python"

    ```python
    import subprocess
    import json

    def get_queue_metrics(queue_name: str) -> dict:
        """Get queue metrics via CLI."""
        result = subprocess.run(
            ["pgqrs", "queue", "metrics", queue_name, "--format", "json"],
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout) if result.returncode == 0 else {}

    def calculate_desired_workers(
        queue_name: str,
        min_workers: int = 1,
        max_workers: int = 10,
        messages_per_worker: int = 100,
    ) -> int:
        """Calculate desired number of workers based on queue depth."""
        metrics = get_queue_metrics(queue_name)
        pending = metrics.get("pending_messages", 0)

        desired = max(min_workers, min(max_workers, pending // messages_per_worker))
        print(f"Queue depth: {pending}, Desired workers: {desired}")
        return desired
    ```

### Worker Pool Manager

=== "Rust"

    ```rust
    struct WorkerPool {
        workers: Vec<JoinHandle<Result<(), Error>>>,
        shutdown_tx: watch::Sender<bool>,
        config: Config,
        queue: QueueInfo,
    }

    impl WorkerPool {
        async fn new(config: Config, queue: QueueInfo, initial_size: usize) -> Self {
            let (shutdown_tx, _) = watch::channel(false);
            let mut pool = Self {
                workers: Vec::new(),
                shutdown_tx,
                config,
                queue,
            };

            pool.scale_to(initial_size).await;
            pool
        }

        async fn scale_to(&mut self, target: usize) {
            let current = self.workers.len();

            if target > current {
                // Add workers
                for i in current..target {
                    let worker = self.spawn_worker(i).await;
                    self.workers.push(worker);
                }
            } else if target < current {
                // Remove workers (send shutdown signal)
                // In practice, you'd mark specific workers for shutdown
            }
        }

        async fn spawn_worker(&self, id: usize) -> JoinHandle<Result<(), Error>> {
            let config = self.config.clone();
            let queue = self.queue.clone();
            let mut shutdown_rx = self.shutdown_tx.subscribe();

            tokio::spawn(async move {
                let admin = Admin::new(&config).await?;
                let consumer = Consumer::new(
                    admin.pool,
                    &queue,
                    &format!("pool-worker-{}", id),
                    3000 + id as i32,
                    &config,
                ).await?;

                loop {
                    if *shutdown_rx.borrow() {
                        consumer.suspend().await?;
                        consumer.shutdown().await?;
                        break;
                    }

                    let messages = consumer.dequeue().await?;
                    for msg in messages {
                        process_message(&msg).await?;
                        consumer.archive(msg.id).await?;
                    }

                    if messages.is_empty() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }

                Ok(())
            })
        }

        async fn shutdown(&mut self) {
            self.shutdown_tx.send(true).ok();

            for worker in self.workers.drain(..) {
                worker.await.ok();
            }
        }
    }
    ```

=== "Python"

    ```python
    import asyncio
    from pgqrs import Consumer

    class WorkerPool:
        def __init__(self, dsn: str, queue_name: str, initial_size: int = 1):
            self.dsn = dsn
            self.queue_name = queue_name
            self.workers: list[asyncio.Task] = []
            self.shutdown_event = asyncio.Event()

            # Spawn initial workers
            for i in range(initial_size):
                self._spawn_worker(i)

        def _spawn_worker(self, worker_id: int):
            task = asyncio.create_task(
                self._worker_loop(worker_id),
                name=f"worker-{worker_id}"
            )
            self.workers.append(task)

        async def _worker_loop(self, worker_id: int):
            consumer = Consumer(
                self.dsn,
                self.queue_name,
                f"pool-worker-{worker_id}",
                3000 + worker_id,
            )

            while not self.shutdown_event.is_set():
                try:
                    messages = await consumer.dequeue()
                    for msg in messages:
                        if self.shutdown_event.is_set():
                            break
                        await self._process_message(msg)
                        await consumer.archive(msg.id)

                    if not messages:
                        await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Worker {worker_id} error: {e}")
                    await asyncio.sleep(1)

            print(f"Worker {worker_id} stopped")

        async def _process_message(self, message):
            # Override this in subclass
            print(f"Processing message {message.id}")

        def scale_to(self, target: int):
            current = len(self.workers)
            if target > current:
                for i in range(current, target):
                    self._spawn_worker(i)
            # Note: scaling down requires more complex logic

        async def shutdown(self):
            self.shutdown_event.set()
            await asyncio.gather(*self.workers, return_exceptions=True)
            print("Worker pool shut down")

    # Usage
    async def main():
        pool = WorkerPool("postgresql://localhost/mydb", "tasks", initial_size=4)

        try:
            # Run until interrupted
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await pool.shutdown()

    asyncio.run(main())
    ```

## Cleanup

### Purge Stopped Workers

```bash
# Remove workers stopped more than 7 days ago
pgqrs worker purge --older-than 7d

# Remove workers stopped more than 30 days ago
pgqrs worker purge --older-than 30d
```

### Delete Specific Worker

```bash
# Only works if worker has no assigned messages
pgqrs worker delete --id 42
```

## Best Practices

1. **Use meaningful identifiers** - Include hostname and worker ID
2. **Send regular heartbeats** - Every 30-60 seconds
3. **Implement graceful shutdown** - Suspend before stopping
4. **Monitor worker health** - Alert on stale heartbeats
5. **Clean up stopped workers** - Purge periodically
6. **Scale based on queue depth** - Not just time of day
7. **Use connection pooling** - Share pools across workers when possible

### Worker Health Monitoring

Monitor workers proactively to detect issues early:

=== "Rust"

    ```rust
    use pgqrs::{Admin, WorkerHandle};
    use chrono::Duration;

    async fn check_worker_health(admin: &Admin) -> Result<(), Box<dyn std::error::Error>> {
        let workers = admin.get_workers().await?;
        let worker_list = workers.list().await?;

        for worker in worker_list {
            let handle = WorkerHandle::new(admin.pool.clone(), worker.id);

            // Check if worker responded to heartbeat in last 5 minutes
            if !handle.is_healthy(Duration::minutes(5)).await? {
                // Alert your monitoring system
                alert_stale_worker(&worker).await?;
            }
        }
        Ok(())
    }
    ```

=== "Python"

    ```python
    async def monitor_workers(admin: pgqrs.Admin):
        workers = await admin.get_workers()
        worker_list = await workers.list()

        stale_threshold = datetime.utcnow() - timedelta(minutes=5)

        for worker in worker_list:
            if worker.updated_at < stale_threshold:
                # Send alert to monitoring system
                await send_alert(f"Worker {worker.id} is stale")
    ```

### Automated Cleanup

Set up automated cleanup of old workers:

=== "Rust"

    ```rust
    // Run daily cleanup
    async fn daily_worker_cleanup(admin: Admin) {
        let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));

        loop {
            interval.tick().await;

            // Clean up workers stopped more than 7 days ago
            if let Err(e) = cleanup_old_workers(&admin, 7).await {
                log::error!("Worker cleanup failed: {}", e);
            }
        }
    }
    ```

=== "Python"

    ```python
    async def scheduled_cleanup(admin: pgqrs.Admin):
        while True:
            try:
                # Clean up workers stopped more than 7 days ago
                count = await cleanup_stopped_workers(admin, older_than_days=7)
                print(f"Cleaned up {count} old workers")

                # Wait 24 hours
                await asyncio.sleep(24 * 60 * 60)
            except Exception as e:
                print(f"Cleanup error: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour
    ```

## What's Next?

- [Batch Processing](batch-processing.md) - High-throughput patterns
- [CLI Reference](../cli-reference.md) - Worker management commands
