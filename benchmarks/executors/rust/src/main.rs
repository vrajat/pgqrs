use clap::Parser;
use pgqrs::store::s3::DurabilityMode;
use pgqrs::{self, Config, Store};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Barrier;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    scenario_id: String,
    #[arg(long)]
    question: String,
    #[arg(long)]
    backend: String,
    #[arg(long)]
    profile: String,
    #[arg(long)]
    dsn: String,
    #[arg(long)]
    prefill_jobs: usize,
    #[arg(long)]
    consumers: usize,
    #[arg(long)]
    dequeue_batch_size: usize,
    #[arg(long, default_value = "small")]
    payload_profile: String,
    #[arg(long, default_value = "durable")]
    durability_mode: String,
}

#[derive(Default)]
struct ConsumerStats {
    dequeues: Vec<f64>,
    archives: Vec<f64>,
    processed: usize,
    empty_polls: usize,
    errors: usize,
}

struct DrainState {
    remaining: AtomicI64,
    done: AtomicBool,
}

impl DrainState {
    fn new(total: usize) -> Self {
        Self {
            remaining: AtomicI64::new(total as i64),
            done: AtomicBool::new(false),
        }
    }

    fn remaining(&self) -> i64 {
        self.remaining.load(Ordering::SeqCst)
    }

    fn mark_archived(&self, count: usize) -> i64 {
        let remaining = self.remaining.fetch_sub(count as i64, Ordering::SeqCst) - count as i64;
        if remaining <= 0 {
            self.done.store(true, Ordering::SeqCst);
        }
        remaining
    }

    fn done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }
}

fn percentile_ms(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = ((percentile / 100.0) * (sorted.len().saturating_sub(1) as f64)).round() as usize;
    sorted
        .get(rank)
        .copied()
        .map(|secs| (secs * 1000.0 * 1000.0).round() / 1000.0)
}

async fn prefill_queue(
    producer: &pgqrs::workers::Producer,
    prefill_jobs: usize,
    payload_profile: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = json!({
        "kind": "benchmark",
        "payload_profile": payload_profile,
    });

    let max_batch_size = 100usize;
    let mut produced = 0usize;
    while produced < prefill_jobs {
        let batch_size = std::cmp::min(max_batch_size, prefill_jobs - produced);
        let payloads: Vec<Value> = (0..batch_size).map(|_| payload.clone()).collect();
        producer.batch_enqueue(&payloads).await?;
        produced += batch_size;
    }
    Ok(())
}

async fn consumer_loop(
    consumer: pgqrs::workers::Consumer,
    batch_size: usize,
    state: Arc<DrainState>,
    start_barrier: Arc<Barrier>,
) -> Result<ConsumerStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut stats = ConsumerStats::default();
    start_barrier.wait().await;

    loop {
        if state.done() {
            return Ok(stats);
        }

        let dequeue_start = Instant::now();
        let messages = consumer.dequeue_many(batch_size).await?;
        stats.dequeues.push(dequeue_start.elapsed().as_secs_f64());

        if messages.is_empty() {
            stats.empty_polls += 1;
            if state.remaining() <= 0 {
                return Ok(stats);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }

        let ids: Vec<i64> = messages.iter().map(|msg| msg.id).collect();

        let archive_start = Instant::now();
        let results = consumer.archive_many(ids).await?;
        stats.archives.push(archive_start.elapsed().as_secs_f64());

        let archived = results.iter().filter(|&&ok| ok).count();
        let failures = results.len() - archived;
        stats.errors += failures;
        stats.processed += archived;
        state.mark_archived(archived);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let mut config = Config::from_dsn(&args.dsn);
    config.validation_config.max_enqueue_per_second = None;
    config.validation_config.max_enqueue_burst = None;
    if args.backend == "s3" {
        config.s3.mode = match args.durability_mode.as_str() {
            "local" => DurabilityMode::Local,
            _ => DurabilityMode::Durable,
        };
    }

    let store = pgqrs::connect_with_config(&config).await?;
    pgqrs::admin(&store).install().await?;

    let queue_name = format!(
        "bench_drain_{:x}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    store.queue(&queue_name).await?;
    let worker_prefix = format!("bench-rust-{queue_name}");

    let producer = pgqrs::producer(&format!("{worker_prefix}-producer"), 0, &queue_name)
        .create(&store)
        .await?;
    prefill_queue(&producer, args.prefill_jobs, &args.payload_profile).await?;

    let state = Arc::new(DrainState::new(args.prefill_jobs));
    let start_barrier = Arc::new(Barrier::new(args.consumers + 1));
    let mut handles = Vec::with_capacity(args.consumers);
    for index in 0..args.consumers {
        let consumer = pgqrs::consumer(
            &format!("{worker_prefix}-consumer-{index}"),
            index as i32,
            &queue_name,
        )
        .create(&store)
        .await?;
        handles.push(tokio::spawn(consumer_loop(
            consumer,
            args.dequeue_batch_size,
            Arc::clone(&state),
            Arc::clone(&start_barrier),
        )));
    }

    let start = Instant::now();
    start_barrier.wait().await;
    let mut all_stats = Vec::with_capacity(args.consumers);
    for handle in handles {
        all_stats.push(handle.await??);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let mut dequeue_latencies = Vec::new();
    let mut archive_latencies = Vec::new();
    let mut total_processed = 0usize;
    let mut total_errors = 0usize;
    let mut empty_polls = 0usize;
    for stats in all_stats {
        dequeue_latencies.extend(stats.dequeues);
        archive_latencies.extend(stats.archives);
        total_processed += stats.processed;
        total_errors += stats.errors;
        empty_polls += stats.empty_polls;
    }

    let result = json!({
        "metadata": {
            "run_id": args.run_id,
            "scenario_id": args.scenario_id,
            "question": args.question,
            "backend": args.backend,
            "binding": "rust",
            "profile": args.profile,
            "fixed_parameters": {
                "payload_profile": args.payload_profile,
                "prefill_jobs": args.prefill_jobs,
                "queue_mode": "drain",
                "durability_mode": args.durability_mode,
            },
            "point_parameters": {
                "consumers": args.consumers,
                "dequeue_batch_size": args.dequeue_batch_size,
            },
        },
        "summary": {
            "drain_messages_per_second": if elapsed > 0.0 {
                ((total_processed as f64 / elapsed) * 1000.0).round() / 1000.0
            } else {
                0.0
            },
            "total_drain_time_ms": ((elapsed * 1000.0) * 1000.0).round() / 1000.0,
            "p95_dequeue_latency_ms": percentile_ms(&dequeue_latencies, 95.0),
            "p95_archive_latency_ms": percentile_ms(&archive_latencies, 95.0),
            "error_rate": if total_processed + total_errors > 0 {
                (((total_errors as f64) / ((total_processed + total_errors) as f64)) * 1_000_000.0).round() / 1_000_000.0
            } else {
                0.0
            },
            "processed_messages": total_processed,
            "empty_polls": empty_polls,
        },
        "samples": [],
    });

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
