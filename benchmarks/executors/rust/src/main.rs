use clap::{Parser, Subcommand};
use pgqrs::{self, store::AnyStore, Config, Store};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::Barrier;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Child,
};

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run(RunArgs),
    Worker(WorkerArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
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
    #[arg(long)]
    process_mode: String,
}

#[derive(Parser, Debug)]
struct WorkerArgs {
    #[arg(long)]
    backend: String,
    #[arg(long)]
    dsn: String,
    #[arg(long)]
    queue_name: String,
    #[arg(long)]
    worker_name: String,
    #[arg(long)]
    worker_index: i32,
    #[arg(long)]
    dequeue_batch_size: usize,
    #[arg(long, default_value = "durable")]
    durability_mode: String,
    #[arg(long)]
    start_at_unix_ms: u64,
    #[arg(long, default_value = "100")]
    idle_exit_polls: usize,
}

#[derive(Default, Serialize, Deserialize)]
struct ConsumerStats {
    dequeues: Vec<f64>,
    archives: Vec<f64>,
    processed: usize,
    empty_polls: usize,
    dequeue_conflicts: usize,
    archive_conflicts: usize,
    create_conflict_retries: usize,
    errors: usize,
}

#[derive(Serialize, Deserialize)]
struct WorkerProcessResult {
    stats: ConsumerStats,
    cache_prefix: Option<String>,
    cache_base_dir: Option<String>,
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

#[cfg(feature = "s3")]
fn maybe_apply_s3_mode(config: &mut Config, backend: &str, durability_mode: &str) {
    if backend == "s3" {
        config.s3.mode = match durability_mode {
            "local" => pgqrs::store::s3::DurabilityMode::Local,
            _ => pgqrs::store::s3::DurabilityMode::Durable,
        };
    }
}

#[cfg(not(feature = "s3"))]
fn maybe_apply_s3_mode(config: &mut Config, backend: &str, durability_mode: &str) {
    let _ = (config, backend, durability_mode);
}

async fn connect_store(
    dsn: &str,
    backend: &str,
    durability_mode: &str,
) -> Result<AnyStore, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = Config::from_dsn(dsn);
    config.validation_config.max_enqueue_per_second = None;
    config.validation_config.max_enqueue_burst = None;
    maybe_apply_s3_mode(&mut config, backend, durability_mode);
    Ok(pgqrs::connect_with_config(&config).await?)
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

async fn create_consumer_with_conflict_retry<S: Store>(
    store: &S,
    worker_name: &str,
    worker_index: i32,
    queue_name: &str,
) -> Result<(pgqrs::workers::Consumer, usize), Box<dyn std::error::Error + Send + Sync>> {
    const MAX_CREATE_RETRIES: usize = 10_000;
    let mut retries = 0usize;
    loop {
        match pgqrs::consumer(worker_name, worker_index, queue_name)
            .create(store)
            .await
        {
            Ok(consumer) => return Ok((consumer, retries)),
            Err(pgqrs::error::Error::Conflict { .. }) if retries < MAX_CREATE_RETRIES => {
                retries += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

fn hash_ids(ids: &[i64]) -> u64 {
    let mut hasher = DefaultHasher::new();
    ids.hash(&mut hasher);
    hasher.finish()
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
        let messages = match consumer.dequeue_many(batch_size).await {
            Ok(messages) => messages,
            Err(pgqrs::error::Error::Conflict { .. }) => {
                stats.dequeue_conflicts += 1;
                continue;
            }
            Err(err) => return Err(err.into()),
        };
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

        loop {
            let archive_start = Instant::now();
            let results = match consumer.archive_many(ids.clone()).await {
                Ok(results) => results,
                Err(pgqrs::error::Error::Conflict { .. }) => {
                    stats.dequeue_conflicts += 1;
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            stats.archives.push(archive_start.elapsed().as_secs_f64());

            let archived = results.iter().filter(|&&ok| ok).count();
            let failures = results.len() - archived;
            stats.errors += failures;
            stats.processed += archived;
            state.mark_archived(archived);
            break;
        }
    }
}

async fn consumer_loop_process_isolated(
    consumer: pgqrs::workers::Consumer,
    worker_name: &str,
    batch_size: usize,
    idle_exit_polls: usize,
    create_conflict_retries: usize,
) -> Result<ConsumerStats, Box<dyn std::error::Error + Send + Sync>> {
    const LOG_EVERY_SECS: u64 = 5;
    const ARCHIVE_STALL_RESET_SECS: u64 = 15;
    const ARCHIVE_WARN_RETRY_THRESHOLDS: [usize; 3] = [50, 200, 1000];

    let mut stats = ConsumerStats {
        create_conflict_retries,
        ..ConsumerStats::default()
    };
    let mut consecutive_empty_polls = 0usize;
    let mut last_progress_log = Instant::now();
    let mut last_archive_success = Instant::now();

    loop {
        if last_progress_log.elapsed() >= Duration::from_secs(LOG_EVERY_SECS) {
            eprintln!(
                "[bench-worker-progress] worker={} create_conflict_retries={} dequeue_ok={} dequeue_empty={} dequeue_conflict={} archive_ok_batches={} archive_ok_messages={} archive_conflict={} last_success_age_ms={}",
                worker_name,
                stats.create_conflict_retries,
                stats.dequeues.len(),
                stats.empty_polls,
                stats.dequeue_conflicts,
                stats.archives.len(),
                stats.processed,
                stats.archive_conflicts,
                last_archive_success.elapsed().as_millis(),
            );
            last_progress_log = Instant::now();
        }

        let dequeue_start = Instant::now();
        let messages = match consumer.dequeue_many(batch_size).await {
            Ok(messages) => messages,
            Err(pgqrs::error::Error::Conflict { .. }) => {
                stats.dequeue_conflicts += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        stats.dequeues.push(dequeue_start.elapsed().as_secs_f64());

        if messages.is_empty() {
            stats.empty_polls += 1;
            consecutive_empty_polls += 1;
            if consecutive_empty_polls >= idle_exit_polls {
                return Ok(stats);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }

        consecutive_empty_polls = 0;
        let ids: Vec<i64> = messages.iter().map(|msg| msg.id).collect();
        let batch_hash = hash_ids(&ids);
        let mut repeated_archive_batch_hash = None;
        let mut repeated_archive_conflicts = 0usize;

        'archive_retry: loop {
            let archive_start = Instant::now();
            let results = match consumer.archive_many(ids.clone()).await {
                Ok(results) => results,
                Err(pgqrs::error::Error::Conflict { .. }) => {
                    stats.archive_conflicts += 1;
                    if repeated_archive_batch_hash == Some(batch_hash) {
                        repeated_archive_conflicts += 1;
                    } else {
                        repeated_archive_batch_hash = Some(batch_hash);
                        repeated_archive_conflicts = 1;
                    }
                    if ARCHIVE_WARN_RETRY_THRESHOLDS.contains(&repeated_archive_conflicts) {
                        eprintln!(
                            "[bench-worker-warn] worker={} archive_conflict_streak={} batch_hash={} batch_size={}",
                            worker_name, repeated_archive_conflicts, batch_hash, ids.len()
                        );
                    }
                    if repeated_archive_conflicts >= ARCHIVE_WARN_RETRY_THRESHOLDS[2]
                        && last_archive_success.elapsed()
                            >= Duration::from_secs(ARCHIVE_STALL_RESET_SECS)
                    {
                        eprintln!(
                            "[bench-worker-warn] worker={} archive_livelock_escape=1 conflict_streak={} batch_hash={} last_success_age_ms={}",
                            worker_name,
                            repeated_archive_conflicts,
                            batch_hash,
                            last_archive_success.elapsed().as_millis(),
                        );
                        break 'archive_retry;
                    }
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            stats.archives.push(archive_start.elapsed().as_secs_f64());

            let archived = results.iter().filter(|&&ok| ok).count();
            let failures = results.len() - archived;
            stats.errors += failures;
            stats.processed += archived;
            if archived > 0 {
                last_archive_success = Instant::now();
            }
            break;
        }
    }
}

fn aggregate_consumer_stats(
    all_stats: Vec<ConsumerStats>,
) -> (Vec<f64>, Vec<f64>, usize, usize, usize, usize, usize, usize) {
    let mut dequeue_latencies = Vec::new();
    let mut archive_latencies = Vec::new();
    let mut total_processed = 0usize;
    let mut total_errors = 0usize;
    let mut empty_polls = 0usize;
    let mut dequeue_conflicts = 0usize;
    let mut archive_conflicts = 0usize;
    let mut create_conflict_retries = 0usize;
    for stats in all_stats {
        dequeue_latencies.extend(stats.dequeues);
        archive_latencies.extend(stats.archives);
        total_processed += stats.processed;
        total_errors += stats.errors;
        empty_polls += stats.empty_polls;
        dequeue_conflicts += stats.dequeue_conflicts;
        archive_conflicts += stats.archive_conflicts;
        create_conflict_retries += stats.create_conflict_retries;
    }
    (
        dequeue_latencies,
        archive_latencies,
        total_processed,
        total_errors,
        empty_polls,
        dequeue_conflicts,
        archive_conflicts,
        create_conflict_retries,
    )
}

async fn run_worker(args: WorkerArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    if args.start_at_unix_ms > now_unix_ms {
        tokio::time::sleep(Duration::from_millis(args.start_at_unix_ms - now_unix_ms)).await;
    }

    let cache_prefix = std::env::var("PGQRS_S3_CACHE_PREFIX")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let cache_base_dir = std::env::var("PGQRS_S3_LOCAL_CACHE_DIR")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if args.backend == "s3" {
        eprintln!(
            "[bench-worker] worker={} cache_prefix={:?} cache_base_dir={:?}",
            args.worker_name, cache_prefix, cache_base_dir
        );
    }

    let store = connect_store(&args.dsn, &args.backend, &args.durability_mode).await?;
    let (consumer, create_conflict_retries) = create_consumer_with_conflict_retry(
        &store,
        &args.worker_name,
        args.worker_index,
        &args.queue_name,
    )
    .await?;
    let stats = consumer_loop_process_isolated(
        consumer,
        &args.worker_name,
        args.dequeue_batch_size,
        args.idle_exit_polls,
        create_conflict_retries,
    )
    .await?;
    let output = WorkerProcessResult {
        stats,
        cache_prefix,
        cache_base_dir,
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

async fn run_multi_process(
    args: &RunArgs,
    queue_name: &str,
) -> Result<(Vec<ConsumerStats>, f64, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let worker_prefix = format!("bench-rust-{queue_name}");
    let exe_path = std::env::current_exe()?;
    let start_at_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64 + 500;
    let mut children = Vec::with_capacity(args.consumers);
    let mut spawned_prefixes = Vec::new();
    for index in 0..args.consumers {
        let cache_prefix = format!("{worker_prefix}-cache-{index}");
        let mut cmd = Command::new(&exe_path);
        cmd.arg("worker")
            .arg("--backend")
            .arg(&args.backend)
            .arg("--dsn")
            .arg(&args.dsn)
            .arg("--queue-name")
            .arg(queue_name)
            .arg("--worker-name")
            .arg(format!("{worker_prefix}-consumer-{index}"))
            .arg("--worker-index")
            .arg(index.to_string())
            .arg("--dequeue-batch-size")
            .arg(args.dequeue_batch_size.to_string())
            .arg("--durability-mode")
            .arg(&args.durability_mode)
            .arg("--start-at-unix-ms")
            .arg(start_at_unix_ms.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.kill_on_drop(true);
        if args.backend == "s3" {
            cmd.env("PGQRS_S3_CACHE_PREFIX", &cache_prefix);
            spawned_prefixes.push(cache_prefix);
        }
        children.push(cmd.spawn()?);
    }

    let mut worker_tasks = tokio::task::JoinSet::new();
    for (index, child) in children.into_iter().enumerate() {
        worker_tasks.spawn(async move { (index, wait_worker_with_streaming_logs(index, child).await) });
    }

    let mut stats_by_index: Vec<Option<ConsumerStats>> = (0..args.consumers).map(|_| None).collect();
    let mut cache_prefix_by_index: Vec<Option<String>> = (0..args.consumers).map(|_| None).collect();
    while let Some(join_result) = worker_tasks.join_next().await {
        let (index, result) =
            join_result.map_err(|err| format!("Worker task join error: {err}"))?;
        let (status, payload) = result.map_err(|err| format!("Worker {index} failed: {err}"))?;
        if !status.success() {
            return Err(format!("Worker {index} exited with {status}").into());
        }
        let payload = payload.ok_or("Worker did not produce JSON stats")?;
        let worker_result: WorkerProcessResult = serde_json::from_str(&payload)?;
        stats_by_index[index] = Some(worker_result.stats);
        cache_prefix_by_index[index] = worker_result.cache_prefix;
    }

    let mut all_stats = Vec::with_capacity(args.consumers);
    let mut worker_cache_prefixes = Vec::new();
    for index in 0..args.consumers {
        let stats = stats_by_index[index]
            .take()
            .ok_or_else(|| format!("Worker {index} did not return stats"))?;
        all_stats.push(stats);
        if let Some(prefix) = cache_prefix_by_index[index].take() {
            worker_cache_prefixes.push(prefix);
        }
    }
    if worker_cache_prefixes.is_empty() {
        worker_cache_prefixes = spawned_prefixes;
    }

    let end_unix_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    let start_unix_secs = (start_at_unix_ms as f64) / 1000.0;
    let elapsed = (end_unix_secs - start_unix_secs).max(0.0);
    Ok((all_stats, elapsed, worker_cache_prefixes))
}

async fn wait_worker_with_streaming_logs(
    index: usize,
    mut child: Child,
) -> Result<(std::process::ExitStatus, Option<String>), String> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("worker {index}: missing stdout pipe"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("worker {index}: missing stderr pipe"))?;

    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut last_stdout_line = None;

    while !stdout_done || !stderr_done {
        tokio::select! {
            line = stdout_lines.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            last_stdout_line = Some(trimmed.to_string());
                        }
                    }
                    Ok(None) => stdout_done = true,
                    Err(err) => return Err(format!("worker {index}: stdout read error: {err}")),
                }
            }
            line = stderr_lines.next_line(), if !stderr_done => {
                match line {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            eprintln!("[bench-worker-{index}] {trimmed}");
                        }
                    }
                    Ok(None) => stderr_done = true,
                    Err(err) => return Err(format!("worker {index}: stderr read error: {err}")),
                }
            }
        }
    }

    let status = child
        .wait()
        .await
        .map_err(|err| format!("worker {index}: wait failed: {err}"))?;
    Ok((status, last_stdout_line))
}

async fn run_single_process(
    args: &RunArgs,
    store: &AnyStore,
    queue_name: &str,
) -> Result<(Vec<ConsumerStats>, f64, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let worker_prefix = format!("bench-rust-{queue_name}");
    let state = Arc::new(DrainState::new(args.prefill_jobs));
    // Keep all consumers parked until the benchmark clock is armed; otherwise
    // fast points can do real work before `start` and inflate throughput.
    let start_barrier = Arc::new(Barrier::new(args.consumers + 1));
    let mut handles = Vec::with_capacity(args.consumers);
    for index in 0..args.consumers {
        let (consumer, _) = create_consumer_with_conflict_retry(
            store,
            &format!("{worker_prefix}-consumer-{index}"),
            index as i32,
            queue_name,
        )
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
    Ok((all_stats, start.elapsed().as_secs_f64(), Vec::new()))
}

async fn run_coordinator(args: RunArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let process_mode = args.process_mode.as_str();
    if process_mode != "single_process" && process_mode != "multi_process" {
        return Err(format!("Unsupported process mode: {process_mode}").into());
    }

    let store = connect_store(&args.dsn, &args.backend, &args.durability_mode).await?;
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

    let (all_stats, elapsed, worker_cache_prefixes) = match process_mode {
        "single_process" => run_single_process(&args, &store, &queue_name).await?,
        "multi_process" => run_multi_process(&args, &queue_name).await?,
        _ => unreachable!(),
    };

    let (
        dequeue_latencies,
        archive_latencies,
        total_processed,
        total_errors,
        empty_polls,
        dequeue_conflicts,
        archive_conflicts,
        create_conflict_retries,
    ) = aggregate_consumer_stats(all_stats);

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
                "process_mode": args.process_mode,
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
            "dequeue_calls": dequeue_latencies.len(),
            "archive_calls": archive_latencies.len(),
            "empty_polls": empty_polls,
            "dequeue_conflicts": dequeue_conflicts,
            "archive_conflicts": archive_conflicts,
            "create_conflict_retries": create_conflict_retries,
            "worker_cache_prefixes": worker_cache_prefixes,
        },
        "samples": [],
    });

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match Cli::parse().command {
        Commands::Run(args) => run_coordinator(args).await,
        Commands::Worker(args) => run_worker(args).await,
    }
}
