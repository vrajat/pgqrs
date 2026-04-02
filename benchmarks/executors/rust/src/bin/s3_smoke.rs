use clap::Parser;
use pgqrs::store::s3::DurabilityMode;
use pgqrs::store::AnyStore;
use pgqrs::{self, Config, Store};
use serde_json::json;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    dsn: String,
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    #[arg(long, default_value_t = 0)]
    jitter_ms: u64,
}

fn as_ms(start: Instant) -> f64 {
    ((start.elapsed().as_secs_f64() * 1000.0) * 1000.0).round() / 1000.0
}

async fn sync_store(store: &mut AnyStore) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match store {
        AnyStore::S3(s3_store) => {
            s3_store.sync().await?;
            Ok(())
        }
        _ => Err("expected S3 store".into()),
    }
}

async fn snapshot_store(
    store: &mut AnyStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match store {
        AnyStore::S3(s3_store) => {
            s3_store.snapshot().await?;
            Ok(())
        }
        _ => Err("expected S3 store".into()),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let mut writer_config = Config::from_dsn(&args.dsn);
    writer_config.s3.mode = DurabilityMode::Local;
    writer_config.validation_config.max_enqueue_per_second = None;
    writer_config.validation_config.max_enqueue_burst = None;

    let mut writer_store = pgqrs::connect_with_config(&writer_config).await?;
    writer_store.bootstrap().await?;
    let bootstrap_sync_start = Instant::now();
    sync_store(&mut writer_store).await?;
    let bootstrap_sync_latency_ms = as_ms(bootstrap_sync_start);

    let queue_name = format!(
        "bench_smoke_{:x}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    pgqrs::admin(&writer_store).create_queue(&queue_name).await?;
    let producer = pgqrs::producer("bench-smoke-producer", 0, &queue_name)
        .create(&writer_store)
        .await?;

    pgqrs::enqueue()
        .message(&json!({ "kind": "s3_bench_smoke" }))
        .worker(&producer)
        .execute(&writer_store)
        .await?;

    let sync_start = Instant::now();
    sync_store(&mut writer_store).await?;
    let sync_latency_ms = as_ms(sync_start);

    let mut follower_config = Config::from_dsn(&args.dsn);
    follower_config.s3.mode = DurabilityMode::Local;
    let mut follower_store = pgqrs::connect_with_config(&follower_config).await?;

    let snapshot_start = Instant::now();
    snapshot_store(&mut follower_store).await?;
    let snapshot_latency_ms = as_ms(snapshot_start);

    let queue_exists = pgqrs::tables(&follower_store)
        .queues()
        .exists(&queue_name)
        .await?;
    let message_count = pgqrs::tables(&follower_store)
        .messages()
        .count()
        .await?;

    let result = json!({
        "metadata": {
            "run_id": args.run_id,
            "scenario_id": "s3.stack.smoke",
            "question": "Does the LocalStack+Toxiproxy S3 benchmark stack work end to end?",
            "backend": "s3",
            "binding": "rust",
            "profile": "smoke",
            "fixed_parameters": {
                "latency_ms": args.latency_ms,
                "jitter_ms": args.jitter_ms,
                "durability_mode": "local",
            },
            "point_parameters": {},
        },
        "summary": {
            "bootstrap_sync_latency_ms": bootstrap_sync_latency_ms,
            "sync_latency_ms": sync_latency_ms,
            "snapshot_latency_ms": snapshot_latency_ms,
            "queue_exists_after_snapshot": queue_exists,
            "message_count_after_snapshot": message_count,
            "smoke_ok": queue_exists && message_count >= 1,
        },
        "samples": [],
    });

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
