#[cfg(feature = "s3")]
use pgqrs::Store;

#[cfg(feature = "s3")]
#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[cfg(feature = "s3")]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut dsn = None::<String>;
    let mut queue = None::<String>;
    let mut cache_prefix = None::<String>;
    let mut sleep_before_sync_ms: u64 = 0;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dsn" => dsn = args.next(),
            "--queue" => queue = args.next(),
            "--cache-prefix" => cache_prefix = args.next(),
            "--sleep-before-sync-ms" => {
                sleep_before_sync_ms = args
                    .next()
                    .ok_or("missing value for --sleep-before-sync-ms")?
                    .parse::<u64>()?
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let dsn = dsn.ok_or("missing --dsn")?;
    let queue = queue.ok_or("missing --queue")?;
    let cache_prefix = cache_prefix.ok_or("missing --cache-prefix")?;

    let mut config = pgqrs::config::Config::from_dsn_with_schema(&dsn, "s3_process_helper")?;
    config.s3.mode = pgqrs::store::s3::DurabilityMode::Local;
    config.s3.cache_prefix = cache_prefix;

    let mut store = pgqrs::connect_with_config(&config).await?;
    if let pgqrs::store::AnyStore::S3(s3_store) = &mut store {
        match s3_store.snapshot().await {
            Ok(()) => {}
            Err(pgqrs::error::Error::NotFound { .. }) => {
                // No remote object yet; initialize local schema for first writer.
                store.bootstrap().await?;
            }
            Err(err) => return Err(err.into()),
        }
    }

    pgqrs::admin(&store).create_queue(&queue).await?;

    if sleep_before_sync_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(sleep_before_sync_ms)).await;
    }

    if let pgqrs::store::AnyStore::S3(s3_store) = &mut store {
        s3_store.sync().await?;
    } else {
        return Err("expected s3 store".into());
    }

    Ok(())
}

#[cfg(not(feature = "s3"))]
fn main() {
    eprintln!("s3_process_helper requires feature 's3'");
    std::process::exit(1);
}
