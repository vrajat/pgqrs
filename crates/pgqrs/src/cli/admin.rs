use std::time::Duration;
use tokio::time::sleep;

pub async fn run(
    dsn: String,
    schema: String,
    interval_ms: u64,
    heartbeat_timeout_secs: i64,
    workflow_timeout_secs: i64,
    cron_batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting pgqrs admin coordinator daemon...");
    println!("Schema: {}", schema);
    println!("Check Interval: {}ms", interval_ms);
    println!("Heartbeat Timeout: {}s", heartbeat_timeout_secs);
    println!("Workflow Timeout: {}s", workflow_timeout_secs);
    println!("Cron Batch Size: {}", cron_batch_size);

    let config = pgqrs::Config::from_dsn_with_schema(&dsn, &schema)?;
    let store = pgqrs::connect_with_config(&config).await?;

    // Make sure tables are migrated / bootstrapped
    store.bootstrap().await?;

    let interval = Duration::from_millis(interval_ms);
    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);

    // Cache of managed producers for active crons to avoid constantly creating workers
    let mut producers = std::collections::HashMap::<String, pgqrs::workers::Producer>::new();

    // Heartbeat ticker for managed producers
    let heartbeat_interval = Duration::from_secs(store.config().heartbeat_interval as u64);
    let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);
    // tick once to align immediately
    heartbeat_ticker.tick().await;

    loop {
        tokio::select! {
            _ = &mut sigint => {
                println!("Received shutdown signal. Stopping pgqrs admin...");
                break;
            }
            _ = heartbeat_ticker.tick() => {
                // Heartbeat all cached producers
                for (queue, producer) in &producers {
                    if let Err(e) = producer.heartbeat().await {
                        eprintln!("Error heartbeating producer for queue '{}': {}", queue, e);
                    }
                }
            }
            _ = sleep(interval) => {
                // 1. Run Cron Scanner loop
                loop {
                    match pgqrs::admin(&store).scan_cron_batch(&mut producers, cron_batch_size).await {
                        Ok(true) => {
                            // Triggered a batch of crons, check immediately for more due crons without delay
                            continue;
                        }
                        Ok(false) => {
                            // No crons due
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error in cron scanner: {}", e);
                            break;
                        }
                    }
                }

                // 2. Run Maintenance Sweeper
                if let Err(e) = pgqrs::admin(&store).run_maintenance_sweep(heartbeat_timeout_secs, workflow_timeout_secs).await {
                    eprintln!("Error in maintenance sweep: {}", e);
                }
            }
        }
    }

    Ok(())
}
