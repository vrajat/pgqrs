use pgqrs::{Admin, Config, Consumer, Producer};
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn test_concurrent_visibility_extension() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup Postgres container
    let container = Postgres::default().with_tag("15-alpine").start().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

    let config = Config::from_dsn(&db_url);
    let admin = Admin::new(&config).await?;
    admin.install().await?;

    let queue_name = "concurrent_vis_queue";
    admin.create_queue(queue_name).await?;

    // 2. Register Consumers
    let consumer_a = Consumer::new(
        admin.pool.clone(),
        &admin.get_queue(queue_name).await?,
        "consumer_a",
        1001,
        &config,
    )
    .await?;

    let consumer_b = Consumer::new(
        admin.pool.clone(),
        &admin.get_queue(queue_name).await?,
        "consumer_b",
        1002,
        &config,
    )
    .await?;

    let producer = Producer::new(
        admin.pool.clone(),
        &admin.get_queue(queue_name).await?,
        "producer",
        2001,
        &config,
    )
    .await?;

    // 3. Enqueue Message
    let msg_id = producer.enqueue(&serde_json::json!({"foo": "bar"})).await?.id;

    // 4. Consumer A dequeues
    let msgs_a = consumer_a.dequeue().await?;
    assert_eq!(msgs_a.len(), 1);
    assert_eq!(msgs_a[0].id, msg_id);

    // 5. Consumer B tries to extend visibility -> SHOULD FAIL
    let extended_by_b = consumer_b.extend_visibility(msg_id, 10).await?;
    assert!(!extended_by_b, "Consumer B should not be able to extend visibility of message owned by A");

    // 6. Consumer A tries to extend visibility -> SHOULD SUCCEED
    let extended_by_a = consumer_a.extend_visibility(msg_id, 10).await?;
    assert!(extended_by_a, "Consumer A should be able to extend visibility of its own message");

    Ok(())
}
