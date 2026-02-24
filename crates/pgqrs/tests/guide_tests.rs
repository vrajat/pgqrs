use pgqrs::store::Store;
use serde_json::json;
use tokio::time::{sleep, timeout, Duration};

mod common;

async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    common::create_store(schema).await
}

async fn wait_for_message(
    store: &pgqrs::store::AnyStore,
    msg_id: i64,
    max_wait: Duration,
    predicate: impl Fn(&pgqrs::types::QueueMessage) -> bool,
) -> pgqrs::types::QueueMessage {
    timeout(max_wait, async {
        loop {
            let msg = store.messages().get(msg_id).await.unwrap();
            if predicate(&msg) {
                return msg;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

async fn wait_for_archived_count(
    store: &pgqrs::store::AnyStore,
    queue_id: i64,
    expected: usize,
    max_wait: Duration,
) {
    timeout(max_wait, async {
        loop {
            let archived = store
                .messages()
                .list_archived_by_queue(queue_id)
                .await
                .unwrap();
            if archived.len() == expected {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_basic_queue_single_consumer_handler_poll() {
    // --8<-- [start:basic_queue_setup]
    let store = create_store("guide_basic_queue_single").await;

    let queue = "guide_basic_queue_single";
    pgqrs::admin(&store).create_queue(queue).await.unwrap();

    let producer = pgqrs::producer("guide-producer", 9001, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer = pgqrs::consumer("guide-consumer", 9101, queue)
        .create(&store)
        .await
        .unwrap();
    // --8<-- [end:basic_queue_setup]

    // --8<-- [start:basic_queue_worker_poll]
    // Assumes `store` and `consumer` already exist.
    let store_task = store.clone();
    let consumer_task_handle = consumer.clone();

    let consumer_task = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_task_handle)
            .batch(1)
            .handle(|_msg| Box::pin(async { Ok(()) }))
            .poll(&store_task)
            .await
    });
    // --8<-- [end:basic_queue_worker_poll]

    // --8<-- [start:basic_queue_enqueue_one]
    let payload = json!({"k": "v"});
    let ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    // --8<-- [end:basic_queue_enqueue_one]

    // --8<-- [start:basic_queue_assert_and_shutdown]
    let msg = wait_for_message(&store, ids[0], Duration::from_secs(5), |m| {
        m.archived_at.is_some() && m.consumer_worker_id.is_some()
    })
    .await;
    assert_eq!(msg.payload, payload);
    assert_eq!(msg.consumer_worker_id, Some(consumer.worker_id()));

    // --8<-- [start:basic_queue_interrupt_and_shutdown]
    consumer.interrupt().await.unwrap();

    let res = timeout(Duration::from_secs(5), consumer_task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res, Err(pgqrs::error::Error::Suspended { .. })));
    assert_eq!(
        consumer.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_interrupt_and_shutdown]
    // --8<-- [end:basic_queue_assert_and_shutdown]
}

#[tokio::test]
async fn test_basic_queue_two_consumers_poll_batch_handoff() {
    let store = create_store("guide_basic_queue_handoff").await;

    let queue = "guide_basic_queue_handoff";
    pgqrs::admin(&store).create_queue(queue).await.unwrap();

    let producer = pgqrs::producer("guide-producer", 9002, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer_a = pgqrs::consumer("guide-consumer-a", 9102, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer_b = pgqrs::consumer("guide-consumer-b", 9103, queue)
        .create(&store)
        .await
        .unwrap();

    // --8<-- [start:basic_queue_handoff_start_consumer_a]
    // Assumes `store` and `consumer_a` already exist.
    let store_a = store.clone();
    let consumer_a_task_handle = consumer_a.clone();
    let task_a = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_a_task_handle)
            .batch(5)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_a)
            .await
    });
    // --8<-- [end:basic_queue_handoff_start_consumer_a]

    let payload1 = json!({"n": 1});
    let ids1 = pgqrs::enqueue()
        .message(&payload1)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg1 = wait_for_message(&store, ids1[0], Duration::from_secs(5), |m| {
        m.archived_at.is_some() && m.consumer_worker_id == Some(consumer_a.worker_id())
    })
    .await;
    assert_eq!(msg1.payload, payload1);

    // --8<-- [start:basic_queue_handoff_interrupt_consumer_a]
    // Assumes `consumer_a` and `task_a` exist.
    consumer_a.interrupt().await.unwrap();
    let res_a = timeout(Duration::from_secs(5), task_a)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res_a, Err(pgqrs::error::Error::Suspended { .. })));
    assert_eq!(
        consumer_a.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_handoff_interrupt_consumer_a]

    // --8<-- [start:basic_queue_handoff_start_consumer_b]
    // Assumes `store` and `consumer_b` already exist.
    let store_b = store.clone();
    let consumer_b_task_handle = consumer_b.clone();
    let task_b = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_b_task_handle)
            .batch(5)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_b)
            .await
    });
    // --8<-- [end:basic_queue_handoff_start_consumer_b]

    let payload2 = json!({"n": 2});
    let ids2 = pgqrs::enqueue()
        .message(&payload2)
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    let msg2 = wait_for_message(&store, ids2[0], Duration::from_secs(5), |m| {
        m.archived_at.is_some() && m.consumer_worker_id == Some(consumer_b.worker_id())
    })
    .await;
    assert_eq!(msg2.payload, payload2);

    // --8<-- [start:basic_queue_handoff_interrupt_consumer_b]
    // Assumes `consumer_b` and `task_b` exist.
    consumer_b.interrupt().await.unwrap();
    let res_b = timeout(Duration::from_secs(5), task_b)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(res_b, Err(pgqrs::error::Error::Suspended { .. })));
    assert_eq!(
        consumer_b.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_handoff_interrupt_consumer_b]
}

#[tokio::test]
async fn test_basic_queue_two_consumers_continuous_handler_poll_interrupt() {
    let store = create_store("guide_basic_queue_continuous").await;

    let queue = "guide_basic_queue_continuous";
    pgqrs::admin(&store).create_queue(queue).await.unwrap();

    let queue_id = store.queues().get_by_name(queue).await.unwrap().id;

    let producer = pgqrs::producer("guide-producer", 9003, queue)
        .create(&store)
        .await
        .unwrap();

    let consumer_a = pgqrs::consumer("guide-consumer-a", 9104, queue)
        .create(&store)
        .await
        .unwrap();
    let consumer_b = pgqrs::consumer("guide-consumer-b", 9105, queue)
        .create(&store)
        .await
        .unwrap();

    // --8<-- [start:basic_queue_continuous_start_two_consumers]
    // Assumes `store`, `consumer_a`, and `consumer_b` already exist.
    let store_a = store.clone();
    let consumer_a_task_handle = consumer_a.clone();
    let task_a = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_a_task_handle)
            .batch(10)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_a)
            .await
    });

    let store_b = store.clone();
    let consumer_b_task_handle = consumer_b.clone();
    let task_b = tokio::spawn(async move {
        pgqrs::dequeue()
            .worker(&consumer_b_task_handle)
            .batch(10)
            .handle_batch(|_msgs| Box::pin(async { Ok(()) }))
            .poll(&store_b)
            .await
    });
    // --8<-- [end:basic_queue_continuous_start_two_consumers]

    let payloads: Vec<serde_json::Value> = (0..40).map(|i| json!({"i": i})).collect();

    // Keep this small: we just want to demonstrate multi-consumer polling.
    let ids_a = pgqrs::enqueue()
        .messages(&payloads[..20])
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();
    let ids_b = pgqrs::enqueue()
        .messages(&payloads[20..])
        .worker(&producer)
        .execute(&store)
        .await
        .unwrap();

    assert_eq!(ids_a.len() + ids_b.len(), 40);

    wait_for_archived_count(&store, queue_id, 40, Duration::from_secs(10)).await;

    // --8<-- [start:basic_queue_continuous_interrupt_two_consumers]
    // Assumes `consumer_a`, `consumer_b`, `task_a`, and `task_b` exist.
    consumer_a.interrupt().await.unwrap();
    consumer_b.interrupt().await.unwrap();

    let res_a = timeout(Duration::from_secs(5), task_a)
        .await
        .unwrap()
        .unwrap();
    let res_b = timeout(Duration::from_secs(5), task_b)
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(res_a, Err(pgqrs::error::Error::Suspended { .. })));
    assert!(matches!(res_b, Err(pgqrs::error::Error::Suspended { .. })));

    assert_eq!(
        consumer_a.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    assert_eq!(
        consumer_b.status().await.unwrap(),
        pgqrs::types::WorkerStatus::Suspended
    );
    // --8<-- [end:basic_queue_continuous_interrupt_two_consumers]
}
// --8<-- [end:basic_queue_two_consumers_continuous]
