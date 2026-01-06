use serde_json::json;
use std::time::Duration;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    common::create_store("pgqrs_builder_ergonomics_test").await
}

#[tokio::test]
async fn test_enqueue_with_delay_duration() {
    let store = create_store().await;
    let queue_name = "test_with_delay";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9100, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test with_delay using Duration
    let payload = json!({"test": "with_delay"});
    let before = chrono::Utc::now();

    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .with_delay(Duration::from_secs(300)) // 5 minutes
        .execute(&store)
        .await
        .expect("Failed to enqueue with Duration delay");
    let msg_id = msg_ids[0];

    assert!(msg_id > 0);

    // Verify the message has correct visibility timeout
    let message = pgqrs::tables(&store)
        .messages()
        .get(msg_id)
        .await
        .expect("Failed to get message");

    assert_eq!(message.payload, payload);

    // VT should be approximately 300 seconds (5 mins) in the future
    let vt_diff = (message.vt - before).num_seconds();
    assert!((295..=305).contains(&vt_diff), "VT diff was {}", vt_diff);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_with_vt_duration() {
    let store = create_store().await;
    let queue_name = "test_with_vt";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9101, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host", 9102, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Enqueue a message
    pgqrs::enqueue()
        .message(&json!({"test": "vt"}))
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue");

    // Dequeue with Duration-based VT
    let before = chrono::Utc::now();
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .with_vt(Duration::from_secs(600)) // 10 minutes
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue with Duration VT");

    assert_eq!(messages.len(), 1);

    // VT should be approximately 600 seconds in the future
    let vt_diff = (messages[0].vt - before).num_seconds();
    assert!((595..=605).contains(&vt_diff), "VT diff was {}", vt_diff);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    // Release messages before shutting down consumer
    let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    consumer.release_messages(&msg_ids).await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_limit() {
    let store = create_store().await;
    let queue_name = "test_limit";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9103, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host", 9104, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Enqueue 10 messages
    for i in 0..10 {
        pgqrs::enqueue()
            .message(&json!({"index": i}))
            .worker(&*producer)
            .execute(&store)
            .await
            .expect("Failed to enqueue");
    }

    // Test limit() method (alias for batch())
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .limit(5)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue with limit");

    assert_eq!(messages.len(), 5);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    // Release messages before shutting down consumer
    let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    consumer.release_messages(&msg_ids).await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_builder_method_chaining() {
    let store = create_store().await;
    let queue_name = "test_chaining";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9105, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host", 9106, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Test chaining multiple ergonomic methods
    let payload = json!({"test": "chaining"});
    let msg_ids = pgqrs::enqueue()
        .message(&payload)
        .worker(&*producer)
        .with_delay(Duration::from_secs(10))
        .execute(&store)
        .await
        .expect("Failed to enqueue");
    let msg_id = msg_ids[0];

    assert!(msg_id > 0);

    // Wait for message to become visible
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;

    // Dequeue with chained ergonomic methods
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .limit(1)
        .with_vt(Duration::from_secs(30))
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload, payload);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    // Release messages before shutting down consumer
    let msg_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    consumer.release_messages(&msg_ids).await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store).purge_queue(queue_name).await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}
