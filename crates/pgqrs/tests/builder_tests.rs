use serde_json::json;

mod common;

async fn create_store() -> pgqrs::store::AnyStore {
    let database_url = common::get_postgres_dsn(Some("pgqrs_builder_test")).await;
    let config = pgqrs::config::Config::from_dsn_with_schema(database_url, "pgqrs_builder_test")
        .expect("Failed to create config with builder_test schema");
    pgqrs::store::AnyStore::connect(&config)
        .await
        .expect("Failed to create store")
}

#[tokio::test]
async fn test_enqueue_all_options() {
    let store = create_store().await;
    let queue_name = "test_enqueue_all_options";

    // Create queue and worker
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9000, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test enqueue with all options set
    let payload = json!({"test": "all_options"});
    let msg_id = pgqrs::enqueue(&payload)
        .worker(&*producer)
        .delay(5) // 5 second delay
        .execute(&store)
        .await
        .expect("Failed to enqueue with all options");

    assert!(msg_id > 0);

    // Verify the message exists
    let message = pgqrs::tables(&store)
        .messages()
        .get(msg_id)
        .await
        .expect("Failed to get message");

    assert_eq!(message.payload, payload);
    assert!(message.vt > chrono::Utc::now()); // Should have future vt due to delay

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_enqueue_edge_cases() {
    let store = create_store().await;
    let queue_name = "test_enqueue_edge_cases";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9001, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test with empty object payload
    let empty_payload = json!({});
    let msg_id = pgqrs::enqueue(&empty_payload)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue empty payload");

    assert!(msg_id > 0);

    // Test with max delay (large number)
    let max_delay_payload = json!({"delayed": true});
    let msg_id = pgqrs::enqueue(&max_delay_payload)
        .worker(&*producer)
        .delay(3600) // 1 hour delay
        .execute(&store)
        .await
        .expect("Failed to enqueue with max delay");

    assert!(msg_id > 0);

    // Test with zero delay (should be immediate)
    let zero_delay_payload = json!({"immediate": true});
    let msg_id = pgqrs::enqueue(&zero_delay_payload)
        .worker(&*producer)
        .delay(0)
        .execute(&store)
        .await
        .expect("Failed to enqueue with zero delay");

    assert!(msg_id > 0);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue_builder_combinations() {
    let store = create_store().await;
    let queue_name = "test_dequeue_combinations";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9002, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host", 9003, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Enqueue multiple messages
    for i in 0..10 {
        pgqrs::enqueue(&json!({"index": i}))
            .worker(&*producer)
            .execute(&store)
            .await
            .expect("Failed to enqueue");
    }

    // Test different batch sizes
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(3)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue batch of 3");

    assert_eq!(messages.len(), 3);

    // Test with custom vt_offset
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(2)
        .vt_offset(30) // 30 second visibility timeout
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue with custom vt");

    assert_eq!(messages.len(), 2);
    for msg in &messages {
        assert!(msg.vt > chrono::Utc::now());
    }

    // Test with batch size of 1
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue single message");

    assert_eq!(messages.len(), 1);

    // Test with large batch size (should get remaining messages)
    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(100)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue large batch");

    assert!(messages.len() >= 4); // At least the remaining 4 messages

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_enqueue_batch_builder() {
    let store = create_store().await;
    let queue_name = "test_enqueue_batch_builder";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9004, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    // Test batch enqueue
    let payloads = vec![
        json!({"batch": 0}),
        json!({"batch": 1}),
        json!({"batch": 2}),
    ];

    let msg_ids = pgqrs::enqueue_batch(&payloads)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue batch");

    assert_eq!(msg_ids.len(), 3);
    for id in &msg_ids {
        assert!(*id > 0);
    }

    // Test empty batch
    let empty_batch: Vec<serde_json::Value> = vec![];
    let msg_ids = pgqrs::enqueue_batch(&empty_batch)
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue empty batch");

    assert_eq!(msg_ids.len(), 0);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_producer_consumer_builders() {
    let store = create_store().await;
    let queue_name = "test_worker_builders";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // Test producer builder
    let producer = pgqrs::producer("builder-host", 9005, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    assert!(producer.worker_id() > 0);
    assert_eq!(producer.queue_name(), queue_name);

    // Test consumer builder
    let consumer = pgqrs::consumer("builder-host", 9006, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    assert!(consumer.worker_id() > 0);
    assert_eq!(consumer.queue_name(), queue_name);

    // Verify workers were created
    let workers = pgqrs::tables(&store)
        .workers()
        .list()
        .await
        .expect("Failed to list workers");

    assert!(workers.iter().any(|w| w.id == producer.worker_id()));
    assert!(workers.iter().any(|w| w.id == consumer.worker_id()));

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_admin_builder() {
    let store = create_store().await;

    // Test admin builder - create new instance for each operation
    let verify_result = pgqrs::admin(&store).verify().await;
    assert!(verify_result.is_ok());

    // Create queue
    let queue_name = "test_admin_builder";
    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    assert!(queue_info.id > 0);

    // Get metrics - create new admin instance
    let metrics = pgqrs::admin(&store)
        .all_queues_metrics()
        .await
        .expect("Failed to get metrics");

    assert!(metrics.iter().any(|m| m.name == queue_name));

    // Delete queue - create new admin instance
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .expect("Failed to delete queue");
}

#[tokio::test]
async fn test_tables_builder() {
    let store = create_store().await;

    // Test tables builder - create new instance for each table access
    let queue_count = pgqrs::tables(&store)
        .queues()
        .count()
        .await
        .expect("Failed to count queues");
    assert!(queue_count >= 0);

    let message_count = pgqrs::tables(&store)
        .messages()
        .count()
        .await
        .expect("Failed to count messages");
    assert!(message_count >= 0);

    let worker_count = pgqrs::tables(&store)
        .workers()
        .count()
        .await
        .expect("Failed to count workers");
    assert!(worker_count >= 0);

    let archive_count = pgqrs::tables(&store)
        .archive()
        .count()
        .await
        .expect("Failed to count archive");
    assert!(archive_count >= 0);

    let workflow_count = pgqrs::tables(&store)
        .workflows()
        .count()
        .await
        .expect("Failed to count workflows");
    assert!(workflow_count >= 0);
}

#[tokio::test]
async fn test_archive_builder() {
    let store = create_store().await;
    let queue_name = "test_archive_builder";

    let queue_info = pgqrs::admin(&store)
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = pgqrs::producer("test-host", 9007, queue_name)
        .create(&store)
        .await
        .expect("Failed to create producer");

    let consumer = pgqrs::consumer("test-host", 9008, queue_name)
        .create(&store)
        .await
        .expect("Failed to create consumer");

    // Enqueue and archive a message
    let msg_id = pgqrs::enqueue(&json!({"archived": true}))
        .worker(&*producer)
        .execute(&store)
        .await
        .expect("Failed to enqueue");

    let messages = pgqrs::dequeue()
        .worker(&*consumer)
        .batch(1)
        .fetch_all(&store)
        .await
        .expect("Failed to dequeue");

    consumer
        .archive(messages[0].id)
        .await
        .expect("Failed to archive");

    // Test archive builder
    let count = pgqrs::archive(&store)
        .count_for_queue(queue_info.id)
        .await
        .expect("Failed to count archive");

    assert_eq!(count, 1);

    let archived_messages = pgqrs::archive(&store)
        .list_by_queue(queue_info.id)
        .await
        .expect("Failed to list archived messages");

    assert_eq!(archived_messages.len(), 1);
    assert_eq!(archived_messages[0].original_msg_id, msg_id);

    // Cleanup
    producer.suspend().await.unwrap();
    producer.shutdown().await.unwrap();
    consumer.suspend().await.unwrap();
    consumer.shutdown().await.unwrap();
    pgqrs::admin(&store)
        .purge_queue(queue_name)
        .await
        .unwrap();
    pgqrs::admin(&store)
        .delete_queue(&queue_info)
        .await
        .unwrap();
}
