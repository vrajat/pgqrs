use pgqrs::{Admin, Config, Consumer, Producer, Worker};
use serde_json::json;
use serial_test::serial;

mod common;

async fn create_admin() -> pgqrs::admin::Admin {
    let database_url = common::get_postgres_dsn(Some("pgqrs_concurrent_test")).await;
    let admin =
        Admin::new(&Config::from_dsn_with_schema(database_url, "pgqrs_concurrent_test").unwrap())
            .await
            .expect("Failed to create Admin");

    // Clean up any existing queues/workers to ensure test isolation
    // Since schema is unique per test run usually via common, this might be redundant but safe
    if let Err(e) =
        sqlx::query("TRUNCATE TABLE pgqrs_workers, pgqrs_queues RESTART IDENTITY CASCADE")
            .execute(&admin.pool)
            .await
    {
        eprintln!("Warning: Failed to truncate tables: {}", e);
    }

    admin
}

#[tokio::test]
#[serial]
async fn test_zombie_consumer_race_condition() {
    let admin = create_admin().await;
    let pool = admin.pool.clone();
    let config = admin.config.clone();

    let queue_name = "race_condition_queue";
    let queue_info = admin
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    // 2. Setup Producer and Consumer A & B
    let producer = Producer::new(pool.clone(), &queue_info, "producer_host", 1000, &config)
        .await
        .expect("Failed to register producer");

    let consumer_a = Consumer::new(pool.clone(), &queue_info, "consumer_a", 2000, &config)
        .await
        .expect("Failed to register consumer A");

    let consumer_b = Consumer::new(pool.clone(), &queue_info, "consumer_b", 2001, &config)
        .await
        .expect("Failed to register consumer B");

    // 3. Enqueue Message
    let payload = json!({"task": "slow_process"});
    let msg = producer.enqueue(&payload).await.expect("Enqueue failed");
    println!("Enqueued message ID: {}", msg.id);

    // 4. Consumer A dequeues with SHORT visibility (e.g., 1 second)
    // We use dequeue_many_with_delay to set explicit short timeout
    let msgs_a = consumer_a
        .dequeue_many_with_delay(1, 1)
        .await
        .expect("Dequeue A failed");
    assert_eq!(msgs_a.len(), 1);
    let msg_a = &msgs_a[0];
    assert_eq!(msg_a.id, msg.id);
    println!("Consumer A dequeued message. Holding lock for 1s...");

    // 5. Simulate Consumer A losing the lease (e.g. system reclamation or crash recovery)
    // We explicitly release the messages from A so B can pick them up.
    // In a real system, a "reaper" process would do this for expired messages.
    println!("Simulating lease reclamation for Consumer A...");
    let released = admin
        .release_worker_messages(consumer_a.worker_id())
        .await
        .expect("Release failed");
    assert_eq!(released, 1, "Should have released 1 message");

    // 6. Consumer B dequeues the SAME message (stealing the lock)
    let msgs_b = consumer_b.dequeue().await.expect("Dequeue B failed");
    assert_eq!(
        msgs_b.len(),
        1,
        "Consumer B should be able to pick up expired message"
    );
    let msg_b = &msgs_b[0];
    assert_eq!(msg_b.id, msg.id);
    println!("Consumer B dequeued message (stole lock).");

    // 7. Consumer A tries to DELETE -> Should FAIL (return false)
    let deleted_a = consumer_a.delete(msg.id).await.expect("Delete A op failed");
    assert!(
        !deleted_a,
        "Consumer A should NOT be able to delete message owned by B"
    );
    println!("Consumer A delete correctly failed.");

    // 8. Consumer A tries to ARCHIVE -> Should FAIL (return None)
    let archived_a = consumer_a
        .archive(msg.id)
        .await
        .expect("Archive A op failed");
    assert!(
        archived_a.is_none(),
        "Consumer A should NOT be able to archive message owned by B"
    );
    println!("Consumer A archive correctly failed.");

    // 9. Consumer B completes work and DELETES -> Should SUCCEED
    let deleted_b = consumer_b.delete(msg.id).await.expect("Delete B op failed");
    assert!(
        deleted_b,
        "Consumer B should be able to delete its own message"
    );
    println!("Consumer B delete succeeded.");
}

#[tokio::test]
#[serial]
async fn test_zombie_consumer_batch_ops() {
    let admin = create_admin().await;
    let pool = admin.pool.clone();
    let config = admin.config.clone();

    let queue_name = "batch_race_queue";
    let queue_info = admin
        .create_queue(queue_name)
        .await
        .expect("Failed to create queue");

    let producer = Producer::new(pool.clone(), &queue_info, "prod", 1, &config)
        .await
        .unwrap();
    let consumer_a = Consumer::new(pool.clone(), &queue_info, "con_a", 2, &config)
        .await
        .unwrap();
    let consumer_b = Consumer::new(pool.clone(), &queue_info, "con_b", 3, &config)
        .await
        .unwrap();

    // Enqueue 2 messages
    let msg1 = producer.enqueue(&json!(1)).await.unwrap();
    let msg2 = producer.enqueue(&json!(2)).await.unwrap();

    // A dequeues both with short timeout
    let msgs_a = consumer_a.dequeue_many_with_delay(2, 1).await.unwrap();
    assert_eq!(msgs_a.len(), 2);

    // Simulate reclamation of messages from A
    let released = admin
        .release_worker_messages(consumer_a.worker_id())
        .await
        .unwrap();
    assert_eq!(released, 2);

    // B dequeues both
    let msgs_b = consumer_b.dequeue_many(2).await.unwrap();
    assert_eq!(msgs_b.len(), 2);

    // A tries delete_many -> Should return [false, false]
    let results_a = consumer_a
        .delete_many(vec![msg1.id, msg2.id])
        .await
        .unwrap();
    assert_eq!(
        results_a,
        vec![false, false],
        "Batch delete by A should fail for all"
    );

    // A tries archive_many -> return [false, false]
    let arch_results_a = consumer_a
        .archive_many(vec![msg1.id, msg2.id])
        .await
        .unwrap();
    assert_eq!(
        arch_results_a,
        vec![false, false],
        "Batch archive by A should fail for all"
    );

    // B deletes -> [true, true]
    let results_b = consumer_b
        .delete_many(vec![msg1.id, msg2.id])
        .await
        .unwrap();
    assert_eq!(
        results_b,
        vec![true, true],
        "Batch delete by B should succeed"
    );
}
