use pgqrs::{store::Store, types::WorkerStatus};
use serial_test::serial;

mod common;

#[tokio::test]
#[serial]
async fn test_zombie_lifecycle_and_reclamation() -> anyhow::Result<()> {
    #[cfg(feature = "s3")]
    {
        if common::current_backend() == pgqrs::store::BackendType::S3 {
            eprintln!("Skipping test: not supported on S3 backend");
            return Ok(());
        }
    }
    let queue_name = "zombie-queue";
    let schema = "pgqrs_zombie_tests";
    // 1-8. Phase 1: Library-based Zombie Reclamation
    let (queue_id, consumer_worker_id, producer_id, c2_id, dsn_str) = {
        let store = common::create_store(schema).await;
        // Capture DSN to return it
        let dsn = store.config().dsn.clone();

        // Create admin via builder
        pgqrs::admin(&store).install().await?;

        // 2. Create Queue
        let queue = store.queue(queue_name).await?;

        // 3. Create Producer & Enqueue
        let producer = pgqrs::producer("producer-host-1001", queue_name)
            .create(&store)
            .await?;

        let payload = serde_json::json!({"task": "brains"});
        let msg_ids = pgqrs::enqueue()
            .message(&payload)
            .worker(&producer)
            .execute(&store)
            .await?;
        let msg_id = msg_ids[0];
        // We can't easily verify payload without fetching, so trust msg_id returned.
        // Or fetch it.
        let stored_msg = pgqrs::tables(&store).messages().get(msg_id).await?;
        assert_eq!(stored_msg.payload, payload);

        // 4. Create Consumer & Dequeue
        let consumer = pgqrs::consumer("consumer-host-2001", queue_name)
            .create(&store)
            .await?;

        let dequeued_messages = pgqrs::dequeue().worker(&consumer).fetch_all(&store).await?;
        assert_eq!(dequeued_messages.len(), 1);
        let locked_msg = &dequeued_messages[0];
        assert_eq!(locked_msg.id, msg_id);

        // 5. Simulate Zombie (Update Heartbeat)
        // Manually set the worker's heartbeat to be old (e.g., 1 hour ago)
        let consumer_worker_id = consumer.worker_id();

        let update_sql = match common::current_backend() {
            #[cfg(feature = "postgres")]
            pgqrs::store::BackendType::Postgres => "UPDATE pgqrs_workers SET heartbeat_at = NOW() - $1 * INTERVAL '1 second' WHERE id = $2",
            #[cfg(feature = "s3")]
            pgqrs::store::BackendType::S3 => "UPDATE pgqrs_workers SET heartbeat_at = datetime('now', '-' || ? || ' seconds') WHERE id = ?",
            #[cfg(feature = "sqlite")]
            pgqrs::store::BackendType::Sqlite => "UPDATE pgqrs_workers SET heartbeat_at = datetime('now', '-' || ? || ' seconds') WHERE id = ?",
            #[cfg(feature = "turso")]
            pgqrs::store::BackendType::Turso => "UPDATE pgqrs_workers SET heartbeat_at = datetime('now', '-' || ? || ' seconds') WHERE id = ?",
        };

        store
            .execute_raw_with_two_i64(update_sql, 3600, consumer_worker_id)
            .await?;

        // 6. Test pgqrs_workers functions
        // Use pgqrs::tables(&store).workers()
        // Verify it's counted as a zombie
        let zombie_count = pgqrs::tables(&store)
            .workers()
            .count_zombies_for_queue(queue.id, chrono::Duration::seconds(60))
            .await?;
        // We intentionally violate encapsulation here for the test setup
        assert_eq!(zombie_count, 1, "Should detect 1 zombie worker");

        let zombies = pgqrs::tables(&store)
            .workers()
            .list_zombies_for_queue(queue.id, chrono::Duration::seconds(60))
            .await?;
        assert_eq!(zombies.len(), 1);
        assert_eq!(zombies[0].id, consumer_worker_id);

        // 7. Test admin.reclaim_messages
        let reclaimed = pgqrs::admin(&store)
            .reclaim_messages(queue.id, Some(chrono::Duration::seconds(60)))
            .await?;
        assert_eq!(reclaimed, 1, "Should reclaim 1 message");

        // Verify message is released
        let stored_msg = pgqrs::tables(&store).messages().get(msg_id).await?;
        assert_eq!(
            stored_msg.consumer_worker_id, None,
            "Message should have no consumer"
        );
        assert_eq!(stored_msg.read_ct, 1, "Read count should be preserved at 1");
        // Verify worker is stopped
        let updated_worker = pgqrs::tables(&store)
            .workers()
            .get(consumer_worker_id)
            .await?;
        assert!(
            matches!(updated_worker.status, WorkerStatus::Stopped),
            "Worker should be stopped"
        );

        // 8. Test reconnect-based reclamation preparation.
        // We'll reset the state and reclaim from a fresh store handle.

        // Reset: Make message owned by a new zombie
        // Use a new consumer for the next test phase
        let consumer_2 = pgqrs::consumer("consumer-2-2002", queue_name)
            .create(&store)
            .await?;

        // Dequeue again (should get the released message)
        let msgs_2 = pgqrs::dequeue()
            .worker(&consumer_2)
            .fetch_all(&store)
            .await?;
        assert_eq!(msgs_2.len(), 1);
        assert_eq!(msgs_2[0].id, msg_id);

        // Make consumer_2 a zombie
        let c2_id = consumer_2.worker_id();
        store
            .execute_raw_with_two_i64(update_sql, 3600, c2_id)
            .await?;

        #[cfg(any(feature = "sqlite", feature = "turso"))]
        {
            let mut needs_checkpoint = false;
            let backend = common::current_backend();

            #[cfg(feature = "sqlite")]
            if backend == pgqrs::store::BackendType::Sqlite {
                needs_checkpoint = true;
            }

            #[cfg(feature = "s3")]
            if backend == pgqrs::store::BackendType::S3 {
                needs_checkpoint = true;
            }

            #[cfg(feature = "turso")]
            if backend == pgqrs::store::BackendType::Turso {
                needs_checkpoint = true;
            }

            if needs_checkpoint {
                let _ = store.execute_raw("PRAGMA wal_checkpoint(TRUNCATE)").await;
            }
        }

        (
            queue.id,
            consumer_worker_id,
            producer.worker_id(),
            c2_id,
            dsn,
        )
    }; // All connections closed here

    // 9. Reconnect with a fresh store handle and reclaim again.
    assert!(!dsn_str.is_empty());
    {
        let config = pgqrs::config::Config::from_dsn_with_schema(&dsn_str, schema)
            .expect("failed to build config from reclaimed DSN");
        let store = pgqrs::connect_with_config(&config)
            .await
            .expect("Failed to reconnect");
        let reclaimed = pgqrs::admin(&store)
            .reclaim_messages(queue_id, Some(chrono::Duration::seconds(60)))
            .await?;
        assert_eq!(reclaimed, 1, "reconnect-based reclaim should succeed");

        let c2_worker = pgqrs::tables(&store).workers().get(c2_id).await?;
        assert!(
            matches!(c2_worker.status, WorkerStatus::Stopped),
            "Consumer 2 should be stopped by reclaim_messages"
        );

        // Cleanup
        pgqrs::admin(&store).purge_queue(queue_name).await?;
        pgqrs::admin(&store).delete_worker(producer_id).await?;
        pgqrs::admin(&store)
            .delete_worker(consumer_worker_id)
            .await?; // consumer 1
        pgqrs::admin(&store).delete_worker(c2_id).await?; // consumer 2
        let queue_info = pgqrs::tables(&store).queues().get(queue_id).await?;
        pgqrs::admin(&store).delete_queue(&queue_info).await?;
    }

    Ok(())
}
