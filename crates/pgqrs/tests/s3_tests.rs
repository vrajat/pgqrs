#![cfg(feature = "s3")]

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use chrono::Utc;
use pgqrs::config::Config;
use pgqrs::store::s3::client::{build_aws_s3_client, AwsS3ClientConfig, AwsS3ObjectStore};
use pgqrs::store::s3::consistent::ConsistentDb;
use pgqrs::store::s3::snapshot::SnapshotDb;
use pgqrs::store::s3::syncstore::SyncStore;
use pgqrs::store::s3::{DurabilityMode, S3Store, SyncDb};
use pgqrs::types::{NewQueueMessage, NewQueueRecord};
use pgqrs::Store;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use uuid::Uuid;

fn endpoint_host_port() -> (String, u16) {
    let endpoint =
        std::env::var("PGQRS_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".to_string());
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(&endpoint);

    let authority = without_scheme.split('/').next().unwrap_or("localhost:4566");
    let mut parts = authority.split(':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port = parts
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(4566);
    (host, port)
}

#[test]
fn localstack_s3_health_endpoint_is_reachable() {
    let (host, port) = endpoint_host_port();
    let addr = format!("{}:{}", host, port);

    let mut stream = TcpStream::connect(&addr)
        .unwrap_or_else(|e| panic!("failed to connect to LocalStack at {}: {}", addr, e));
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(3))).ok();

    let request = format!(
        "GET /_localstack/health HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        host
    );
    stream
        .write_all(request.as_bytes())
        .expect("failed to write HTTP request to LocalStack");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("failed to read HTTP response from LocalStack");

    assert!(
        response.contains("200 OK"),
        "unexpected LocalStack health response status: {}",
        response
    );
    assert!(
        response.contains("\"s3\""),
        "LocalStack health payload does not report S3 service: {}",
        response
    );
}

fn s3_endpoint() -> String {
    env::var("PGQRS_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".to_string())
}

fn s3_region() -> String {
    env::var("PGQRS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string())
}

fn s3_bucket() -> String {
    env::var("PGQRS_S3_BUCKET").unwrap_or_else(|_| "pgqrs-test-bucket".to_string())
}

fn unique_key(prefix: &str) -> String {
    format!("smoke/{}-{}.sqlite", prefix, Uuid::new_v4())
}

fn unique_s3_dsn(prefix: &str) -> (String, String) {
    let bucket = s3_bucket();
    let key = unique_key(prefix);
    (format!("s3://{}/{}", bucket, key), key)
}

fn prepare_localstack_tls_env() {
    let endpoint = s3_endpoint();
    if endpoint.starts_with("http://") {
        std::env::remove_var("SSL_CERT_FILE");
        std::env::remove_var("SSL_CERT_DIR");
        std::env::remove_var("AWS_CA_BUNDLE");
    }
}

fn s3_config_for(dsn: &str, mode: DurabilityMode) -> Config {
    let mut cfg = Config::from_dsn(dsn);
    cfg.s3.mode = mode;
    cfg
}

async fn localstack_client() -> Client {
    prepare_localstack_tls_env();
    build_aws_s3_client(AwsS3ClientConfig {
        region: s3_region(),
        endpoint: Some(s3_endpoint()),
        access_key: Some("test".to_string()),
        secret_key: Some("test".to_string()),
        force_path_style: true,
        credentials_provider_name: "localstack",
    })
    .await
}

async fn ensure_bucket(client: &Client, bucket: &str) {
    let create_res = client.create_bucket().bucket(bucket).send().await;
    if let Err(e) = create_res {
        let msg = e.to_string();
        assert!(
            msg.contains("BucketAlreadyOwnedByYou")
                || msg.contains("BucketAlreadyExists")
                || msg.contains("OperationAborted"),
            "create_bucket failed unexpectedly: {}",
            msg
        );
    }
}

async fn delete_key(client: &Client, bucket: &str, key: &str) {
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
}

mod tables_tests {
    use super::*;

    async fn sync_store(prefix: &str) -> SyncStore<SnapshotDb> {
        let (dsn, _key) = unique_s3_dsn(prefix);
        let config = s3_config_for(&dsn, DurabilityMode::Local);
        let client = localstack_client().await;
        ensure_bucket(&client, &s3_bucket()).await;
        SyncStore::<SnapshotDb>::new(&config)
            .await
            .expect("sync store should open from config")
    }

    #[tokio::test]
    async fn tables_sqlite_queue_insert_and_get() {
        let mut store = sync_store("tables-queue").await;

        let inserted = pgqrs::tables(&store)
            .queues()
            .insert(NewQueueRecord {
                queue_name: "jobs".to_string(),
            })
            .await
            .expect("queue insert should succeed");

        let pre_sync_get = pgqrs::tables(&store).queues().get(inserted.id).await;
        assert!(
            pre_sync_get.is_err(),
            "read store should not see write-side queue before sync"
        );

        store.sync().await.expect("sync should succeed");

        let fetched = pgqrs::tables(&store)
            .queues()
            .get(inserted.id)
            .await
            .expect("queue get should succeed");
        assert_eq!(fetched.queue_name, "jobs");
    }

    #[tokio::test]
    async fn tables_sqlite_message_insert_and_count() {
        let mut store = sync_store("tables-message").await;

        let queue = pgqrs::tables(&store)
            .queues()
            .insert(NewQueueRecord {
                queue_name: "jobs".to_string(),
            })
            .await
            .expect("queue insert should succeed");

        let now = Utc::now();
        let inserted = pgqrs::tables(&store)
            .messages()
            .insert(NewQueueMessage {
                queue_id: queue.id,
                payload: serde_json::json!({ "kind": "basic" }),
                read_ct: 0,
                enqueued_at: now,
                vt: now,
                producer_worker_id: None,
                consumer_worker_id: None,
            })
            .await
            .expect("message insert should succeed");

        assert_eq!(inserted.queue_id, queue.id);
        assert_eq!(
            pgqrs::tables(&store)
                .messages()
                .count()
                .await
                .expect("count should succeed before sync"),
            0,
            "read store should not see write-side message before sync"
        );
        store.sync().await.expect("sync should succeed");
        assert_eq!(
            pgqrs::tables(&store)
                .messages()
                .count()
                .await
                .expect("count should succeed"),
            1
        );
    }

    #[tokio::test]
    async fn syncstore_normal_flow_with_explicit_refresh_calls() {
        let (dsn, _key) = unique_s3_dsn("syncstore-normal");
        let config = s3_config_for(&dsn, DurabilityMode::Local);
        let queue_name = "jobs";

        let mut sync_store = SyncStore::<SnapshotDb>::new(&config)
            .await
            .expect("sync store should open from config");

        pgqrs::admin(&sync_store)
            .create_queue(queue_name)
            .await
            .expect("queue insert should succeed");

        let producer_worker = pgqrs::producer("test-host", 9201, queue_name)
            .create(&sync_store)
            .await
            .expect("producer worker create should succeed");

        let consumer_worker = pgqrs::consumer("test-host", 9202, queue_name)
            .create(&sync_store)
            .await
            .expect("consumer worker create should succeed");

        // App chooses to refresh before doing read-side operations.
        sync_store
            .refresh()
            .await
            .expect("pre-run refresh should succeed");

        pgqrs::enqueue()
            .message(&serde_json::json!({ "job": "snapshot-doc-test" }))
            .worker(&producer_worker)
            .execute(&sync_store)
            .await
            .expect("enqueue should succeed");

        let before_sync_count = pgqrs::tables(&sync_store)
            .messages()
            .count()
            .await
            .expect("pre-sync count should not fail");
        assert!(
            before_sync_count == 0,
            "read-side message count should be zero before sync"
        );
        let mut follower_store = SyncStore::<SnapshotDb>::new(&config)
            .await
            .expect("follower sync store should open from config");
        follower_store
            .snapshot()
            .await
            .expect("follower snapshot should succeed");
        let follower_before = pgqrs::tables(&follower_store)
            .messages()
            .count()
            .await
            .expect("follower pre-sync count should not fail");
        assert_eq!(
            follower_before, 0,
            "remote object should still be unchanged before sync"
        );

        sync_store.sync().await.expect("sync should succeed");

        sync_store
            .refresh()
            .await
            .expect("post-sync refresh should succeed");

        let after_sync_count = pgqrs::tables(&sync_store)
            .messages()
            .count()
            .await
            .expect("post-sync count should not fail");
        assert_eq!(
            after_sync_count, 1,
            "read-side count should be one after sync"
        );
        follower_store
            .snapshot()
            .await
            .expect("follower snapshot should succeed");
        let follower_after = pgqrs::tables(&follower_store)
            .messages()
            .count()
            .await
            .expect("follower post-sync count should not fail");
        assert_eq!(
            follower_after, 1,
            "remote object should reflect synced message"
        );

        let after = pgqrs::dequeue()
            .worker(&consumer_worker)
            .fetch_all(&sync_store)
            .await
            .expect("post-refresh dequeue should not fail");
        assert_eq!(after.len(), 1, "dequeue should return synced message");
    }

    #[tokio::test]
    async fn consistentdb_normal_flow_without_explicit_sync_or_refresh() {
        let (dsn, _key) = unique_s3_dsn("consistent-normal");
        let config = s3_config_for(&dsn, DurabilityMode::Local);
        let queue_name = "jobs";

        let consistent_store = SyncStore::<ConsistentDb>::new(&config)
            .await
            .expect("consistent store should open from config");

        pgqrs::admin(&consistent_store)
            .create_queue(queue_name)
            .await
            .expect("queue insert should succeed");

        let producer_worker = pgqrs::producer("test-host", 9301, queue_name)
            .create(&consistent_store)
            .await
            .expect("producer worker create should succeed");

        let consumer_worker = pgqrs::consumer("test-host", 9302, queue_name)
            .create(&consistent_store)
            .await
            .expect("consumer worker create should succeed");

        let before_count = pgqrs::tables(&consistent_store)
            .messages()
            .count()
            .await
            .expect("pre-enqueue count should not fail");
        assert_eq!(before_count, 0, "read-side count should start at zero");

        let queue = pgqrs::tables(&consistent_store)
            .queues()
            .get_by_name(queue_name)
            .await
            .expect("queue should exist");
        pgqrs::tables(&consistent_store)
            .messages()
            .insert(NewQueueMessage {
                queue_id: queue.id,
                payload: serde_json::json!({ "job": "consistent-doc-test" }),
                read_ct: 0,
                enqueued_at: Utc::now(),
                vt: Utc::now(),
                producer_worker_id: Some(producer_worker.worker_id()),
                consumer_worker_id: None,
            })
            .await
            .expect("message insert should succeed");

        let after_count = pgqrs::tables(&consistent_store)
            .messages()
            .count()
            .await
            .expect("post-enqueue count should not fail");
        assert_eq!(
            after_count, 1,
            "read-side count should be one after enqueue"
        );

        let mut follower_store = SyncStore::<SnapshotDb>::new(&config)
            .await
            .expect("follower sync store should open from config");
        follower_store
            .snapshot()
            .await
            .expect("follower snapshot should succeed");
        let source_after = pgqrs::tables(&follower_store)
            .messages()
            .count()
            .await
            .expect("follower count should not fail");
        assert_eq!(
            source_after, 1,
            "remote object should be synced automatically"
        );

        let after = pgqrs::dequeue()
            .worker(&consumer_worker)
            .fetch_all(&consistent_store)
            .await
            .expect("dequeue should not fail");
        assert_eq!(after.len(), 1, "dequeue should return synced message");
    }

    #[tokio::test]
    async fn consistentdb_sequences_reads_and_writes_under_contention() {
        let (dsn, _key) = unique_s3_dsn("consistent-lock");
        let config = s3_config_for(&dsn, DurabilityMode::Local);

        let store = SyncStore::<ConsistentDb>::new(&config)
            .await
            .expect("consistent store should open from config");

        // Acquire ConsistentDb write lock and hold it until explicitly released.
        let db = store.db().clone();
        let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let hold_task = tokio::spawn(async move {
            db.with_write(|_| {
                Box::pin(async move {
                    let _ = held_tx.send(());
                    release_rx
                        .await
                        .expect("release signal should arrive while lock is held");
                    Ok(())
                })
            })
            .await
            .expect("lock-holding write should succeed");
        });

        held_rx.await.expect("write lock should be held");

        // Launch read and write together; both should block while write lock is held.
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let (read_done_tx, mut read_done_rx) = tokio::sync::oneshot::channel::<()>();
        let (write_done_tx, mut write_done_rx) = tokio::sync::oneshot::channel::<()>();

        let read_store = store.clone();
        let read_barrier = barrier.clone();
        let read_task = tokio::spawn(async move {
            read_barrier.wait().await;
            pgqrs::tables(&read_store)
                .messages()
                .count()
                .await
                .expect("count should succeed");
            let _ = read_done_tx.send(());
        });

        let write_store = store.clone();
        let write_barrier = barrier.clone();
        let write_task = tokio::spawn(async move {
            write_barrier.wait().await;
            pgqrs::tables(&write_store)
                .queues()
                .insert(NewQueueRecord {
                    queue_name: format!("blocked-write-{}", Uuid::new_v4()),
                })
                .await
                .expect("queue insert should succeed after lock release");
            let _ = write_done_tx.send(());
        });

        // Release both contenders at the same time.
        barrier.wait().await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(
            !read_task.is_finished(),
            "read should be blocked while write lock is held"
        );
        assert!(
            !write_task.is_finished(),
            "write should be blocked while write lock is held"
        );

        let _ = release_tx.send(());
        hold_task
            .await
            .expect("lock-holding task should complete cleanly");
        (&mut read_done_rx)
            .await
            .expect("read completion signal should arrive");
        (&mut write_done_rx)
            .await
            .expect("write completion signal should arrive");
        read_task.await.expect("read task should complete");
        write_task.await.expect("write task should complete");
    }
}

#[tokio::test]
async fn localstack_s3_basic_ops_and_cas_etag() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    let key = format!("smoke/etag-cas-{}.bin", uuid::Uuid::new_v4());

    ensure_bucket(&client, &bucket).await;

    let put_v1 = client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from_static(b"payload-v1"))
        .send()
        .await
        .expect("put v1 should succeed");

    let etag_v1 = put_v1
        .e_tag()
        .expect("put v1 should return etag")
        .to_string();

    let head_v1 = client
        .head_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .expect("head should succeed");
    let head_etag_v1 = head_v1
        .e_tag()
        .expect("head should include etag")
        .to_string();
    assert_eq!(etag_v1, head_etag_v1, "head etag should match put etag");

    let get_v1 = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .expect("get should succeed");
    let bytes_v1 = get_v1.body.collect().await.expect("read body").into_bytes();
    assert_eq!(bytes_v1.as_ref(), b"payload-v1");

    let put_v2 = client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .if_match(etag_v1.clone())
        .body(ByteStream::from_static(b"payload-v2"))
        .send()
        .await
        .expect("CAS put with current etag should succeed");
    let etag_v2 = put_v2
        .e_tag()
        .expect("put v2 should return etag")
        .to_string();
    assert_ne!(etag_v1, etag_v2, "etag should change after update");

    let _get_ok = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .if_match(etag_v2)
        .send()
        .await
        .expect("conditional get with latest etag should succeed");

    let err = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .if_match(etag_v1)
        .send()
        .await
        .expect_err("conditional get with stale etag should fail");
    let _ = err;

    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_aws_adapter_round_trip() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    let key = format!("smoke/adapter-{}.bin", uuid::Uuid::new_v4());
    let adapter = AwsS3ObjectStore::new(client.clone(), bucket.clone());

    ensure_bucket(&client, &bucket).await;

    let etag_v1 = adapter
        .put_object_if_match(&key, b"adapter-v1", None)
        .await
        .expect("adapter put v1");
    let obj_v1 = adapter.get_object(&key).await.expect("adapter get v1");
    assert_eq!(obj_v1.bytes, b"adapter-v1");
    assert_eq!(obj_v1.etag.as_deref(), Some(etag_v1.as_str()));

    let _etag_v2 = adapter
        .put_object_if_match(&key, b"adapter-v2", Some(&etag_v1))
        .await
        .expect("adapter cas put v2");
    let obj_v2 = adapter.get_object(&key).await.expect("adapter get v2");
    assert_eq!(obj_v2.bytes, b"adapter-v2");

    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_open_and_queue_lifecycle() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("store-open");
    let config = s3_config_for(&dsn, DurabilityMode::Local);
    let store = S3Store::new(&config).await.expect("open should succeed");

    assert_eq!(store.mode(), DurabilityMode::Local);
    assert_eq!(store.backend_name(), "s3");

    pgqrs::admin(&store)
        .create_queue("jobs-open")
        .await
        .expect("queue creation should succeed");

    let producer = pgqrs::producer("host", 9801, "jobs-open")
        .create(&store)
        .await
        .expect("producer create should succeed");

    let consumer = pgqrs::consumer("host", 9802, "jobs-open")
        .create(&store)
        .await
        .expect("consumer create should succeed");

    pgqrs::enqueue()
        .message(&serde_json::json!({ "job": "smoke" }))
        .worker(&producer)
        .execute(&store)
        .await
        .expect("enqueue should succeed");

    let popped = pgqrs::dequeue()
        .worker(&consumer)
        .fetch_all(&store)
        .await
        .expect("dequeue should succeed");

    assert_eq!(
        popped.len(),
        1,
        "message enqueued through S3Store should be visible to consumer"
    );
    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_bootstrap_restores_seeded_queue() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("store-bootstrap-seed");
    let config = s3_config_for(&dsn, DurabilityMode::Local);

    {
        let seed_store = S3Store::new(&config).await.expect("seed store should open");
        pgqrs::admin(&seed_store)
            .create_queue("seeded")
            .await
            .expect("seed queue should be created");

        let producer = pgqrs::producer("seed-host", 9811, "seeded")
            .create(&seed_store)
            .await
            .expect("producer create should succeed");

        pgqrs::enqueue()
            .message(&serde_json::json!({ "job": "seeded" }))
            .worker(&producer)
            .execute(&seed_store)
            .await
            .expect("seed enqueue should succeed");
    }

    let store = S3Store::new(&config).await.expect("store should open");
    store.bootstrap().await.expect("bootstrap should succeed");

    let exists = pgqrs::tables(&store)
        .queues()
        .exists("seeded")
        .await
        .expect("queue existence check should succeed");
    assert!(exists, "seeded queue should be restored after bootstrap");

    let count = pgqrs::tables(&store)
        .messages()
        .count()
        .await
        .expect("message count should succeed");
    assert!(
        count > 0,
        "seeded queue should include messages after bootstrap"
    );

    let consumer = pgqrs::consumer("seed-host", 9812, "seeded")
        .create(&store)
        .await
        .expect("consumer create should succeed");
    let popped = pgqrs::dequeue()
        .worker(&consumer)
        .fetch_all(&store)
        .await
        .expect("dequeue should succeed after bootstrap");
    assert!(
        !popped.is_empty(),
        "bootstraped queue should yield messages"
    );

    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_compatibility_aliases() {
    let (dsn, key) = unique_s3_dsn("store-sync-refresh");
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let config = s3_config_for(&dsn, DurabilityMode::Durable);
    let mut store = S3Store::new(&config).await.expect("open should succeed");

    pgqrs::admin(&store)
        .create_queue("stable")
        .await
        .expect("queue create should succeed");

    store
        .snapshot()
        .await
        .expect("snapshot should succeed as compatibility alias");
    store
        .sync()
        .await
        .expect("sync should succeed as compatibility alias");
    store
        .refresh()
        .await
        .expect("refresh should succeed as compatibility alias");

    let follower = S3Store::new(&config).await.expect("follower should open");
    follower
        .bootstrap()
        .await
        .expect("follower bootstrap should succeed");
    let queues = pgqrs::tables(&follower)
        .queues()
        .exists("stable")
        .await
        .expect("queue existence check should succeed");
    assert!(queues);

    delete_key(&client, &bucket, &key).await;
}
