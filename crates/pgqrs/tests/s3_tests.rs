#![cfg(feature = "s3")]

mod common;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use chrono::Utc;
use pgqrs::store::s3::DurabilityMode;
use pgqrs::store::AnyStore;
use pgqrs::types::NewQueueMessage;
use pgqrs::Store;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn endpoint_host_port() -> (String, u16) {
    let endpoint =
        std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".to_string());
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
    env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".to_string())
}

fn s3_region() -> String {
    env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

fn s3_bucket() -> String {
    env::var("PGQRS_S3_BUCKET").unwrap_or_else(|_| "pgqrs-test-bucket".to_string())
}

fn prepare_localstack_tls_env() {
    let endpoint = s3_endpoint();
    if endpoint.starts_with("http://") {
        std::env::remove_var("SSL_CERT_FILE");
        std::env::remove_var("SSL_CERT_DIR");
        std::env::remove_var("AWS_CA_BUNDLE");
    }
}

async fn create_s3_store_for_test(schema: &str, mode: DurabilityMode) -> AnyStore {
    let base_store = common::create_store_with_config(schema, |cfg| {
        cfg.s3.mode = mode;
    })
    .await;
    assert_eq!(
        base_store.backend_name(),
        "s3",
        "S3 test expected AnyStore::S3 backend"
    );

    base_store
}

fn store_mode(store: &AnyStore) -> DurabilityMode {
    match store {
        AnyStore::S3(store) => store.mode(),
        _ => panic!("Expected AnyStore::S3 for s3 tests"),
    }
}

async fn s3_sync_store(store: &mut AnyStore) -> pgqrs::error::Result<()> {
    match store {
        AnyStore::S3(store) => store.sync().await,
        _ => panic!("Expected AnyStore::S3 for s3 tests"),
    }
}

async fn s3_snapshot_store(store: &mut AnyStore) -> pgqrs::error::Result<()> {
    match store {
        AnyStore::S3(store) => store.snapshot().await,
        _ => panic!("Expected AnyStore::S3 for s3 tests"),
    }
}

async fn localstack_client() -> Client {
    prepare_localstack_tls_env();
    let endpoint = s3_endpoint();
    let region = s3_region();
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region))
        .credentials_provider(Credentials::new("test", "test", None, None, "localstack"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    Client::from_conf(config)
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

fn parse_s3_bucket_key(dsn: &str) -> (String, String) {
    let full = dsn.strip_prefix("s3://").unwrap_or(dsn);
    let mut parts = full.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default().trim().to_string();
    let key = parts.next().unwrap_or_default().trim().to_string();
    (bucket, key)
}

async fn wait_for_head_object(client: &Client, bucket: &str, key: &str) -> bool {
    for _ in 0..10 {
        if client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .is_ok()
        {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    false
}

mod tables_tests {
    use super::*;

    #[tokio::test]
    async fn consistentdb_normal_flow_without_explicit_sync_or_refresh() {
        let base_store = create_s3_store_for_test(
            "consistentdb_normal_flow_without_explicit_sync_or_refresh",
            DurabilityMode::Durable,
        )
        .await;
        let consistent_store = base_store.clone();
        let queue_name = "jobs";

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

        let mut follower_cfg = base_store.config().clone();
        follower_cfg.s3.mode = DurabilityMode::Local;
        let mut follower_store = pgqrs::connect_with_config(&follower_cfg)
            .await
            .expect("follower sync store should open from config");
        s3_snapshot_store(&mut follower_store)
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
        let store = create_s3_store_for_test(
            "consistentdb_sequences_reads_and_writes_under_contention",
            DurabilityMode::Durable,
        )
        .await;
        pgqrs::admin(&store)
            .create_queue("jobs")
            .await
            .expect("queue creation should succeed");
        let producer = pgqrs::producer("test-host", 9303, "jobs")
            .create(&store)
            .await
            .expect("producer should be created");

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let write_store = store.clone();
        let write_barrier = barrier.clone();
        let producer_for_task = producer.clone();
        let write_task = tokio::spawn(async move {
            write_barrier.wait().await;
            pgqrs::enqueue()
                .message(&serde_json::json!({ "job": "concurrency" }))
                .worker(&producer_for_task)
                .execute(&write_store)
                .await
                .expect("enqueue should succeed");
        });

        let read_store = store.clone();
        let read_barrier = barrier.clone();
        let read_task = tokio::spawn(async move {
            read_barrier.wait().await;
            pgqrs::tables(&read_store)
                .messages()
                .count()
                .await
                .expect("count should succeed");
        });

        barrier.wait().await;
        write_task.await.expect("write task should complete");
        read_task.await.expect("read task should complete");

        let final_count = pgqrs::tables(&store)
            .messages()
            .count()
            .await
            .expect("final count should succeed");
        assert_eq!(final_count, 1);
    }
}

mod snapshot_db_tests {
    use super::*;

    #[tokio::test]
    async fn s3_bootstrap_creates_remote_state_when_key_is_missing() {
        let client = localstack_client().await;
        let dsn =
            common::get_test_dsn("s3_bootstrap_creates_remote_state_when_key_is_missing").await;
        let (bucket, key) = parse_s3_bucket_key(&dsn);
        ensure_bucket(&client, &bucket).await;
        delete_key(&client, &bucket, &key).await;

        let mut config = pgqrs::config::Config::from_dsn_with_schema(&dsn, "s3_bootstrap").unwrap();
        config.s3.mode = DurabilityMode::Local;
        let mut store = pgqrs::connect_with_config(&config)
            .await
            .expect("store should open");

        store
            .bootstrap()
            .await
            .expect("bootstrap should initialize local schema");
        s3_sync_store(&mut store)
            .await
            .expect("sync should publish missing remote state");

        let exists = wait_for_head_object(&client, &bucket, &key).await;
        assert!(
            exists,
            "sync should publish missing remote state in LocalStack-backed bootstrap flow"
        );
    }

    #[tokio::test]
    async fn s3_bootstrap_restores_existing_remote_state() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let seed_store = create_s3_store_for_test(
            "s3_bootstrap_restores_existing_remote_state_seed",
            DurabilityMode::Local,
        )
        .await;
        let mut seed_store_writer = seed_store.clone();
        let seed_config = seed_store.config().clone();
        seed_store_writer
            .bootstrap()
            .await
            .expect("seed bootstrap should initialize local schema");

        pgqrs::admin(&seed_store_writer)
            .create_queue("seeded")
            .await
            .expect("seed queue should be created");
        let producer = pgqrs::producer("seed-host", 9821, "seeded")
            .create(&seed_store_writer)
            .await
            .expect("seed producer should be created");
        s3_sync_store(&mut seed_store_writer)
            .await
            .expect("seed sync should publish schema and worker state before enqueue");
        pgqrs::enqueue()
            .message(&serde_json::json!({ "job": "seeded" }))
            .worker(&producer)
            .execute(&seed_store_writer)
            .await
            .expect("seed enqueue should succeed");
        s3_sync_store(&mut seed_store_writer)
            .await
            .expect("seed sync should succeed");

        let mut reopened = pgqrs::connect_with_config(&seed_config)
            .await
            .expect("store should open for restore");
        s3_snapshot_store(&mut reopened)
            .await
            .expect("snapshot should restore from remote");

        let restored = pgqrs::tables(&reopened)
            .queues()
            .exists("seeded")
            .await
            .expect("queue existence check should succeed");
        assert!(restored, "seeded queue should be restored after bootstrap");
        let count = pgqrs::tables(&reopened)
            .messages()
            .count()
            .await
            .expect("message count should succeed");
        assert!(
            count > 0,
            "seeded messages should be restored after bootstrap"
        );

        let mut replacement_writer = pgqrs::connect_with_config(&seed_config)
            .await
            .expect("replacement store should open against seeded config");
        replacement_writer
            .bootstrap()
            .await
            .expect("bootstrap should still initialize the local replacement store");
        let sync_err = s3_sync_store(&mut replacement_writer)
            .await
            .expect_err("fresh store should not overwrite existing remote state");
        assert!(
            matches!(sync_err, pgqrs::error::Error::Conflict { .. }),
            "replacement sync should fail with conflict, got: {sync_err}"
        );
    }

    #[tokio::test]
    async fn s3_bootstrap_is_idempotent() {
        let client = localstack_client().await;
        let dsn = common::get_test_dsn("s3_bootstrap_is_idempotent").await;
        let (bucket, key) = parse_s3_bucket_key(&dsn);
        ensure_bucket(&client, &bucket).await;
        delete_key(&client, &bucket, &key).await;

        let mut config = pgqrs::config::Config::from_dsn_with_schema(&dsn, "s3_bootstrap").unwrap();
        config.s3.mode = DurabilityMode::Local;
        let mut store = pgqrs::connect_with_config(&config)
            .await
            .expect("store should open");
        store
            .bootstrap()
            .await
            .expect("initial bootstrap should initialize local schema");
        s3_sync_store(&mut store)
            .await
            .expect("initial sync should succeed");

        let before = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should exist after bootstrap")
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default();

        s3_sync_store(&mut store)
            .await
            .expect("second sync should be idempotent");
        s3_sync_store(&mut store)
            .await
            .expect("third sync should be idempotent");

        let after = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should still exist after repeated bootstrap")
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default();

        assert_eq!(
            before, after,
            "etag should remain stable for repeated bootstrap on unchanged state"
        );
    }

    #[tokio::test]
    async fn s3_bootstrap_recovers_when_remote_state_deleted() {
        let client = localstack_client().await;
        let dsn = common::get_test_dsn("s3_bootstrap_recovers_when_remote_state_deleted").await;
        let (bucket, key) = parse_s3_bucket_key(&dsn);
        ensure_bucket(&client, &bucket).await;

        let mut config = pgqrs::config::Config::from_dsn_with_schema(&dsn, "s3_bootstrap").unwrap();
        config.s3.mode = DurabilityMode::Local;
        let mut seed_store = pgqrs::connect_with_config(&config)
            .await
            .expect("seed store should open");
        seed_store
            .bootstrap()
            .await
            .expect("seed bootstrap should initialize local schema");
        s3_sync_store(&mut seed_store)
            .await
            .expect("seed sync should publish initial remote state");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should exist after bootstrap");
        assert!(head.e_tag().is_some(), "remote state should include etag");

        delete_key(&client, &bucket, &key).await;

        pgqrs::admin(&seed_store)
            .create_queue("recreated")
            .await
            .expect("local mutation after remote delete should succeed");
        s3_sync_store(&mut seed_store)
            .await
            .expect("dirty sync should recreate missing remote state");
        assert!(
            wait_for_head_object(&client, &bucket, &key).await,
            "dirty sync should recreate missing state object"
        );
    }

    #[tokio::test]
    async fn s3_bootstrap_rejects_invalid_s3_dsn() {
        let mut config = pgqrs::config::Config::from_dsn_with_schema(
            "s3://invalid-s3-dsn-no-key",
            "s3_bootstrap",
        )
        .unwrap();
        config.s3.mode = DurabilityMode::Local;
        let result = pgqrs::connect_with_config(&config).await;
        assert!(
            result.is_err(),
            "connect or bootstrap should fail for invalid S3 DSN"
        );
    }

    fn cache_dir_for_dsn(dsn: &str) -> std::path::PathBuf {
        let bootstrap_dsn = pgqrs::store::s3::S3Store::sqlite_cache_dsn_from_s3_dsn(dsn)
            .expect("cache dsn should be derivable");
        let path_str = bootstrap_dsn
            .strip_prefix("sqlite://")
            .and_then(|s| s.strip_suffix("?mode=rwc"))
            .expect("cache dsn should be sqlite://...?... format");
        std::path::Path::new(path_str)
            .parent()
            .expect("bootstrap sqlite path should have parent")
            .to_path_buf()
    }

    fn non_bootstrap_sqlite_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut files = std::fs::read_dir(dir)
            .expect("cache dir should exist")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.extension().is_some_and(|ext| ext == "sqlite")
                    && path
                        .file_name()
                        .is_some_and(|name| name != "bootstrap.sqlite")
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[tokio::test]
    async fn given_matching_local_and_remote_etag_when_snapshot_then_returns_without_rewrite() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store = create_s3_store_for_test(
            "snapshot_db_matching_etag_is_idempotent",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("seeded")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("sync should publish initial remote state");

        let cache_dir = cache_dir_for_dsn(&store.config().dsn);
        let revision_files = non_bootstrap_sqlite_files(&cache_dir);
        assert_eq!(
            revision_files.len(),
            1,
            "expected exactly one revision sqlite file after initial sync"
        );
        let revision_path = &revision_files[0];
        let before_modified = std::fs::metadata(revision_path)
            .expect("revision file should exist")
            .modified()
            .expect("revision mtime should be readable");

        tokio::time::sleep(Duration::from_millis(25)).await;

        s3_snapshot_store(&mut store)
            .await
            .expect("snapshot with matching etag should succeed");

        let after_files = non_bootstrap_sqlite_files(&cache_dir);
        assert_eq!(
            after_files.len(),
            1,
            "snapshot should not materialize a new revision file when etag matches"
        );
        let after_modified = std::fs::metadata(&after_files[0])
            .expect("revision file should still exist")
            .modified()
            .expect("revision mtime should be readable");
        assert_eq!(
            before_modified, after_modified,
            "snapshot should not rewrite the local revision file when etag matches"
        );
    }

    #[tokio::test]
    async fn given_dirty_local_state_when_snapshot_then_returns_conflict() {
        let mut store = create_s3_store_for_test(
            "snapshot_db_dirty_snapshot_conflicts",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("dirty")
            .await
            .expect("queue creation should succeed");

        let err = s3_snapshot_store(&mut store)
            .await
            .expect_err("snapshot should fail when local state is dirty");
        assert!(
            matches!(err, pgqrs::error::Error::Conflict { .. }),
            "expected Conflict for dirty snapshot, got: {err}"
        );
    }

    #[tokio::test]
    async fn given_clean_local_state_when_sync_then_returns_without_remote_change() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store =
            create_s3_store_for_test("snapshot_db_clean_sync_is_noop", DurabilityMode::Local).await;

        pgqrs::admin(&store)
            .create_queue("seeded")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("initial sync should publish remote state");

        let (bucket, key) = parse_s3_bucket_key(&store.config().dsn);
        let before = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should exist after initial sync")
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default();

        let cache_dir = cache_dir_for_dsn(&store.config().dsn);
        let revision_files = non_bootstrap_sqlite_files(&cache_dir);
        assert_eq!(
            revision_files.len(),
            1,
            "expected exactly one revision sqlite file after initial sync"
        );
        let revision_path = &revision_files[0];
        let before_modified = std::fs::metadata(revision_path)
            .expect("revision file should exist")
            .modified()
            .expect("revision mtime should be readable");

        tokio::time::sleep(Duration::from_millis(25)).await;

        s3_sync_store(&mut store)
            .await
            .expect("clean sync should be a no-op");

        let after = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should still exist after no-op sync")
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default();
        assert_eq!(before, after, "clean sync should not change remote etag");

        let after_modified = std::fs::metadata(revision_path)
            .expect("revision file should still exist")
            .modified()
            .expect("revision mtime should be readable");
        assert_eq!(
            before_modified, after_modified,
            "clean sync should not rewrite the local revision file"
        );
    }

    #[tokio::test]
    async fn given_deleted_remote_object_and_clean_local_state_when_sync_then_returns_without_recreating_remote_state(
    ) {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store = create_s3_store_for_test(
            "snapshot_db_deleted_remote_clean_sync_stays_noop",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("seeded")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("initial sync should publish remote state");

        let (bucket, key) = parse_s3_bucket_key(&store.config().dsn);
        delete_key(&client, &bucket, &key).await;

        s3_sync_store(&mut store)
            .await
            .expect("clean sync should remain a no-op even when remote is missing");

        assert!(
            !wait_for_head_object(&client, &bucket, &key).await,
            "clean sync should not recreate deleted remote state"
        );
    }

    #[tokio::test]
    async fn given_dirty_local_state_when_sync_then_publishes_remote_state() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store =
            create_s3_store_for_test("snapshot_db_dirty_sync_publishes", DurabilityMode::Local)
                .await;

        pgqrs::admin(&store)
            .create_queue("published")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("sync should publish initial remote state");

        let follower_config = store.config().clone();
        let mut follower = pgqrs::connect_with_config(&follower_config)
            .await
            .expect("follower should open");
        s3_snapshot_store(&mut follower)
            .await
            .expect("follower snapshot should succeed");
        assert!(
            pgqrs::tables(&follower)
                .queues()
                .exists("published")
                .await
                .expect("follower queue existence check should succeed"),
            "dirty sync should publish queue to remote state"
        );
    }

    #[tokio::test]
    async fn given_deleted_remote_object_and_dirty_local_state_when_sync_then_recreates_remote_state(
    ) {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store = create_s3_store_for_test(
            "snapshot_db_deleted_remote_dirty_sync_recreates",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("seeded")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("initial sync should publish remote state");

        let (bucket, key) = parse_s3_bucket_key(&store.config().dsn);
        delete_key(&client, &bucket, &key).await;

        pgqrs::admin(&store)
            .create_queue("recreated")
            .await
            .expect("local mutation after remote delete should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("dirty sync should recreate missing remote state");

        assert!(
            wait_for_head_object(&client, &bucket, &key).await,
            "dirty sync should recreate deleted remote state"
        );

        let mut follower = pgqrs::connect_with_config(store.config())
            .await
            .expect("follower should open");
        s3_snapshot_store(&mut follower)
            .await
            .expect("follower snapshot should succeed after recreation");
        assert!(
            pgqrs::tables(&follower)
                .queues()
                .exists("recreated")
                .await
                .expect("follower queue existence check should succeed"),
            "recreated remote state should include latest dirty write"
        );
    }

    #[tokio::test]
    async fn given_successful_sync_when_remote_head_is_checked_then_etag_advances_and_dirty_state_is_cleared(
    ) {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store = create_s3_store_for_test(
            "snapshot_db_sync_advances_etag_and_clears_dirty",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("first")
            .await
            .expect("initial queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("initial sync should publish remote state");

        let (bucket, key) = parse_s3_bucket_key(&store.config().dsn);
        let before = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should exist after initial sync")
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default();

        pgqrs::admin(&store)
            .create_queue("second")
            .await
            .expect("second queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("dirty sync should publish updated remote state");

        let after = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .expect("remote state should exist after second sync")
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default();
        assert_ne!(before, after, "successful sync should advance remote etag");

        s3_snapshot_store(&mut store)
            .await
            .expect("snapshot after successful sync should succeed once dirty state is cleared");
    }

    #[tokio::test]
    async fn given_successful_sync_when_local_revision_is_reopened_then_latest_state_remains_queryable(
    ) {
        let mut store = create_s3_store_for_test(
            "snapshot_db_successful_sync_reopened_revision_queryable",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("queryable")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("sync should reopen to the latest local revision");

        assert!(
            pgqrs::tables(&store)
                .queues()
                .exists("queryable")
                .await
                .expect("queue existence check should succeed after successful sync"),
            "latest local revision should remain queryable after sync reopen"
        );
    }

    #[tokio::test]
    async fn given_successful_sync_when_snapshot_follows_then_snapshot_returns_without_change() {
        let mut store = create_s3_store_for_test(
            "snapshot_db_sync_then_snapshot_is_idempotent",
            DurabilityMode::Local,
        )
        .await;

        pgqrs::admin(&store)
            .create_queue("stable")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("sync should publish remote state");

        let cache_dir = cache_dir_for_dsn(&store.config().dsn);
        let revision_files = non_bootstrap_sqlite_files(&cache_dir);
        assert_eq!(
            revision_files.len(),
            1,
            "expected exactly one revision sqlite file after sync"
        );
        let revision_path = &revision_files[0];
        let before_modified = std::fs::metadata(revision_path)
            .expect("revision file should exist")
            .modified()
            .expect("revision mtime should be readable");

        tokio::time::sleep(Duration::from_millis(25)).await;

        s3_snapshot_store(&mut store)
            .await
            .expect("snapshot after successful sync should be a no-op");

        let after_files = non_bootstrap_sqlite_files(&cache_dir);
        assert_eq!(
            after_files.len(),
            1,
            "snapshot should not create a new revision file when sync already matches remote"
        );
        let after_modified = std::fs::metadata(&after_files[0])
            .expect("revision file should still exist")
            .modified()
            .expect("revision mtime should be readable");
        assert_eq!(
            before_modified, after_modified,
            "snapshot after successful sync should not rewrite local revision state"
        );
    }

    #[tokio::test]
    async fn given_changed_remote_state_when_snapshot_then_latest_remote_data_is_visible_locally() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut seed_store = create_s3_store_for_test(
            "snapshot_db_refreshes_changed_remote_state",
            DurabilityMode::Local,
        )
        .await;
        pgqrs::admin(&seed_store)
            .create_queue("first")
            .await
            .expect("seed queue should be created");
        s3_sync_store(&mut seed_store)
            .await
            .expect("initial sync should succeed");

        let follower_config = seed_store.config().clone();
        let mut follower = pgqrs::connect_with_config(&follower_config)
            .await
            .expect("follower should open");
        s3_snapshot_store(&mut follower)
            .await
            .expect("initial follower snapshot should succeed");
        assert!(
            pgqrs::tables(&follower)
                .queues()
                .exists("first")
                .await
                .expect("follower queue existence check should succeed"),
            "initial snapshot should load first queue"
        );

        pgqrs::admin(&seed_store)
            .create_queue("second")
            .await
            .expect("second queue should be created");
        s3_sync_store(&mut seed_store)
            .await
            .expect("second sync should publish changed remote state");

        s3_snapshot_store(&mut follower)
            .await
            .expect("snapshot after remote change should succeed");
        assert!(
            pgqrs::tables(&follower)
                .queues()
                .exists("second")
                .await
                .expect("follower queue existence check should succeed"),
            "snapshot should refresh follower to latest remote state"
        );
    }

    #[tokio::test]
    async fn given_missing_remote_object_when_snapshot_then_returns_not_found() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let mut store = create_s3_store_for_test(
            "snapshot_db_missing_remote_returns_not_found",
            DurabilityMode::Local,
        )
        .await;
        pgqrs::admin(&store)
            .create_queue("seeded")
            .await
            .expect("queue creation should succeed");
        s3_sync_store(&mut store)
            .await
            .expect("sync should publish initial remote state");

        let (bucket, key) = parse_s3_bucket_key(&store.config().dsn);
        delete_key(&client, &bucket, &key).await;

        let err = s3_snapshot_store(&mut store)
            .await
            .expect_err("snapshot should fail when remote object is missing");
        assert!(
            matches!(err, pgqrs::error::Error::NotFound { .. }),
            "expected NotFound when remote object is missing, got: {err}"
        );
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

    ensure_bucket(&client, &bucket).await;

    let put_v1 = client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from_static(b"adapter-v1"))
        .send()
        .await
        .expect("adapter put v1");
    let etag_v1 = put_v1
        .e_tag()
        .expect("adapter put should return etag")
        .to_string();
    let obj_v1 = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .expect("adapter get v1");
    let bytes_v1 = obj_v1
        .body
        .collect()
        .await
        .expect("adapter read v1")
        .into_bytes();
    assert_eq!(bytes_v1.as_ref(), b"adapter-v1");

    let _put_v2 = client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .if_match(etag_v1)
        .body(ByteStream::from_static(b"adapter-v2"))
        .send()
        .await
        .expect("adapter cas put v2");
    let obj_v2 = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .expect("adapter get v2");
    let bytes_v2 = obj_v2
        .body
        .collect()
        .await
        .expect("adapter read v2")
        .into_bytes();
    assert_eq!(bytes_v2.as_ref(), b"adapter-v2");

    delete_key(&client, &bucket, &key).await;
}

mod consistent_db_tests {
    use super::*;

    #[tokio::test]
    async fn consistent_bootstrap_open_and_queue_lifecycle() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let store = create_s3_store_for_test(
            "consistent_bootstrap_open_and_queue_lifecycle",
            DurabilityMode::Durable,
        )
        .await;

        assert_eq!(store_mode(&store), DurabilityMode::Durable);
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
    }

    #[tokio::test]
    async fn consistent_across_connections() {
        let client = localstack_client().await;
        let bucket = s3_bucket();
        ensure_bucket(&client, &bucket).await;

        let follower_base_store = create_s3_store_for_test(
            "consistent_bootstrap_compatibility_aliases",
            DurabilityMode::Durable,
        )
        .await;
        let store = follower_base_store.clone();

        pgqrs::admin(&store)
            .create_queue("stable")
            .await
            .expect("queue create should succeed");

        let follower_cfg = follower_base_store.config().clone();
        let follower = pgqrs::connect_with_config(&follower_cfg)
            .await
            .expect("follower should open");
        let bootstrap_err = follower
            .bootstrap()
            .await
            .expect_err("follower bootstrap should conflict when remote state already exists");
        assert!(
            matches!(bootstrap_err, pgqrs::error::Error::Conflict { .. }),
            "follower bootstrap should surface conflict, got: {bootstrap_err}"
        );

        let mut restore_follower = pgqrs::connect_with_config(&follower_cfg)
            .await
            .expect("fresh follower should reopen cleanly after bootstrap conflict");
        s3_snapshot_store(&mut restore_follower)
            .await
            .expect("follower snapshot should succeed as compatibility alias");
        let queues = pgqrs::tables(&restore_follower)
            .queues()
            .exists("stable")
            .await
            .expect("queue existence check should succeed");
        assert!(queues);
    }

    async fn reopen_durable_store(config: &pgqrs::config::Config) -> AnyStore {
        let store = pgqrs::connect_with_config(config)
            .await
            .expect("durable store should reopen");
        store
            .bootstrap()
            .await
            .expect("durable bootstrap should succeed");
        store
    }

    async fn remote_etag(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        key: &str,
        context: &str,
    ) -> String {
        client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect(context)
            .e_tag()
            .map(|etag| etag.to_string())
            .unwrap_or_default()
    }

    async fn durable_config_for_missing_remote(schema: &str) -> pgqrs::config::Config {
        let dsn = common::get_test_dsn(schema).await;
        let mut config = pgqrs::config::Config::from_dsn_with_schema(&dsn, "s3_bootstrap").unwrap();
        config.s3.mode = DurabilityMode::Durable;
        config
    }

    #[tokio::test]
    async fn given_missing_remote_state_when_consistent_bootstrap_then_creates_remote_state() {
        let client = localstack_client().await;
        let config = durable_config_for_missing_remote("cboot_create_remote").await;
        let (bucket, key) = parse_s3_bucket_key(&config.dsn);
        ensure_bucket(&client, &bucket).await;
        delete_key(&client, &bucket, &key).await;

        let _store = reopen_durable_store(&config).await;

        assert!(
            wait_for_head_object(&client, &bucket, &key).await,
            "durable bootstrap should publish missing remote state without explicit sync"
        );
    }

    #[tokio::test]
    async fn given_missing_remote_state_when_consistent_bootstrap_then_state_is_immediately_readable_without_sync_or_refresh(
    ) {
        let client = localstack_client().await;
        let config = durable_config_for_missing_remote("cboot_readable_no_sync").await;
        let (bucket, key) = parse_s3_bucket_key(&config.dsn);
        ensure_bucket(&client, &bucket).await;
        delete_key(&client, &bucket, &key).await;
        let store = reopen_durable_store(&config).await;

        let queue_exists = pgqrs::tables(&store)
            .queues()
            .exists("jobs")
            .await
            .expect("queue existence check should succeed on bootstrapped schema");
        assert!(
            !queue_exists,
            "bootstrapped durable store should be readable immediately without explicit sync or refresh"
        );
    }

    #[tokio::test]
    async fn given_existing_remote_state_when_consistent_bootstrap_then_returns_conflict() {
        let seed_store =
            create_s3_store_for_test("cboot_conflict_existing_seed", DurabilityMode::Durable).await;
        let seed_config = seed_store.config().clone();

        pgqrs::admin(&seed_store)
            .create_queue("seeded")
            .await
            .expect("seed queue should be created");
        let producer = pgqrs::producer("seed-host", 9921, "seeded")
            .create(&seed_store)
            .await
            .expect("seed producer should be created");
        pgqrs::enqueue()
            .message(&serde_json::json!({ "job": "seeded" }))
            .worker(&producer)
            .execute(&seed_store)
            .await
            .expect("seed enqueue should succeed");

        let reopened = pgqrs::connect_with_config(&seed_config)
            .await
            .expect("durable store should reopen");
        let err = reopened
            .bootstrap()
            .await
            .expect_err("bootstrap should conflict when remote state already exists");
        assert!(
            matches!(err, pgqrs::error::Error::Conflict { .. }),
            "existing remote bootstrap should surface conflict, got: {err}"
        );
    }

    #[tokio::test]
    async fn given_unchanged_remote_state_when_consistent_bootstrap_repeated_then_state_is_unchanged(
    ) {
        let client = localstack_client().await;
        let config = durable_config_for_missing_remote("cboot_repeat_same").await;
        let (bucket, key) = parse_s3_bucket_key(&config.dsn);
        ensure_bucket(&client, &bucket).await;
        delete_key(&client, &bucket, &key).await;

        let store = reopen_durable_store(&config).await;

        pgqrs::admin(&store)
            .create_queue("stable")
            .await
            .expect("queue creation should succeed");

        let before = remote_etag(
            &client,
            &bucket,
            &key,
            "remote state should exist after durable bootstrap",
        )
        .await;

        store
            .bootstrap()
            .await
            .expect("repeated bootstrap should succeed");
        assert!(
            pgqrs::tables(&store)
                .queues()
                .exists("stable")
                .await
                .expect("queue existence check should succeed after repeated bootstrap"),
            "repeated bootstrap should preserve durable state"
        );

        let after = remote_etag(
            &client,
            &bucket,
            &key,
            "remote state should still exist after durable reopen",
        )
        .await;

        assert_eq!(
            before, after,
            "etag should remain stable across repeated consistent bootstrap on unchanged state"
        );
    }
}
