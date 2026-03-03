#![cfg(feature = "s3")]

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use pgqrs::config::Config;
use pgqrs::error::Error;
use pgqrs::store::s3::client::{build_aws_s3_client, AwsS3ClientConfig, AwsS3ObjectStore};
use pgqrs::store::s3::{DurabilityMode, S3Store, S3SyncConfig};
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
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

async fn localstack_client() -> Client {
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
async fn localstack_s3_store_open_uses_sqlite_cache() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("open");
    let config = Config::from_dsn("sqlite::memory:");
    let store = S3Store::open(
        &dsn,
        &config,
        DurabilityMode::Local,
        S3SyncConfig {
            flush_interval_ms: 60_000,
            ..S3SyncConfig::default()
        },
    )
    .await
    .expect("open should succeed");

    assert_eq!(store.mode(), DurabilityMode::Local);
    assert_eq!(store.source_dsn(), dsn);
    assert!(store.sqlite_cache_dsn().starts_with("sqlite://"));
    assert_eq!(store.current_sequence(), 0);
    assert_eq!(store.last_flushed_sequence(), 0);

    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_flush_sequence_tracking() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("flush-seq");
    let config = Config::from_dsn("sqlite::memory:");
    let store = S3Store::open(
        &dsn,
        &config,
        DurabilityMode::Local,
        S3SyncConfig::default(),
    )
    .await
    .expect("open should succeed");

    let seq = store.reserve_write_sequence();
    assert_eq!(seq, 1);
    assert_eq!(store.current_sequence(), 1);
    assert_eq!(store.last_flushed_sequence(), 0);

    store.mark_flushed(seq);
    store
        .wait_until_flushed(seq, Duration::from_millis(100))
        .await
        .expect("wait should complete");
    assert_eq!(store.last_flushed_sequence(), 1);

    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_flush_once_promotes_and_marks_flushed() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("flush-once");
    let config = Config::from_dsn("sqlite::memory:");
    let store = S3Store::open(
        &dsn,
        &config,
        DurabilityMode::Durable,
        S3SyncConfig::default(),
    )
    .await
    .expect("open should succeed");

    std::fs::write(store.state().write_path(), b"hello-world").expect("write test db bytes");
    let seq = store.reserve_write_sequence();

    store.flush_once(&key).await.expect("flush should succeed");
    store
        .wait_until_flushed(seq, Duration::from_millis(500))
        .await
        .expect("seq should be durable");

    let read_bytes = std::fs::read(store.state().read_path()).expect("read read_db");
    assert_eq!(read_bytes, b"hello-world");

    let adapter = AwsS3ObjectStore::new(client.clone(), bucket.clone());
    let obj = adapter.get_object(&key).await.expect("object should exist");
    assert_eq!(obj.bytes, b"hello-world");

    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_flush_once_reports_conflict() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn_a, _key_a) = unique_s3_dsn("conflict-a");
    let (dsn_b, _key_b) = unique_s3_dsn("conflict-b");
    let shared_key = unique_key("shared-conflict");

    let config = Config::from_dsn("sqlite::memory:");
    let adapter = AwsS3ObjectStore::new(client.clone(), bucket.clone());

    let probe_key = unique_key("cas-probe");
    let _probe_etag = adapter
        .put_object_if_match(&probe_key, b"probe-v1", None)
        .await
        .expect("seed probe object");
    let cas_supported = adapter
        .put_object_if_match(&probe_key, b"probe-v2", Some("stale-etag"))
        .await
        .is_err();
    delete_key(&client, &bucket, &probe_key).await;
    if !cas_supported {
        eprintln!("Skipping conflict semantics check: LocalStack did not enforce CAS on PutObject");
        return;
    }

    let store_a = S3Store::open(
        &dsn_a,
        &config,
        DurabilityMode::Local,
        S3SyncConfig::default(),
    )
    .await
    .expect("open a");
    std::fs::write(store_a.state().write_path(), b"v1").expect("write v1");
    store_a.reserve_write_sequence();
    store_a.flush_once(&shared_key).await.expect("first flush");

    let store_b = S3Store::open(
        &dsn_b,
        &config,
        DurabilityMode::Local,
        S3SyncConfig::default(),
    )
    .await
    .expect("open b");
    std::fs::write(store_b.state().write_path(), b"v2").expect("write v2");
    let seq = store_b.reserve_write_sequence();

    let err = store_b
        .flush_once(&shared_key)
        .await
        .expect_err("second store should conflict");
    assert!(matches!(err, Error::Conflict { .. }));

    let wait_err = store_b
        .wait_until_flushed(seq, Duration::from_millis(500))
        .await
        .expect_err("pending waiters should fail after conflict");
    assert!(matches!(wait_err, Error::Conflict { .. }));

    let write_bytes = std::fs::read(store_b.state().write_path()).expect("read recovered write");
    assert_eq!(write_bytes, b"v1");

    std::fs::write(store_b.state().write_path(), b"v2").expect("rewrite v2");
    store_b.reserve_write_sequence();
    store_b
        .flush_once(&shared_key)
        .await
        .expect("retry flush should succeed after recovery");

    delete_key(&client, &bucket, &shared_key).await;
}

#[tokio::test]
async fn localstack_s3_store_run_sync_task_flushes_on_wake() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("sync-wake");
    let config = Config::from_dsn("sqlite::memory:");
    let store = S3Store::open(
        &dsn,
        &config,
        DurabilityMode::Local,
        S3SyncConfig {
            flush_interval_ms: 60_000,
            ..S3SyncConfig::default()
        },
    )
    .await
    .expect("open should succeed");

    std::fs::write(store.state().write_path(), b"sync-wake").expect("write test bytes");
    let seq = store.reserve_write_sequence();

    let adapter = Arc::new(AwsS3ObjectStore::new(client.clone(), bucket.clone()));
    let (wake, wake_rx) = store.create_wake_channel();

    let task_store = store.clone();
    let task_adapter = adapter.clone();
    let task_key = key.clone();
    let handle = tokio::spawn(async move {
        task_store
            .run_sync_task(task_adapter, task_key, wake_rx)
            .await;
    });

    wake.wake();
    store
        .wait_until_flushed(seq, Duration::from_millis(1000))
        .await
        .expect("wake-triggered flush should complete");

    let obj = adapter
        .get_object(&key)
        .await
        .expect("object should exist after wake flush");
    assert_eq!(obj.bytes, b"sync-wake");

    drop(wake);
    handle.await.expect("sync loop task should exit");
    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_bootstrap_from_remote_loads_existing_state_and_etag() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("bootstrap-existing");
    let config = Config::from_dsn("sqlite::memory:");
    let store = S3Store::open(
        &dsn,
        &config,
        DurabilityMode::Durable,
        S3SyncConfig::default(),
    )
    .await
    .expect("open should succeed");

    let adapter = AwsS3ObjectStore::new(client.clone(), bucket.clone());
    let seed_bytes = std::fs::read(store.state().write_path()).expect("read seed sqlite bytes");
    adapter
        .put_object_if_match(&key, &seed_bytes, None)
        .await
        .expect("seed remote object");

    let loaded = store
        .bootstrap_from_remote(&key)
        .await
        .expect("bootstrap should succeed");
    assert!(loaded);
    assert_eq!(
        std::fs::read(store.state().write_path()).unwrap(),
        seed_bytes
    );
    assert_eq!(
        std::fs::read(store.state().read_path()).unwrap(),
        seed_bytes
    );

    std::fs::write(store.state().write_path(), b"remote-v2").expect("write v2");
    store.reserve_write_sequence();
    store
        .flush_once(&key)
        .await
        .expect("flush after bootstrap should succeed");

    delete_key(&client, &bucket, &key).await;
}

#[tokio::test]
async fn localstack_s3_store_bootstrap_from_remote_missing_object_returns_false() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    ensure_bucket(&client, &bucket).await;

    let (dsn, key) = unique_s3_dsn("bootstrap-missing");
    let config = Config::from_dsn("sqlite::memory:");
    let store = S3Store::open(
        &dsn,
        &config,
        DurabilityMode::Durable,
        S3SyncConfig::default(),
    )
    .await
    .expect("open should succeed");

    std::fs::write(store.state().write_path(), b"local-only").expect("seed local");
    let loaded = store
        .bootstrap_from_remote(&key)
        .await
        .expect("bootstrap should not fail when object missing");
    assert!(!loaded);
    assert_eq!(
        std::fs::read(store.state().write_path()).unwrap(),
        b"local-only"
    );

    delete_key(&client, &bucket, &key).await;
}
