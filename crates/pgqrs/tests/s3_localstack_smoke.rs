#![cfg(feature = "s3")]

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

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

async fn localstack_client() -> Client {
    let creds = Credentials::new("test", "test", None, None, "localstack");
    let conf = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(s3_region()))
        .credentials_provider(creds)
        .endpoint_url(s3_endpoint())
        .load()
        .await;

    let s3_conf = aws_sdk_s3::config::Builder::from(&conf)
        .force_path_style(true)
        .build();
    Client::from_conf(s3_conf)
}

#[tokio::test]
async fn localstack_s3_basic_ops_and_cas_etag() {
    let client = localstack_client().await;
    let bucket = s3_bucket();
    let key = format!("smoke/etag-cas-{}.bin", uuid::Uuid::new_v4());

    let create_res = client.create_bucket().bucket(&bucket).send().await;
    if let Err(e) = create_res {
        let msg = e.to_string();
        assert!(
            msg.contains("BucketAlreadyOwnedByYou") || msg.contains("BucketAlreadyExists"),
            "create_bucket failed unexpectedly: {}",
            msg
        );
    }

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
}
