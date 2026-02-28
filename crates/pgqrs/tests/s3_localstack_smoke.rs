#![cfg(feature = "sqlite")]

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
