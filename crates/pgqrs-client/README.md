# pgqrs-client

A Rust client library for the PGQRS queue service.

## Features

- **Builder Pattern**: Easy-to-use client configuration with `PgqrsClient::builder()`
- **Health Checks**: Implemented `liveness()` and `readiness()` methods
- **Payload Codec**: Trait-based payload encoding/decoding with default JSON support
- **Error Handling**: Comprehensive error types with proper error propagation
- **Async/Await**: Full async support with `tokio`

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
pgqrs-client = { path = "../pgqrs-client" }
tokio = { version = "1", features = ["full"] }
```

Basic usage:

```rust
use pgqrs_client::PgqrsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client
    let mut client = PgqrsClient::builder()
        .endpoint("http://localhost:50051")
        .api_key("your-api-key")  // optional
        .connect_timeout(std::time::Duration::from_secs(10))
        .rpc_timeout(std::time::Duration::from_secs(30))
        .build()
        .await?;

    // Check service health
    let liveness = client.liveness().await?;
    println!("Service status: {}", liveness.status);

    let readiness = client.readiness().await?;
    println!("Service ready: {}", readiness.status);

    Ok(())
}
```

## Configuration Options

- `endpoint(String)` - Server endpoint URL (required)
- `api_key(Option<String>)` - API key for authentication
- `connect_timeout(Duration)` - Connection timeout (default: 10s)
- `rpc_timeout(Duration)` - RPC call timeout (default: 30s)

## Payload Encoding

Use the `PgqrsPayloadCodec` trait for custom payload encoding, or use the built-in JSON helpers:

```rust
use pgqrs_client::json;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct MyMessage {
    id: u64,
    content: String,
}

// Encode to bytes
let message = MyMessage { id: 1, content: "hello".to_string() };
let bytes = json::to_bytes(&message)?;

// Decode from bytes
let decoded: MyMessage = json::from_bytes(&bytes)?;
```

## Current Implementation Status

- ✅ **Health Checks**: `liveness()` and `readiness()` methods are fully implemented
- ⏳ **Queue Operations**: Other methods are stubbed with `unimplemented!()` and ready for implementation
- ⏳ **TLS Support**: Placeholder for future implementation
- ⏳ **Retry Logic**: Placeholder for future implementation

## Examples

See the `examples/` directory for usage examples:

- `basic_client.rs` - Basic client setup and health checks

Run examples:

```bash
cargo run --example basic_client -p pgqrs-client
```

## Testing

The client includes comprehensive integration tests that verify the health check functionality:

```bash
# Run all tests
cargo test -p pgqrs-client

# Run specific health check tests
cargo test -p pgqrs-client --test healthcheck

# Run with output
cargo test -p pgqrs-client --test healthcheck -- --nocapture
```

### Test Coverage

- ✅ **Proto Message Creation**: Verifies protobuf message compilation
- ✅ **Liveness Probe**: Tests with healthy and failing dependencies
- ✅ **Readiness Probe**: Tests with healthy and failing dependencies  
- ✅ **Performance**: Ensures liveness calls are fast (< 50ms average)
- ✅ **Client Configuration**: Tests builder pattern and configurations
- ✅ **Error Handling**: Tests connection failures and timeout scenarios

The integration tests use an in-process gRPC server with mock dependencies, providing fast and deterministic test execution without external dependencies.