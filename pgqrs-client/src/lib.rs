pub mod api;
pub mod error;
pub mod codec;

use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use backoff::{ExponentialBackoff, Operation};
use url::Url;

use crate::api::queue_service_client::QueueServiceClient;
use crate::api::*;
use crate::error::{PgqrsClientError, Result};

pub use crate::codec::{PgqrsPayloadCodec, JsonCodec, json};
pub use crate::error::PgqrsClientError;

/// Configuration for TLS
#[derive(Debug, Clone)]
pub enum TlsConfig {
    /// No TLS
    None,
    /// TLS with system certificates
    System,
    /// TLS with custom certificates
    Custom {
        ca_cert: Option<Vec<u8>>,
        client_cert: Option<Vec<u8>>,
        client_key: Option<Vec<u8>>,
    },
}

/// Client configuration builder
#[derive(Debug, Clone)]
pub struct PgqrsClientBuilder {
    endpoint: Option<String>,
    tls: TlsConfig,
    api_key: Option<String>,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    max_retries: usize,
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
}

impl Default for PgqrsClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            tls: TlsConfig::None,
            api_key: None,
            connect_timeout: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(30),
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),
        }
    }
}

impl PgqrsClientBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the server endpoint
    pub fn endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Configure TLS
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Enable TLS with system certificates
    pub fn with_tls(mut self) -> Self {
        self.tls = TlsConfig::System;
        self
    }

    /// Set API key for authentication
    pub fn api_key<S: Into<String>>(mut self, api_key: S) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set RPC timeout
    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }

    /// Configure retry behavior
    pub fn retries(mut self, max_retries: usize, initial_delay: Duration, max_delay: Duration) -> Self {
        self.max_retries = max_retries;
        self.initial_retry_delay = initial_delay;
        self.max_retry_delay = max_delay;
        self
    }

    /// Build the client
    pub async fn build(self) -> Result<PgqrsClient> {
        let endpoint = self.endpoint.ok_or_else(|| {
            PgqrsClientError::InvalidConfig("endpoint is required".to_string())
        })?;

        // Parse and validate the endpoint
        let url = Url::parse(&endpoint)
            .map_err(|e| PgqrsClientError::InvalidConfig(format!("Invalid endpoint URL: {}", e)))?;

        let mut endpoint = Endpoint::from_shared(endpoint)
            .map_err(|e| PgqrsClientError::InvalidConfig(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(self.connect_timeout)
            .timeout(self.rpc_timeout);

        // Configure TLS
        match self.tls {
            TlsConfig::None => {
                // No TLS configuration needed
            }
            TlsConfig::System => {
                let tls = ClientTlsConfig::new();
                endpoint = endpoint.tls_config(tls)?;
            }
            TlsConfig::Custom { ca_cert, client_cert, client_key } => {
                let mut tls = ClientTlsConfig::new();

                if let Some(ca) = ca_cert {
                    tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca));
                }

                if let (Some(cert), Some(key)) = (client_cert, client_key) {
                    let identity = tonic::transport::Identity::from_pem(cert, key);
                    tls = tls.identity(identity);
                }

                endpoint = endpoint.tls_config(tls)?;
            }
        }

        let channel = endpoint.connect().await?;

        let backoff = ExponentialBackoff {
            initial_interval: self.initial_retry_delay,
            max_interval: self.max_retry_delay,
            max_elapsed_time: Some(self.max_retry_delay * (self.max_retries as u32)),
            ..Default::default()
        };

        Ok(PgqrsClient {
            client: QueueServiceClient::new(channel),
            api_key: self.api_key,
            rpc_timeout: self.rpc_timeout,
            backoff,
            max_retries: self.max_retries,
        })
    }
}

/// Main client for interacting with PGQRS
pub struct PgqrsClient {
    client: QueueServiceClient<Channel>,
    api_key: Option<String>,
    rpc_timeout: Duration,
    backoff: ExponentialBackoff,
    max_retries: usize,
}

impl PgqrsClient {
    /// Create a new client builder
    pub fn builder() -> PgqrsClientBuilder {
        PgqrsClientBuilder::new()
    }

    /// Helper method to create a request with API key if configured
    fn with_auth<T>(&self, request: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(request);
        req.set_timeout(self.rpc_timeout);

        if let Some(api_key) = &self.api_key {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", api_key).parse().unwrap(),
            );
        }

        req
    }

    /// Execute an operation with retry logic
    async fn with_retry<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut backoff = self.backoff.clone();
        let mut attempts = 0;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    attempts += 1;
                    if attempts >= self.max_retries {
                        return Err(e);
                    }

                    if let Some(delay) = backoff.next_backoff() {
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(PgqrsClientError::BackoffExhausted);
                    }
                }
            }
        }
    }

    // Health check methods - these are implemented

    /// Check service liveness
    pub async fn liveness(&mut self) -> Result<LivenessResponse> {
        self.with_retry(|| async {
            let request = self.with_auth(LivenessRequest {});
            let response = self.client.liveness(request).await?;
            Ok(response.into_inner())
        }).await
    }

    /// Check service readiness
    pub async fn readiness(&mut self) -> Result<ReadinessResponse> {
        self.with_retry(|| async {
            let request = self.with_auth(ReadinessRequest {});
            let response = self.client.readiness(request).await?;
            Ok(response.into_inner())
        }).await
    }

    // Queue management methods - stubs

    /// Create a new queue
    pub async fn create_queue(&mut self, _name: &str, _unlogged: bool) -> Result<Queue> {
        unimplemented!("create_queue not yet implemented")
    }

    /// Delete a queue
    pub async fn delete_queue(&mut self, _name: &str) -> Result<()> {
        unimplemented!("delete_queue not yet implemented")
    }

    /// Get queue information
    pub async fn get_queue(&mut self, _name: &str) -> Result<Queue> {
        unimplemented!("get_queue not yet implemented")
    }

    /// List all queues
    pub async fn list_queues(&mut self) -> Result<Vec<Queue>> {
        unimplemented!("list_queues not yet implemented")
    }

    // Message operations - stubs

    /// Enqueue a message
    pub async fn enqueue(&mut self, _queue_name: &str, _payload: Vec<u8>, _delay_seconds: i64) -> Result<String> {
        unimplemented!("enqueue not yet implemented")
    }

    /// Enqueue a JSON message
    pub async fn enqueue_json<T: serde::Serialize>(&mut self, _queue_name: &str, _payload: &T, _delay_seconds: i64) -> Result<String> {
        unimplemented!("enqueue_json not yet implemented")
    }

    /// Dequeue messages
    pub async fn dequeue(&mut self, _queue_name: &str, _max_messages: i32, _lease_seconds: i64) -> Result<Vec<Message>> {
        unimplemented!("dequeue not yet implemented")
    }

    /// Acknowledge a message
    pub async fn ack(&mut self, _message_id: &str) -> Result<()> {
        unimplemented!("ack not yet implemented")
    }

    /// Negative acknowledge a message
    pub async fn nack(&mut self, _message_id: &str, _reason: Option<String>, _dead_letter: bool) -> Result<()> {
        unimplemented!("nack not yet implemented")
    }

    /// Requeue a message
    pub async fn requeue(&mut self, _message_id: &str, _delay_seconds: i64) -> Result<()> {
        unimplemented!("requeue not yet implemented")
    }

    /// Extend message lease
    pub async fn extend_lease(&mut self, _message_id: &str, _additional_seconds: i64) -> Result<()> {
        unimplemented!("extend_lease not yet implemented")
    }

    // Queue inspection methods - stubs

    /// Peek at messages without dequeuing
    pub async fn peek(&mut self, _queue_name: &str, _limit: i32) -> Result<Vec<Message>> {
        unimplemented!("peek not yet implemented")
    }

    /// Get queue statistics
    pub async fn stats(&mut self, _queue_name: &str) -> Result<StatsResponse> {
        unimplemented!("stats not yet implemented")
    }

    /// List in-flight messages
    pub async fn list_in_flight(&mut self, _queue_name: &str, _limit: i32) -> Result<Vec<Message>> {
        unimplemented!("list_in_flight not yet implemented")
    }

    /// List dead letter messages
    pub async fn list_dead_letters(&mut self, _queue_name: &str, _limit: i32) -> Result<Vec<Message>> {
        unimplemented!("list_dead_letters not yet implemented")
    }

    /// Purge dead letter messages
    pub async fn purge_dead_letters(&mut self, _queue_name: &str) -> Result<()> {
        unimplemented!("purge_dead_letters not yet implemented")
    }

    /// Health check
    pub async fn health_check(&mut self) -> Result<HealthCheckResponse> {
        unimplemented!("health_check not yet implemented")
    }
}

// Re-export commonly used types
pub use crate::api::{
    Queue, Message, HealthCheckResponse, LivenessResponse, ReadinessResponse, StatsResponse,
};
