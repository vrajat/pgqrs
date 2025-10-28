use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

use crate::api::queue_service_client::QueueServiceClient;
use crate::error::{PgqrsClientError, Result};
use crate::DeleteQueueRequest;

/// Client configuration builder
#[derive(Debug, Clone)]
pub struct PgqrsClientBuilder {
    endpoint: Option<String>,
    api_key: Option<String>,
    connect_timeout: Duration,
    rpc_timeout: Duration,
}

impl Default for PgqrsClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            api_key: None,
            connect_timeout: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(30),
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

    /// Build the client
    pub async fn build(self) -> Result<PgqrsClient> {
        let endpoint = self
            .endpoint
            .ok_or_else(|| PgqrsClientError::InvalidConfig("endpoint is required".to_string()))?;

        let endpoint = Endpoint::from_shared(endpoint)
            .map_err(|e| PgqrsClientError::InvalidConfig(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(self.connect_timeout)
            .timeout(self.rpc_timeout);

        let channel = endpoint.connect().await?;

        Ok(PgqrsClient {
            client: QueueServiceClient::new(channel),
            api_key: self.api_key,
            rpc_timeout: self.rpc_timeout,
        })
    }
}

/// Main client for interacting with PGQRS
pub struct PgqrsClient {
    client: QueueServiceClient<Channel>,
    api_key: Option<String>,
    rpc_timeout: Duration,
}

impl PgqrsClient {
    /// Create a new client builder
    pub fn builder() -> PgqrsClientBuilder {
        PgqrsClientBuilder::new()
    }

    /// Check service liveness
    pub async fn liveness(&mut self) -> Result<crate::api::LivenessResponse> {
        let request = crate::api::LivenessRequest {};
        let response = self.client.liveness(request).await?;
        Ok(response.into_inner())
    }

    /// Check service readiness
    pub async fn readiness(&mut self) -> Result<crate::api::ReadinessResponse> {
        let request = crate::api::ReadinessRequest {};
        let response = self.client.readiness(request).await?;
        Ok(response.into_inner())
    }

    // Queue management methods - stubs

    /// Create a new queue
    pub async fn create_queue(&mut self, name: &str, unlogged: bool) -> Result<crate::api::Queue> {
        let request = crate::api::CreateQueueRequest {
            name: name.to_string(),
            unlogged,
        };
        let response = self.client.create_queue(request).await?;
        Ok(response.into_inner())
    }

    /// Delete a queue
    pub async fn delete_queue(&mut self, name: &str) -> Result<()> {
        let request = DeleteQueueRequest {
            name: name.to_string(),
        };
        let _response = self.client.delete_queue(request).await?;
        Ok(())
    }

    /// Get queue information
    pub async fn get_queue(&mut self, name: &str) -> Result<crate::api::Queue> {
        let request = crate::GetQueueRequest {
            name: name.to_string(),
        };
        let response = self.client.get_queue(request).await?;
        Ok(response.into_inner())
    }

    /// List all queues
    pub async fn list_queues(&mut self) -> Result<Vec<crate::api::Queue>> {
        let request = crate::api::ListQueuesRequest {};
        let response = self.client.list_queues(request).await?;
        Ok(response.into_inner().queues)
    }

    // Message operations - stubs

    /// Enqueue a message
    pub async fn enqueue(
        &mut self,
        _queue_name: &str,
        _payload: Vec<u8>,
        _delay_seconds: i64,
    ) -> Result<String> {
        unimplemented!("enqueue not yet implemented")
    }

    /// Enqueue a JSON message
    pub async fn enqueue_json<T: serde::Serialize>(
        &mut self,
        _queue_name: &str,
        _payload: &T,
        _delay_seconds: i64,
    ) -> Result<String> {
        unimplemented!("enqueue_json not yet implemented")
    }

    /// Dequeue messages
    pub async fn dequeue(
        &mut self,
        _queue_name: &str,
        _max_messages: i32,
        _lease_seconds: i64,
    ) -> Result<Vec<crate::api::Message>> {
        unimplemented!("dequeue not yet implemented")
    }

    /// Acknowledge a message
    pub async fn ack(&mut self, _message_id: &str) -> Result<()> {
        unimplemented!("ack not yet implemented")
    }

    /// Negative acknowledge a message
    pub async fn nack(
        &mut self,
        _message_id: &str,
        _reason: Option<String>,
        _dead_letter: bool,
    ) -> Result<()> {
        unimplemented!("nack not yet implemented")
    }

    /// Requeue a message
    pub async fn requeue(&mut self, _message_id: &str, _delay_seconds: i64) -> Result<()> {
        unimplemented!("requeue not yet implemented")
    }

    /// Extend message lease
    pub async fn extend_lease(
        &mut self,
        _message_id: &str,
        _additional_seconds: i64,
    ) -> Result<()> {
        unimplemented!("extend_lease not yet implemented")
    }

    // Queue inspection methods - stubs

    /// Peek at messages without dequeuing
    pub async fn peek(
        &mut self,
        _queue_name: &str,
        _limit: i32,
    ) -> Result<Vec<crate::api::Message>> {
        unimplemented!("peek not yet implemented")
    }

    /// Get queue statistics
    pub async fn stats(&mut self, _queue_name: &str) -> Result<crate::api::StatsResponse> {
        unimplemented!("stats not yet implemented")
    }

    /// List in-flight messages
    pub async fn list_in_flight(
        &mut self,
        _queue_name: &str,
        _limit: i32,
    ) -> Result<Vec<crate::api::Message>> {
        unimplemented!("list_in_flight not yet implemented")
    }

    /// List dead letter messages
    pub async fn list_dead_letters(
        &mut self,
        _queue_name: &str,
        _limit: i32,
    ) -> Result<Vec<crate::api::Message>> {
        unimplemented!("list_dead_letters not yet implemented")
    }

    /// Purge dead letter messages
    pub async fn purge_dead_letters(&mut self, _queue_name: &str) -> Result<()> {
        unimplemented!("purge_dead_letters not yet implemented")
    }

    /// Health check
    pub async fn health_check(&mut self) -> Result<crate::api::HealthCheckResponse> {
        unimplemented!("health_check not yet implemented")
    }
}
