use async_trait::async_trait;
use pgqrs_server::db::error::PgqrsError;
use pgqrs_server::db::traits::{MessageRepo, Queue, QueueMessage, QueueRepo, QueueStats};
use sqlx::types::JsonValue;

/// Mock queue repository for testing
#[derive(Clone)]
pub struct MockQueueRepo {
    pub should_fail: bool,
}

impl MockQueueRepo {
    /// Create a new mock queue repository that will succeed
    pub fn healthy() -> Self {
        Self { should_fail: false }
    }

    /// Create a new mock queue repository that will fail
    pub fn failing() -> Self {
        Self { should_fail: true }
    }
}

#[async_trait]
impl QueueRepo for MockQueueRepo {
    async fn list_queues(&self) -> Result<Vec<Queue>, PgqrsError> {
        if self.should_fail {
            Err(PgqrsError::Other("Database connection failed".to_string()))
        } else {
            Ok(vec![])
        }
    }

    async fn create_queue(&self, _name: &str, _unlogged: bool) -> Result<Queue, PgqrsError> {
        unimplemented!()
    }

    async fn get_queue(&self, _name: &str) -> Result<Queue, PgqrsError> {
        unimplemented!()
    }

    async fn delete_queue(&self, _name: &str) -> Result<(), PgqrsError> {
        unimplemented!()
    }

    async fn purge_queue(&self, _name: &str) -> Result<(), PgqrsError> {
        unimplemented!()
    }
}

/// Mock message repository for testing
#[derive(Clone)]
pub struct MockMessageRepo;

impl MockMessageRepo {
    /// Create a new mock message repository
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockMessageRepo {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageRepo for MockMessageRepo {
    async fn enqueue(
        &self,
        _queue: &str,
        _payload: &JsonValue,
    ) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }

    async fn enqueue_delayed(
        &self,
        _queue: &str,
        _payload: &JsonValue,
        _delay_seconds: u32,
    ) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }

    async fn batch_enqueue(
        &self,
        _queue: &str,
        _payloads: &[JsonValue],
    ) -> Result<Vec<QueueMessage>, PgqrsError> {
        unimplemented!()
    }

    async fn dequeue(&self, _queue: &str, _message_id: i64) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }

    async fn ack(&self, _queue: &str, _message_id: i64) -> Result<(), PgqrsError> {
        unimplemented!()
    }

    async fn nack(&self, _queue: &str, _message_id: i64) -> Result<(), PgqrsError> {
        unimplemented!()
    }

    async fn peek(&self, _queue: &str, _limit: usize) -> Result<Vec<QueueMessage>, PgqrsError> {
        unimplemented!()
    }

    async fn stats(&self, _queue: &str) -> Result<QueueStats, PgqrsError> {
        unimplemented!()
    }

    async fn get_message_by_id(
        &self,
        _queue: &str,
        _message_id: i64,
    ) -> Result<QueueMessage, PgqrsError> {
        unimplemented!()
    }

    async fn heartbeat(
        &self,
        _queue: &str,
        _message_id: i64,
        _additional_seconds: u32,
    ) -> Result<(), PgqrsError> {
        unimplemented!()
    }
}
