//! Producer builder for creating managed producer workers

use crate::error::Result;
use crate::store::Store;

/// Builder for creating a managed producer worker
pub struct ProducerBuilder<'a> {
    hostname: &'a str,
    port: i32,
    queue: &'a str,
}

impl<'a> ProducerBuilder<'a> {
    pub fn new(hostname: &'a str, port: i32, queue: &'a str) -> Self {
        Self {
            hostname,
            port,
            queue,
        }
    }

    /// Create the producer worker
    pub async fn create<S: Store>(self, store: &S) -> Result<Box<dyn crate::store::Producer + '_>> {
        store
            .producer(self.queue, self.hostname, self.port, store.config())
            .await
    }
}

/// Create a managed producer worker
///
/// # Example
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use pgqrs::Config;
/// use pgqrs::store::AnyStore;
/// let config = Config::from_dsn("postgres://localhost/mydb");
/// let store = pgqrs::connect_with_config(&config).await?;
/// let producer = pgqrs::producer("localhost", 3000, "orders").create(&store).await?;
/// # Ok(())
/// # }
/// ```
pub fn producer<'a>(hostname: &'a str, port: i32, queue: &'a str) -> ProducerBuilder<'a> {
    ProducerBuilder::new(hostname, port, queue)
}
