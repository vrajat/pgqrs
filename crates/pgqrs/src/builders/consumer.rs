//! Consumer builder for creating managed consumer workers

use crate::error::Result;
use crate::store::Store;

/// Builder for creating a managed consumer worker
pub struct ConsumerBuilder<'a> {
    hostname: &'a str,
    port: i32,
    queue: &'a str,
}

impl<'a> ConsumerBuilder<'a> {
    pub fn new(hostname: &'a str, port: i32, queue: &'a str) -> Self {
        Self {
            hostname,
            port,
            queue,
        }
    }

    /// Create the consumer worker
    pub async fn create<S: Store>(self, store: &S) -> Result<Box<dyn crate::store::Consumer + '_>> {
        store
            .consumer(self.queue, self.hostname, self.port, store.config())
            .await
    }
}

/// Create a managed consumer worker
///
/// # Example
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use pgqrs::Config;
/// use pgqrs::store::AnyStore;
/// let config = Config::from_dsn("postgres://localhost/mydb");
/// let store = pgqrs::connect_with_config(&config).await?;
/// let consumer = pgqrs::consumer("localhost", 3000, "orders").create(&store).await?;
/// # Ok(())
/// # }
/// ```
pub fn consumer<'a>(hostname: &'a str, port: i32, queue: &'a str) -> ConsumerBuilder<'a> {
    ConsumerBuilder::new(hostname, port, queue)
}
