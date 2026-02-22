//! Consumer builder for creating managed consumer workers.

use crate::error::Result;
use crate::store::Store;

/// Create a managed consumer worker.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let consumer = pgqrs::consumer("localhost", 3000, "orders").create(&store).await?;
/// # Ok(()) }
/// ```
pub fn consumer<'a>(hostname: &'a str, port: i32, queue: &'a str) -> ConsumerBuilder<'a> {
    ConsumerBuilder::new(hostname, port, queue)
}

/// Builder for creating a managed consumer worker.
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
    pub async fn create<S: Store>(self, store: &S) -> Result<crate::workers::Consumer> {
        store
            .consumer(self.queue, self.hostname, self.port, store.config())
            .await
    }
}
