//! Consumer builder for creating managed consumer workers.

use crate::error::Result;
use crate::store::Store;

/// Create a managed consumer worker.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let consumer = pgqrs::consumer("orders-worker", "orders").create(&store).await?;
/// # Ok(()) }
/// ```
pub fn consumer<'a>(name: &'a str, queue: &'a str) -> ConsumerBuilder<'a> {
    ConsumerBuilder::new(name, queue)
}

/// Builder for creating a managed consumer worker.
pub struct ConsumerBuilder<'a> {
    name: &'a str,
    queue: &'a str,
}

impl<'a> ConsumerBuilder<'a> {
    pub fn new(name: &'a str, queue: &'a str) -> Self {
        Self { name, queue }
    }

    /// Create the consumer worker
    pub async fn create<S: Store>(self, store: &S) -> Result<crate::workers::Consumer> {
        store.consumer(self.queue, self.name).await
    }
}
