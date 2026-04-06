//! Producer builder for creating managed producer workers.

use crate::error::Result;
use crate::store::Store;

/// Create a managed producer worker.
///
/// ```rust,no_run
/// # use pgqrs;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = pgqrs::connect("postgresql://localhost/mydb").await?;
/// let producer = pgqrs::producer("orders-worker", "orders").create(&store).await?;
/// # Ok(()) }
/// ```
pub fn producer<'a>(name: &'a str, queue: &'a str) -> ProducerBuilder<'a> {
    ProducerBuilder::new(name, queue)
}

/// Builder for creating a managed producer worker.
pub struct ProducerBuilder<'a> {
    name: &'a str,
    queue: &'a str,
}

impl<'a> ProducerBuilder<'a> {
    pub fn new(name: &'a str, queue: &'a str) -> Self {
        Self { name, queue }
    }

    /// Create the producer worker
    pub async fn create<S: Store>(self, store: &S) -> Result<crate::workers::Producer> {
        store.producer(self.queue, self.name, store.config()).await
    }
}
