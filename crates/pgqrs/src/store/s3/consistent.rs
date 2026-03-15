use crate::config::Config;
use crate::error::Result;
use crate::store::s3::snapshot::SnapshotDb;
use crate::store::s3::{StoreOpFuture, SyncDb};
use crate::store::{ConcurrencyModel, Store};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

/// SyncDb variant that enforces durable writes.
///
/// Every write operation executes on snapshot write state and then immediately
/// runs `sync()+refresh()` before returning.
#[derive(Clone)]
pub struct ConsistentDb {
    inner: Arc<RwLock<SnapshotDb>>,
    concurrency_model: ConcurrencyModel,
}

impl ConsistentDb {
    pub async fn new(config: &Config) -> Result<Self> {
        let snapshot = SnapshotDb::new(config).await?;
        let concurrency_model = snapshot.concurrency_model();

        Ok(Self {
            inner: Arc::new(RwLock::new(snapshot)),
            concurrency_model,
        })
    }
}

#[async_trait]
impl SyncDb for ConsistentDb {
    fn config(&self) -> &Config {
        self.inner.blocking_read().config()
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.concurrency_model
    }

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        let guard = self.inner.blocking_read();
        guard.with_read_ref(f)
    }

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        let guard = self.inner.blocking_write();
        guard.with_write_ref(f)
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        let guard = self.inner.read().await;
        guard.with_read(f).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        let mut guard = self.inner.write().await;
        let out = guard.with_write(f).await?;
        guard.sync().await?;
        guard.refresh().await?;
        Ok(out)
    }

    async fn snapshot(&mut self) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard.snapshot().await
    }

    async fn refresh(&mut self) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard.refresh().await
    }

    async fn sync(&mut self) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard.sync().await
    }
}
