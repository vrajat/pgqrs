use crate::config::Config;
use crate::error::Result;
use crate::store::s3::snapshot::SnapshotDb;
use crate::store::s3::{StoreOpFuture, SyncDb, SyncState};
use crate::store::{ConcurrencyModel, Store};
use async_trait::async_trait;

/// SyncDb variant that enforces durable writes.
///
/// Every write operation executes on snapshot write state and then immediately
/// runs `sync()` before returning.
#[derive(Clone)]
pub struct ConsistentDb {
    inner: SnapshotDb,
    concurrency_model: ConcurrencyModel,
}

impl ConsistentDb {
    pub async fn new(config: &Config) -> Result<Self> {
        let snapshot = SnapshotDb::new(config).await?;
        let concurrency_model = snapshot.concurrency_model();

        Ok(Self {
            inner: snapshot,
            concurrency_model,
        })
    }

    pub async fn state(&self) -> Result<SyncState> {
        self.inner.state().await
    }
}

#[async_trait]
impl SyncDb for ConsistentDb {
    fn config(&self) -> &Config {
        self.inner.config()
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.concurrency_model
    }

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        self.inner.with_read_ref(f)
    }

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        self.inner.with_write_ref(f)
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        self.inner.with_read(f).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        let _write_guard = self.inner.write_gate().lock().await;
        let out = self.inner.with_write(f).await?;
        let mut inner = self.inner.clone();
        SyncDb::sync(&mut inner).await?;
        Ok(out)
    }

    async fn snapshot(&mut self) -> Result<()> {
        SyncDb::snapshot(&mut self.inner).await
    }

    async fn sync(&mut self) -> Result<()> {
        SyncDb::sync(&mut self.inner).await
    }
}
