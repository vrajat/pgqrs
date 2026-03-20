use crate::config::Config;
use crate::error::Result;
use crate::store::{ConcurrencyModel, Store};
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub use crate::store::s3::tables::Tables;

pub type StoreOpFuture<'a, R> = Pin<Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>>;

#[async_trait]
pub trait DbLock: Clone + Send + Sync + 'static {
    fn config(&self) -> &Config;
    fn concurrency_model(&self) -> ConcurrencyModel;

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send;

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send;

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send;

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send;
}

#[derive(Debug, Clone)]
pub struct SerializedLock<DB> {
    inner: DB,
    write_gate: Arc<Mutex<()>>,
}

impl<DB> SerializedLock<DB> {
    pub fn new(inner: DB) -> Self {
        Self {
            inner,
            write_gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn inner(&self) -> &DB {
        &self.inner
    }
}

#[async_trait]
impl<DB> DbLock for SerializedLock<DB>
where
    DB: Store + Clone + Send + Sync + 'static,
{
    fn config(&self) -> &Config {
        self.inner.config()
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.inner.concurrency_model()
    }

    fn with_read_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        f(&self.inner)
    }

    fn with_write_ref<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Store) -> R + Send,
    {
        let _guard = self.write_gate.blocking_lock();
        f(&self.inner)
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        f(&self.inner).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn Store) -> StoreOpFuture<'a, R> + Send,
    {
        let _guard = self.write_gate.lock().await;
        f(&self.inner).await
    }
}
