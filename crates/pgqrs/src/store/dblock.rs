use crate::config::Config;
use crate::error::Result;
use crate::store::{
    ConcurrencyModel, DbStateTable, MessageTable, QueueTable, RunRecordTable, StepRecordTable,
    WorkerTable, WorkflowTable,
};
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub use crate::store::tables::Tables;

pub type StoreOpFuture<'a, R> = Pin<Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>>;

#[async_trait]
pub trait DbTables: Send + Sync + 'static {
    async fn execute_raw(&self, sql: &str) -> Result<()>;
    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> Result<()>;
    async fn execute_raw_with_two_i64(&self, sql: &str, param1: i64, param2: i64) -> Result<()>;
    async fn query_int(&self, sql: &str) -> Result<i64>;
    async fn query_string(&self, sql: &str) -> Result<String>;
    async fn query_bool(&self, sql: &str) -> Result<bool>;
    fn config(&self) -> &Config;
    fn concurrency_model(&self) -> ConcurrencyModel;
    fn queues(&self) -> &dyn QueueTable;
    fn messages(&self) -> &dyn MessageTable;
    fn workers(&self) -> &dyn WorkerTable;
    fn db_state(&self) -> &dyn DbStateTable;
    fn workflows(&self) -> &dyn WorkflowTable;
    fn workflow_runs(&self) -> &dyn RunRecordTable;
    fn workflow_steps(&self) -> &dyn StepRecordTable;
    async fn bootstrap(&self) -> Result<()>;
}

#[async_trait]
pub trait DbLock: Clone + Send + Sync + 'static {
    fn config(&self) -> &Config;
    fn concurrency_model(&self) -> ConcurrencyModel;

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> StoreOpFuture<'a, R> + Send;

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> StoreOpFuture<'a, R> + Send;
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
    DB: DbTables + Clone + Send + Sync + 'static,
{
    fn config(&self) -> &Config {
        self.inner.config()
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        DbTables::concurrency_model(&self.inner)
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> StoreOpFuture<'a, R> + Send,
    {
        f(&self.inner).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> StoreOpFuture<'a, R> + Send,
    {
        let _guard = self.write_gate.lock().await;
        f(&self.inner).await
    }
}
