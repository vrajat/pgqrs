use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::dblock::DbLock;
use crate::store::s3::snapshot::SnapshotDb;
use crate::store::s3::{SyncDb, SyncState};
use crate::store::{ConcurrencyModel, DbOpFuture, DbTables};
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
impl DbLock for ConsistentDb {
    fn config(&self) -> &Config {
        self.inner.config()
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        self.concurrency_model
    }

    async fn with_read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> DbOpFuture<'a, R> + Send,
    {
        // Durable mode read path: verify local snapshot freshness against remote
        // before serving data. This preserves cross-process visibility guarantees
        // for handles that start from an older local cache.
        let _write_guard = self.inner.write_gate().lock().await;
        let state = self.inner.state().await?;
        let mut inner = self.inner.clone();
        match state {
            SyncState::InSync | SyncState::RemoteMissing { local_dirty: false } => {}
            SyncState::LocalMissing | SyncState::RemoteChanges => {
                SyncDb::snapshot(&mut inner).await?
            }
            SyncState::RemoteMissing { local_dirty: true }
            | SyncState::LocalChanges
            | SyncState::ConcurrentChanges => {
                SyncDb::sync(&mut inner).await.map_err(|err| match err {
                    Error::Conflict { .. } => Error::Conflict {
                        message: "Durable S3 read refused: local state is dirty and cannot be synchronized".to_string(),
                    },
                    other => other,
                })?
            }
        }
        self.inner.with_read(f).await
    }

    async fn with_write<R, F>(&self, f: F) -> Result<R>
    where
        R: Send,
        F: for<'a> FnOnce(&'a dyn DbTables) -> DbOpFuture<'a, R> + Send,
    {
        let _write_guard = self.inner.write_gate().lock().await;
        let out = self.inner.with_write(f).await?;
        let mut inner = self.inner.clone();
        SyncDb::sync(&mut inner).await?;
        Ok(out)
    }
}

#[async_trait]
impl SyncDb for ConsistentDb {
    async fn snapshot(&mut self) -> Result<()> {
        SyncDb::snapshot(&mut self.inner).await
    }

    async fn sync(&mut self) -> Result<()> {
        SyncDb::sync(&mut self.inner).await
    }
}
