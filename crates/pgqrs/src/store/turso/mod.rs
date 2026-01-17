use crate::config::Config;
use crate::error::Result;
use crate::store::{
    Admin, ArchiveTable, BackendType, ConcurrencyModel, Consumer, MessageTable, Producer,
    QueueTable, StepResult, Store, Worker, WorkerTable, Workflow, WorkflowTable,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use turso::{Database, Row};

pub mod tables;
pub mod worker;
pub mod workflow;

use self::tables::archive::TursoArchiveTable;
use self::tables::messages::TursoMessageTable;
use self::tables::queues::TursoQueueTable;
use self::tables::workers::TursoWorkerTable;
use self::tables::workflows::TursoWorkflowTable;

#[derive(Debug, Clone)]
pub struct TursoStore {
    db: Arc<Database>,
    config: Config,
    queues: Arc<TursoQueueTable>,
    messages: Arc<TursoMessageTable>,
    workers: Arc<TursoWorkerTable>,
    archive: Arc<TursoArchiveTable>,
    workflows: Arc<TursoWorkflowTable>,
}

impl TursoStore {
    pub async fn new(dsn: &str, config: &Config) -> Result<Self> {
        let path = BackendType::TURSO_PREFIXES
            .iter()
            .find_map(|prefix| dsn.strip_prefix(prefix))
            .ok_or_else(|| crate::error::Error::InvalidConfig {
                field: "dsn".to_string(),
                message: "Unsupported DSN format: <redacted>".to_string(),
            })?;
        let builder = turso::Builder::new_local(path);
        let db = builder
            .build()
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to connect to Turso: {}", e),
            })?;

        let db = Arc::new(db);

        let conn = db.connect().map_err(|e| crate::error::Error::Internal {
            message: format!("Failed to get connection: {}", e),
        })?;

        // Enable WAL mode and busy timeout for better concurrency in local mode
        // PRAGMA journal_mode returns a row, so we use query() to avoid "unexpected row" error
        let mut rows = conn
            .query("PRAGMA journal_mode=WAL;", ())
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to set WAL mode: {}", e),
            })?;
        while rows
            .next()
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to consume WAL pragma result: {}", e),
            })?
            .is_some()
        {}

        conn.execute("PRAGMA busy_timeout = 5000;", ())
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to set busy timeout: {}", e),
            })?;

        // Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON;", ())
            .await
            .map_err(|e| crate::error::Error::Internal {
                message: format!("Failed to set foreign_keys: {}", e),
            })?;

        Ok(Self {
            db: Arc::clone(&db),
            config: config.clone(),
            queues: Arc::new(TursoQueueTable::new(Arc::clone(&db))),
            messages: Arc::new(TursoMessageTable::new(Arc::clone(&db))),
            workers: Arc::new(TursoWorkerTable::new(Arc::clone(&db))),
            archive: Arc::new(TursoArchiveTable::new(Arc::clone(&db))),
            workflows: Arc::new(TursoWorkflowTable::new(Arc::clone(&db))),
        })
    }
}

pub fn parse_turso_timestamp(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| crate::error::Error::Internal {
            message: format!("Invalid timestamp: {}", e),
        })
}

pub fn format_turso_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub trait FromTursoRow: Sized {
    fn from_row(row: &Row, idx: usize) -> Result<Self>;
}

impl FromTursoRow for i64 {
    fn from_row(row: &Row, idx: usize) -> Result<Self> {
        row.get(idx).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })
    }
}

impl FromTursoRow for String {
    fn from_row(row: &Row, idx: usize) -> Result<Self> {
        row.get(idx).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })
    }
}

impl FromTursoRow for bool {
    fn from_row(row: &Row, idx: usize) -> Result<Self> {
        let val: i64 = row.get(idx).map_err(|e| crate::error::Error::Internal {
            message: e.to_string(),
        })?;
        Ok(val != 0)
    }
}

pub struct TursoQueryBuilder {
    sql: String,
    params: Vec<turso::Value>,
}

pub async fn connect_db(db: &Database) -> Result<turso::Connection> {
    let conn = db.connect().map_err(|e| crate::error::Error::Internal {
        message: format!("Connect failed: {}", e),
    })?;

    // precise busy_timeout for every connection to handle concurrency
    conn.execute("PRAGMA busy_timeout = 5000;", ())
        .await
        .map_err(|e| crate::error::Error::Internal {
            message: format!("Failed to set busy timeout: {}", e),
        })?;

    conn.execute("PRAGMA foreign_keys = ON;", ())
        .await
        .map_err(|e| crate::error::Error::Internal {
            message: format!("Failed to set foreign_keys: {}", e),
        })?;

    Ok(conn)
}

impl TursoQueryBuilder {
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            params: Vec::new(),
        }
    }

    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: Into<turso::Value>,
    {
        self.params.push(value.into());
        self
    }

    pub async fn execute(self, db: &Database) -> Result<u64> {
        let conn = connect_db(db).await?;
        self.execute_on_connection(&conn).await
    }

    pub async fn execute_on_connection(self, conn: &turso::Connection) -> Result<u64> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 10;
        let mut delay = 50u64; // ms
        const MAX_DELAY: u64 = 5000;

        loop {
            let res = conn.execute(&self.sql, self.params.clone()).await;

            match res {
                Ok(count) => return Ok(count),
                Err(e) => {
                    let msg = e.to_string();
                    let is_locked = msg.contains("database is locked")
                        || msg.contains("SQLITE_BUSY")
                        || msg.contains("snapshot is stale");

                    if is_locked && retries < MAX_RETRIES {
                        retries += 1;

                        // Add jitter: +/- 10%
                        let jitter = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .subsec_nanos()
                            % 20) as i64
                            - 10;
                        let jittered_delay = (delay as i64 + jitter).max(1) as u64;

                        tracing::warn!(
                            "Database locked, retrying {}/{} in {}ms: {}",
                            retries,
                            MAX_RETRIES,
                            jittered_delay,
                            self.sql
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(jittered_delay))
                            .await;
                        delay = delay.saturating_mul(2).min(MAX_DELAY);
                        continue;
                    }

                    return Err(crate::error::Error::QueryFailed {
                        query: self.sql,
                        source: Box::new(e),
                        context: if is_locked {
                            "Execute on conn failed (locked)".into()
                        } else {
                            "Execute on conn failed".into()
                        },
                    });
                }
            }
        }
    }

    pub async fn fetch_all(self, db: &Database) -> Result<Vec<Row>> {
        let conn = connect_db(db).await?;
        self.fetch_all_on_connection(&conn).await
    }

    pub async fn fetch_all_on_connection(self, conn: &turso::Connection) -> Result<Vec<Row>> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 10;
        let mut delay = 50u64; // ms
        const MAX_DELAY: u64 = 5000;

        loop {
            let res = conn.query(&self.sql, self.params.clone()).await;

            match res {
                Ok(mut rows) => {
                    let mut result = Vec::new();
                    let mut loop_err = None;

                    loop {
                        match rows.next().await {
                            Ok(Some(row)) => result.push(row),
                            Ok(None) => break,
                            Err(e) => {
                                loop_err = Some(e);
                                break;
                            }
                        }
                    }

                    if let Some(e) = loop_err {
                        let msg = e.to_string();
                        let is_locked = msg.contains("database is locked")
                            || msg.contains("SQLITE_BUSY")
                            || msg.contains("snapshot is stale");

                        if is_locked && retries < MAX_RETRIES {
                            retries += 1;

                            // Add jitter: +/- 10%
                            let jitter = (std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .subsec_nanos()
                                % 20) as i64
                                - 10;
                            let jittered_delay = (delay as i64 + jitter).max(1) as u64;

                            tracing::warn!(
                                "Database locked during fetch, retrying {}/{} in {}ms: {}",
                                retries,
                                MAX_RETRIES,
                                jittered_delay,
                                self.sql
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(jittered_delay))
                                .await;
                            delay = delay.saturating_mul(2).min(MAX_DELAY);
                            continue;
                        }

                        return Err(crate::error::Error::QueryFailed {
                            query: self.sql.clone(),
                            source: Box::new(e),
                            context: "Next row failed".into(),
                        });
                    }

                    return Ok(result);
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_locked = msg.contains("database is locked")
                        || msg.contains("SQLITE_BUSY")
                        || msg.contains("snapshot is stale");

                    if is_locked && retries < MAX_RETRIES {
                        retries += 1;

                        // Add jitter: +/- 10%
                        let jitter = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .subsec_nanos()
                            % 20) as i64
                            - 10;
                        let jittered_delay = (delay as i64 + jitter).max(1) as u64;

                        tracing::warn!(
                            "Database locked query start, retrying {}/{} in {}ms: {}",
                            retries,
                            MAX_RETRIES,
                            jittered_delay,
                            self.sql
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(jittered_delay))
                            .await;
                        delay = delay.saturating_mul(2).min(MAX_DELAY);
                        continue;
                    }

                    return Err(crate::error::Error::QueryFailed {
                        query: self.sql.clone(),
                        source: Box::new(e),
                        context: "Query on conn failed".into(),
                    });
                }
            }
        }
    }

    pub async fn fetch_one(self, db: &Database) -> Result<Row> {
        let conn = connect_db(db).await?;
        self.fetch_one_on_connection(&conn).await
    }

    pub async fn fetch_one_on_connection(self, conn: &turso::Connection) -> Result<Row> {
        let rows = self.fetch_all_on_connection(conn).await?;
        if rows.is_empty() {
            Err(crate::error::Error::NotFound {
                entity: "Row".into(),
                id: "None".into(),
            })
        } else {
            Ok(rows.into_iter().next().unwrap())
        }
    }

    pub async fn fetch_optional(self, db: &Database) -> Result<Option<Row>> {
        let conn = connect_db(db).await?;
        self.fetch_optional_on_connection(&conn).await
    }

    pub async fn fetch_optional_on_connection(
        self,
        conn: &turso::Connection,
    ) -> Result<Option<Row>> {
        let rows = self.fetch_all_on_connection(conn).await?;
        Ok(rows.into_iter().next())
    }
}

pub fn query(sql: &str) -> TursoQueryBuilder {
    TursoQueryBuilder::new(sql)
}

pub struct GenericScalarBuilder {
    builder: TursoQueryBuilder,
}

impl GenericScalarBuilder {
    pub fn bind<V: Into<turso::Value>>(mut self, value: V) -> Self {
        self.builder = self.builder.bind(value);
        self
    }

    pub async fn fetch_one<T>(self, db: &Database) -> Result<T>
    where
        T: FromTursoRow,
    {
        let row = self.builder.fetch_one(db).await?;
        T::from_row(&row, 0)
    }

    pub async fn fetch_optional<T>(self, db: &Database) -> Result<Option<T>>
    where
        T: FromTursoRow,
    {
        let row = self.builder.fetch_optional(db).await?;
        if let Some(r) = row {
            Ok(Some(T::from_row(&r, 0)?))
        } else {
            Ok(None)
        }
    }
}

pub fn query_scalar(sql: &str) -> GenericScalarBuilder {
    GenericScalarBuilder {
        builder: TursoQueryBuilder::new(sql),
    }
}

#[async_trait]
impl Store for TursoStore {
    async fn execute_raw(&self, sql: &str) -> Result<()> {
        query(sql).execute(&self.db).await?;
        Ok(())
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> Result<()> {
        query(sql).bind(param).execute(&self.db).await?;
        Ok(())
    }

    async fn execute_raw_with_two_i64(&self, sql: &str, param1: i64, param2: i64) -> Result<()> {
        query(sql)
            .bind(param1)
            .bind(param2)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn query_int(&self, sql: &str) -> Result<i64> {
        query_scalar(sql).fetch_one(&self.db).await
    }

    async fn query_string(&self, sql: &str) -> Result<String> {
        query_scalar(sql).fetch_one(&self.db).await
    }

    async fn query_bool(&self, sql: &str) -> Result<bool> {
        query_scalar(sql).fetch_one(&self.db).await
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn queues(&self) -> &dyn QueueTable {
        self.queues.as_ref()
    }

    fn messages(&self) -> &dyn MessageTable {
        self.messages.as_ref()
    }

    fn workers(&self) -> &dyn WorkerTable {
        self.workers.as_ref()
    }

    fn archive(&self) -> &dyn ArchiveTable {
        self.archive.as_ref()
    }

    fn workflows(&self) -> &dyn WorkflowTable {
        self.workflows.as_ref()
    }

    async fn create_workflow<T: serde::Serialize + Send + Sync>(
        &self,
        name: &str,
        input: &T,
    ) -> Result<Box<dyn Workflow>> {
        use self::workflow::handle::TursoWorkflow;
        let workflow = TursoWorkflow::create(self.db.clone(), name, input).await?;
        Ok(Box::new(workflow))
    }

    async fn acquire_step(
        &self,
        workflow_id: i64,
        step_id: &str,
    ) -> Result<StepResult<serde_json::Value>> {
        use self::workflow::guard::TursoStepGuard;
        TursoStepGuard::acquire(&self.db, workflow_id, step_id).await
    }

    async fn admin(&self, config: &Config) -> Result<Box<dyn Admin>> {
        use self::worker::admin::TursoAdmin;
        let admin = TursoAdmin::new(self.db.clone(), 0, config.clone());
        Ok(Box::new(admin))
    }

    async fn producer(
        &self,
        queue_name: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<Box<dyn Producer>> {
        use self::worker::producer::TursoProducer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let producer =
            TursoProducer::new(self.db.clone(), &queue_info, hostname, port, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer(
        &self,
        queue_name: &str,
        hostname: &str,
        port: i32,
        config: &Config,
    ) -> Result<Box<dyn Consumer>> {
        use self::worker::consumer::TursoConsumer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let consumer =
            TursoConsumer::new(self.db.clone(), &queue_info, hostname, port, config.clone())
                .await?;
        Ok(Box::new(consumer))
    }

    async fn producer_ephemeral(
        &self,
        queue_name: &str,
        config: &Config,
    ) -> Result<Box<dyn Producer>> {
        use self::worker::producer::TursoProducer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let producer = TursoProducer::new_ephemeral(self.db.clone(), &queue_info, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer_ephemeral(
        &self,
        queue_name: &str,
        config: &Config,
    ) -> Result<Box<dyn Consumer>> {
        use self::worker::consumer::TursoConsumer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let consumer = TursoConsumer::new_ephemeral(self.db.clone(), &queue_info, config).await?;
        Ok(Box::new(consumer))
    }

    fn workflow(&self, id: i64) -> Box<dyn Workflow> {
        use self::workflow::handle::TursoWorkflow;
        Box::new(TursoWorkflow::new(self.db.clone(), id))
    }

    fn worker(&self, id: i64) -> Box<dyn Worker> {
        use self::worker::TursoWorkerHandle;
        Box::new(TursoWorkerHandle::new(self.db.clone(), id))
    }

    fn concurrency_model(&self) -> ConcurrencyModel {
        ConcurrencyModel::SingleProcess
    }

    fn backend_name(&self) -> &'static str {
        "turso"
    }
}
