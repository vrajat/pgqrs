use crate::config::Config;
use crate::error::{Error, Result};
use crate::store::{
    Admin, ArchiveTable, ConcurrencyModel, Consumer, MessageTable, Producer, QueueTable,
    StepResult, Store, Worker, WorkerTable, Workflow, WorkflowTable,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use turso::{Database, Row};
use std::sync::Arc;

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
        let builder = turso::Builder::new_local(dsn);
        let db = builder
            .build()
            .await
            .map_err(|e| Error::Internal {
                message: format!("Failed to connect to Turso: {}", e),
            })?;

        let db = Arc::new(db);

        let conn = db.connect().map_err(|e| Error::Internal {
            message: format!("Failed to get connection: {}", e),
        })?;

        const Q1: &str = include_str!("../../../migrations/turso/01_create_queues.sql");
        const Q2: &str = include_str!("../../../migrations/turso/02_create_workers.sql");
        const Q3: &str = include_str!("../../../migrations/turso/03_create_messages.sql");
        const Q4: &str = include_str!("../../../migrations/turso/04_create_archive.sql");
        const Q5: &str = include_str!("../../../migrations/turso/05_create_workflows.sql");
        const Q6: &str = include_str!("../../../migrations/turso/06_create_workflow_steps.sql");

        for (i, sql) in [Q1, Q2, Q3, Q4, Q5, Q6].iter().enumerate() {
            conn.execute(sql, ()).await.map_err(|e| Error::Internal {
                message: format!("Failed to run migration {}: {}", i + 1, e),
            })?;
        }

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
        .map_err(|e| Error::Internal {
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
        row.get(idx).map_err(|e| Error::Internal { message: e.to_string() })
    }
}

impl FromTursoRow for String {
    fn from_row(row: &Row, idx: usize) -> Result<Self> {
        row.get(idx).map_err(|e| Error::Internal { message: e.to_string() })
    }
}

impl FromTursoRow for bool {
    fn from_row(row: &Row, idx: usize) -> Result<Self> {
        let val: i64 = row.get(idx).map_err(|e| Error::Internal { message: e.to_string() })?;
        Ok(val != 0)
    }
}

pub struct TursoQueryBuilder {
    sql: String,
    params: Vec<turso::Value>,
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
        let conn = db.connect().map_err(|e| Error::Internal {
             message: format!("Connect failed: {}", e)
        })?;

        self.execute_on_connection(&conn).await
    }

    pub async fn execute_on_connection(self, conn: &turso::Connection) -> Result<u64> {
        let res = conn.execute(&self.sql, self.params).await.map_err(|e| Error::TursoQueryFailed {
            query: self.sql,
            source: e,
            context: "Execute on conn failed".into(),
        })?;
        Ok(res as u64)
    }

    pub async fn fetch_all(self, db: &Database) -> Result<Vec<Row>> {
        let conn = db.connect().map_err(|e| Error::Internal { message: e.to_string() })?;
        self.fetch_all_on_connection(&conn).await
    }

    pub async fn fetch_all_on_connection(self, conn: &turso::Connection) -> Result<Vec<Row>> {
        let mut rows = conn.query(&self.sql, self.params).await.map_err(|e| Error::TursoQueryFailed {
            query: self.sql.clone(),
            source: e,
            context: "Query on conn failed".into(),
        })?;

        let mut result = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            result.push(row);
        }
        Ok(result)
    }

    pub async fn fetch_one(self, db: &Database) -> Result<Row> {
        let conn = db.connect().map_err(|e| Error::Internal { message: e.to_string() })?;
        self.fetch_one_on_connection(&conn).await
    }

    pub async fn fetch_one_on_connection(self, conn: &turso::Connection) -> Result<Row> {
        let mut rows = conn.query(&self.sql, self.params).await.map_err(|e| Error::TursoQueryFailed {
            query: self.sql.clone(),
            source: e,
            context: "Query scalar on conn failed".into(),
        })?;

        if let Ok(Some(row)) = rows.next().await {
            Ok(row)
        } else {
             Err(Error::NotFound {
                 entity: "Row".into(),
                 id: "None".into(),
             })
        }
    }

    pub async fn fetch_optional(self, db: &Database) -> Result<Option<Row>> {
        let conn = db.connect().map_err(|e| Error::Internal { message: e.to_string() })?;
        self.fetch_optional_on_connection(&conn).await
    }

    pub async fn fetch_optional_on_connection(self, conn: &turso::Connection) -> Result<Option<Row>> {
         let mut rows = conn.query(&self.sql, self.params).await.map_err(|e| Error::TursoQueryFailed {
            query: self.sql.clone(),
            source: e,
            context: "Query optional on conn failed".into(),
        })?;

        if let Ok(Some(row)) = rows.next().await {
            Ok(Some(row))
        } else {
            Ok(None)
        }
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
    where T: FromTursoRow
    {
        let row = self.builder.fetch_one(db).await?;
        T::from_row(&row, 0)
    }

    pub async fn fetch_optional<T>(self, db: &Database) -> Result<Option<T>>
    where T: FromTursoRow {
         let row = self.builder.fetch_optional(db).await?;
         if let Some(r) = row {
             Ok(Some(T::from_row(&r, 0)?))
         } else {
             Ok(None)
         }
    }
}

pub fn query_scalar(sql: &str) -> GenericScalarBuilder {
    GenericScalarBuilder { builder: TursoQueryBuilder::new(sql) }
}

#[async_trait]
impl Store for TursoStore {
    type Db = sqlx::Sqlite;

    async fn execute_raw(&self, sql: &str) -> Result<()> {
        query(sql)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn execute_raw_with_i64(&self, sql: &str, param: i64) -> Result<()> {
        query(sql)
            .bind(param)
            .execute(&self.db)
            .await?;
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
        query_scalar(sql)
            .fetch_one(&self.db)
            .await
    }

    async fn query_string(&self, sql: &str) -> Result<String> {
        query_scalar(sql)
            .fetch_one(&self.db)
            .await
    }

    async fn query_bool(&self, sql: &str) -> Result<bool> {
        query_scalar(sql)
            .fetch_one(&self.db)
            .await
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

    async fn admin(&self, _config: &Config) -> Result<Box<dyn Admin>> {
        use self::worker::admin::TursoAdmin;
        let admin = TursoAdmin::new(self.db.clone(), 0);
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
            TursoConsumer::new(self.db.clone(), &queue_info.queue_name, hostname, port).await?;
        Ok(Box::new(consumer))
    }

    async fn producer_ephemeral(
        &self,
        queue_name: &str,
        config: &Config,
    ) -> Result<Box<dyn Producer>> {
        use self::worker::producer::TursoProducer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let producer =
            TursoProducer::new_ephemeral(self.db.clone(), &queue_info, config).await?;
        Ok(Box::new(producer))
    }

    async fn consumer_ephemeral(
        &self,
        queue_name: &str,
        config: &Config,
    ) -> Result<Box<dyn Consumer>> {
        use self::worker::consumer::TursoConsumer;
        let queue_info = self.queues.get_by_name(queue_name).await?;
        let consumer =
            TursoConsumer::new_ephemeral(self.db.clone(), &queue_info, config).await?;
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
