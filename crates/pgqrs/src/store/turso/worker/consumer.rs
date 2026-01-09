use crate::error::{Error, Result};
use crate::store::turso::tables::messages::TursoMessageTable;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use crate::store::{ArchiveTable, Consumer, MessageTable, Worker};
use crate::types::{ArchivedMessage, QueueInfo, QueueMessage, WorkerInfo, WorkerStatus};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use turso::Database;
use std::sync::Arc;

pub struct TursoConsumer {
    db: Arc<Database>,
    queue_id: i64,
    worker_id: i64,
    #[allow(dead_code)]
    worker_info: WorkerInfo,
    #[allow(dead_code)]
    queue_info: QueueInfo,
}

impl TursoConsumer {
    pub async fn new(
        db: Arc<Database>,
        queue: &str,
        hostname: &str,
        port: i32,
    ) -> Result<Self> {
         // Load queue
         let queue_row = crate::store::turso::query("SELECT id, queue_name, created_at FROM pgqrs_queues WHERE queue_name = $1")
            .bind(queue)
            .fetch_one(&db)
            .await?;

         let queue_id: i64 = queue_row.get(0)?;
         let queue_name: String = queue_row.get(1)?;
         let created_at_str: String = queue_row.get(2)?;
         let created_at = parse_turso_timestamp(&created_at_str)?;

         let queue_info = QueueInfo { id: queue_id, queue_name, created_at };

         // Register worker
         let worker_info = crate::store::WorkerTable::register(
             &crate::store::turso::tables::workers::TursoWorkerTable::new(db.clone()),
             Some(queue_id),
             hostname,
             port
         ).await?;

         Ok(Self {
             db,
             queue_id,
             worker_id: worker_info.id,
             worker_info,
             queue_info,
         })
    }

    pub async fn new_ephemeral(
        db: Arc<Database>,
        queue_info: &QueueInfo,
        _config: &crate::config::Config,
    ) -> Result<Self> {
         let worker_info = crate::store::WorkerTable::register_ephemeral(
             &crate::store::turso::tables::workers::TursoWorkerTable::new(db.clone()),
             Some(queue_info.id),
         ).await?;

         Ok(Self {
             db,
             queue_id: queue_info.id,
             worker_id: worker_info.id,
             worker_info,
             queue_info: queue_info.clone(),
         })
    }
}

#[async_trait]
impl Worker for TursoConsumer {
    fn worker_id(&self) -> i64 {
        self.worker_id
    }

    async fn status(&self) -> Result<WorkerStatus> {
        let admin = crate::store::turso::worker::admin::TursoAdmin::new(self.db.clone(), self.worker_id);
        admin.status().await
    }

    async fn suspend(&self) -> Result<()> {
        let admin = crate::store::turso::worker::admin::TursoAdmin::new(self.db.clone(), self.worker_id);
        admin.suspend().await
    }

    async fn resume(&self) -> Result<()> {
        let admin = crate::store::turso::worker::admin::TursoAdmin::new(self.db.clone(), self.worker_id);
        admin.resume().await
    }

    async fn shutdown(&self) -> Result<()> {
        let admin = crate::store::turso::worker::admin::TursoAdmin::new(self.db.clone(), self.worker_id);
        admin.shutdown().await
    }

    async fn heartbeat(&self) -> Result<()> {
        let admin = crate::store::turso::worker::admin::TursoAdmin::new(self.db.clone(), self.worker_id);
        admin.heartbeat().await
    }

    async fn is_healthy(&self, max_age: Duration) -> Result<bool> {
        let admin = crate::store::turso::worker::admin::TursoAdmin::new(self.db.clone(), self.worker_id);
        admin.is_healthy(max_age).await
    }
}

#[async_trait]
impl Consumer for TursoConsumer {
    async fn dequeue(&self) -> Result<Vec<QueueMessage>> {
        self.dequeue_many(1).await
    }

    async fn dequeue_many(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(limit, 30).await
    }

    async fn dequeue_delay(&self, vt: u32) -> Result<Vec<QueueMessage>> {
        self.dequeue_many_with_delay(1, vt).await
    }

    async fn dequeue_many_with_delay(
        &self,
        limit: usize,
        vt: u32,
    ) -> Result<Vec<QueueMessage>> {
        let now = Utc::now();
        self.dequeue_at(limit, vt, now).await
    }

    async fn dequeue_at(
        &self,
        limit: usize,
        vt: u32,
        now: DateTime<Utc>,
    ) -> Result<Vec<QueueMessage>> {
        // Implement Manual Transaction Dequeue
        let now_str = format_turso_timestamp(&now);
        let conn = self.db.connect().map_err(|e| Error::Internal { message: e.to_string() })?;

        conn.execute("BEGIN", ()).await.map_err(|e| Error::TursoQueryFailed { query: "BEGIN".into(), source: e, context: "Dequeue start".into() })?;

        // 1. Select candidates
        let sql_select = r#"
            SELECT id
            FROM pgqrs_messages
            WHERE queue_id = ? AND (vt IS NULL OR vt <= ?) AND consumer_worker_id IS NULL
            ORDER BY enqueued_at ASC
            LIMIT ?
        "#;

        let mut grabbed_ids = Vec::new();

        let rows = crate::store::turso::query(sql_select)
            .bind(self.queue_id)
            .bind(now_str.clone())
            .bind(limit as i64)
            .fetch_all_on_connection(&conn)
            .await
            .map_err(|e| { let _ = conn.execute("ROLLBACK", ()); e })?;

        for row in rows {
            let id: i64 = row.get(0)?;
            grabbed_ids.push(id);
        }

        if grabbed_ids.is_empty() {
             conn.execute("ROLLBACK", ()).await.map_err(|e| Error::TursoQueryFailed { query: "ROLLBACK".into(), source: e, context: "Dequeue empty".into() })?;
             return Ok(vec![]);
        }

        // 2. Update them
        let new_vt = now + chrono::Duration::seconds(vt as i64);
        let new_vt_str = format_turso_timestamp(&new_vt);

        for id in &grabbed_ids {
            crate::store::turso::query("UPDATE pgqrs_messages SET consumer_worker_id = ?, vt = ?, read_ct = read_ct + 1, dequeued_at = ? WHERE id = ?")
                .bind(self.worker_id)
                .bind(new_vt_str.clone())
                .bind(now_str.clone())
                .bind(*id)
                .execute_on_connection(&conn)
                .await
                .map_err(|e| { let _ = conn.execute("ROLLBACK", ()); e})?;
        }

        conn.execute("COMMIT", ()).await.map_err(|e| Error::TursoQueryFailed { query: "COMMIT".into(), source: e, context: "Dequeue commit".into() })?;

        // 3. Fetch full objects
        let msg_table = TursoMessageTable::new(self.db.clone());
        msg_table.get_by_ids(&grabbed_ids).await
    }

    async fn extend_visibility(
        &self,
        message_id: i64,
        additional_seconds: u32,
    ) -> Result<bool> {
        let msg_table = TursoMessageTable::new(self.db.clone());
        let count = msg_table.extend_visibility(message_id, self.worker_id, additional_seconds).await?;
        Ok(count > 0)
    }

    async fn delete(&self, message_id: i64) -> Result<bool> {
        let msg_table = TursoMessageTable::new(self.db.clone());
        let count = msg_table.delete(message_id).await?;
        Ok(count > 0)
    }

    async fn delete_many(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        let msg_table = TursoMessageTable::new(self.db.clone());
        msg_table.delete_by_ids(&message_ids).await
    }

    async fn archive(&self, msg_id: i64) -> Result<Option<ArchivedMessage>> {
         // Manual Transaction: Select, Insert Archive, Delete Message
         let conn = self.db.connect().map_err(|e| Error::Internal { message: e.to_string() })?;
         conn.execute("BEGIN", ()).await.map_err(|e| Error::TursoQueryFailed { query: "BEGIN".into(), source: e, context: "Archive start".into() })?;

         let msg_row_opt = crate::store::turso::query("SELECT id, queue_id, payload, enqueued_at, vt, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE id = ?")
             .bind(msg_id)
             .fetch_optional_on_connection(&conn)
             .await
             .map_err(|e| { let _ = conn.execute("ROLLBACK", ()); e })?;

         if let Some(row) = msg_row_opt {
              let payload_str: String = row.get(2)?;
              let now_str = format_turso_timestamp(&Utc::now());

              // Insert Archive
              let archive_row = crate::store::turso::query(r#"
                  INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at)
                  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                  RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at;
              "#)
              .bind(msg_id)
              .bind(row.get::<i64>(1)?)
              .bind(row.get::<Option<i64>>(7)?)
              .bind(row.get::<Option<i64>>(8)?)
              .bind(payload_str)
              .bind(row.get::<String>(3)?)
              .bind(row.get::<Option<String>>(4)?)
              .bind(row.get::<i32>(5)?)
              .bind(row.get::<Option<String>>(6)?)
              .bind(now_str)
              .fetch_one_on_connection(&conn)
              .await
             .map_err(|e| { let _ = conn.execute("ROLLBACK", ()); e })?;

             // Delete Original
             crate::store::turso::query("DELETE FROM pgqrs_messages WHERE id = ?")
                 .bind(msg_id)
                 .execute_on_connection(&conn)
                 .await
                 .map_err(|e| { let _ = conn.execute("ROLLBACK", ()); e })?;

             conn.execute("COMMIT", ()).await.map_err(|e| Error::TursoQueryFailed { query: "COMMIT".into(), source: e, context: "Archive commit".into() })?;

             let archive_table = crate::store::turso::tables::archive::TursoArchiveTable::new(self.db.clone());

             let id: i64 = archive_row.get(0)?;
             archive_table.get(id).await.map(Some)

         } else {
              conn.execute("ROLLBACK", ()).await.map_err(|e| Error::TursoQueryFailed { query: "ROLLBACK".into(), source: e, context: "Archive not found".into() })?;
              Ok(None)
         }
    }

    async fn archive_many(&self, msg_ids: Vec<i64>) -> Result<Vec<bool>> {
        let mut results = Vec::new();
        for id in msg_ids {
            results.push(self.archive(id).await?.is_some());
        }
        Ok(results)
    }

    async fn release_messages(&self, message_ids: &[i64]) -> Result<u64> {
         let msg_table = TursoMessageTable::new(self.db.clone());
         let res = msg_table.release_messages_by_ids(message_ids, self.worker_id).await?;
         Ok(res.iter().filter(|&&x| x).count() as u64)
    }
}
