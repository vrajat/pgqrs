use crate::error::Result;
use crate::store::turso::{format_turso_timestamp, parse_turso_timestamp};
use crate::types::{ArchivedMessage, NewArchivedMessage, QueueMessage};
use async_trait::async_trait;
use std::sync::Arc;
use turso::Database;

const INSERT_ARCHIVED_MESSAGE: &str = r#"
    INSERT INTO pgqrs_archive (
        original_msg_id, queue_id, producer_worker_id, consumer_worker_id,
        payload, enqueued_at, vt, read_ct, dequeued_at, archived_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    RETURNING id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at;
"#;

const GET_ARCHIVED_MESSAGE_BY_ID: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at
    FROM pgqrs_archive
    WHERE id = ?;
"#;

const LIST_ALL_ARCHIVED_MESSAGES: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at
    FROM pgqrs_archive
    ORDER BY archived_at DESC;
"#;

const DELETE_ARCHIVED_MESSAGE_BY_ID: &str = r#"
    DELETE FROM pgqrs_archive
    WHERE id = ?;
"#;

const LIST_ARCHIVED_MESSAGES_BY_QUEUE: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at
    FROM pgqrs_archive
    WHERE queue_id = ?
    ORDER BY archived_at DESC;
"#;

const REPLAY_MESSAGE_INSERT: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt)
    VALUES (?, ?, 0, ?, ?)
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

#[derive(Debug, Clone)]
pub struct TursoArchiveTable {
    db: Arc<Database>,
}

impl TursoArchiveTable {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn map_row(row: &turso::Row) -> Result<ArchivedMessage> {
        let id: i64 = row.get(0)?;
        let original_msg_id: i64 = row.get(1)?;
        let queue_id: i64 = row.get(2)?;
        let producer_worker_id: Option<i64> = row.get(3)?;
        let consumer_worker_id: Option<i64> = row.get(4)?;

        let payload_str: String = row.get(5)?;
        let payload: serde_json::Value = serde_json::from_str(&payload_str)?;

        let enqueued_at_str: String = row.get(6)?;
        let enqueued_at = parse_turso_timestamp(&enqueued_at_str)?;

        let vt_str: Option<String> = row.get(7)?;
        let vt = match vt_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let read_ct: i32 = row.get(8)?;

        let dequeued_at_str: Option<String> = row.get(9)?;
        let dequeued_at = match dequeued_at_str {
            Some(s) => Some(parse_turso_timestamp(&s)?),
            None => None,
        };

        let archived_at_str: String = row.get(10)?;
        let archived_at = parse_turso_timestamp(&archived_at_str)?;

        Ok(ArchivedMessage {
            id,
            original_msg_id,
            queue_id,
            producer_worker_id,
            consumer_worker_id,
            payload,
            enqueued_at,
            vt: vt.unwrap_or(enqueued_at),
            read_ct,
            dequeued_at,
            archived_at,
        })
    }
}

#[async_trait]
impl crate::store::ArchiveTable for TursoArchiveTable {
    async fn insert(&self, data: NewArchivedMessage) -> Result<ArchivedMessage> {
        let payload_str = data.payload.to_string();
        let enqueued_at_str = format_turso_timestamp(&data.enqueued_at);
        let vt_str = format_turso_timestamp(&data.vt);
        let dequeued_at_str = data.dequeued_at.map(|t| format_turso_timestamp(&t));
        let now_str = format_turso_timestamp(&chrono::Utc::now());

        let row = crate::store::turso::query(INSERT_ARCHIVED_MESSAGE)
            .bind(data.original_msg_id)
            .bind(data.queue_id)
            .bind(match data.producer_worker_id {
                Some(id) => turso::Value::Integer(id),
                None => turso::Value::Null,
            })
            .bind(match data.consumer_worker_id {
                Some(id) => turso::Value::Integer(id),
                None => turso::Value::Null,
            })
            .bind(payload_str)
            .bind(enqueued_at_str)
            .bind(vt_str)
            .bind(data.read_ct)
            .bind(match dequeued_at_str {
                Some(s) => turso::Value::Text(s),
                None => turso::Value::Null,
            })
            .bind(now_str)
            .fetch_one(&self.db)
            .await?;

        Self::map_row(&row)
    }

    async fn get(&self, id: i64) -> Result<ArchivedMessage> {
        let row = crate::store::turso::query(GET_ARCHIVED_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.db)
            .await?;

        Self::map_row(&row)
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        let rows = crate::store::turso::query(LIST_ALL_ARCHIVED_MESSAGES)
            .fetch_all(&self.db)
            .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar("SELECT COUNT(*) FROM pgqrs_archive")
            .fetch_one(&self.db)
            .await?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let count = crate::store::turso::query(DELETE_ARCHIVED_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.db)
            .await?;
        Ok(count)
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        let rows = crate::store::turso::query(LIST_ARCHIVED_MESSAGES_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.db)
            .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let rows = crate::store::turso::query(
            "SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at FROM pgqrs_archive WHERE read_ct >= ? ORDER BY archived_at DESC LIMIT ? OFFSET ?"
        )
        .bind(max_attempts)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db)
        .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn dlq_count(&self, max_attempts: i32) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_archive WHERE read_ct >= ?",
        )
        .bind(max_attempts)
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let rows = crate::store::turso::query(
            "SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at FROM pgqrs_archive WHERE consumer_worker_id = ? ORDER BY archived_at DESC LIMIT ? OFFSET ?",
        )
        .bind(worker_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db)
        .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(&row)?);
        }
        Ok(messages)
    }

    async fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_archive WHERE consumer_worker_id = ?",
        )
        .bind(worker_id)
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }

    async fn delete_by_worker(&self, worker_id: i64) -> Result<u64> {
        let count =
            crate::store::turso::query("DELETE FROM pgqrs_archive WHERE consumer_worker_id = ?")
                .bind(worker_id)
                .execute(&self.db)
                .await?;
        Ok(count)
    }

    async fn replay_message(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        // Need manual transaction to delete from archive and insert into messages
        let conn = self
            .db
            .connect()
            .map_err(|e| crate::error::Error::Internal {
                message: e.to_string(),
            })?;

        conn.execute("BEGIN", ())
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BEGIN".into(),
                source: Box::new(e),
                context: "Begin replay".into(),
            })?;

        let row_opt = crate::store::turso::query(GET_ARCHIVED_MESSAGE_BY_ID)
            .bind(msg_id)
            .fetch_optional_on_connection(&conn)
            .await;

        let row_opt = match row_opt {
            Ok(res) => res,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        };

        if let Some(row) = row_opt {
            let archive_msg = Self::map_row(&row)?;
            let payload_str = archive_msg.payload.to_string();
            let now_str = format_turso_timestamp(&chrono::Utc::now());

            let msg_row = crate::store::turso::query(REPLAY_MESSAGE_INSERT)
                .bind(archive_msg.queue_id)
                .bind(payload_str)
                .bind(now_str.clone())
                .bind(now_str)
                .fetch_one_on_connection(&conn)
                .await;

            let msg_row = match msg_row {
                Ok(res) => res,
                Err(e) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(e);
                }
            };

            // Delete from archive
            let delete_res = crate::store::turso::query(DELETE_ARCHIVED_MESSAGE_BY_ID)
                .bind(msg_id)
                .execute_on_connection(&conn)
                .await;

            if let Err(e) = delete_res {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }

            conn.execute("COMMIT", ())
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "COMMIT".into(),
                    source: Box::new(e),
                    context: "Commit replay".into(),
                })?;

            let msg = crate::store::turso::tables::messages::TursoMessageTable::map_row(&msg_row)?;
            Ok(Some(msg))
        } else {
            conn.execute("ROLLBACK", ())
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: "ROLLBACK".into(),
                    source: Box::new(e),
                    context: "Rollback replay (not found)".into(),
                })?;
            Ok(None)
        }
    }

    async fn count_for_queue(&self, queue_id: i64) -> Result<i64> {
        let count: i64 = crate::store::turso::query_scalar(
            "SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = ?",
        )
        .bind(queue_id)
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }
}
