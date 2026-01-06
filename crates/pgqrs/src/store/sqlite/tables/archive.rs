use crate::error::Result;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::{ArchivedMessage, NewArchivedMessage, QueueMessage};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{Row, SqlitePool};

const INSERT_ARCHIVE: &str = r#"
    INSERT INTO pgqrs_archive (original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, dequeued_at, archived_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    RETURNING id;
"#;

const GET_ARCHIVE_BY_ID: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE id = $1;
"#;

const LIST_ALL_ARCHIVE: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    ORDER BY archived_at DESC;
"#;

const LIST_ARCHIVE_BY_QUEUE: &str = r#"
    SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt, read_ct, archived_at, dequeued_at
    FROM pgqrs_archive
    WHERE queue_id = $1
    ORDER BY archived_at DESC;
"#;

const DELETE_ARCHIVE_BY_ID: &str = r#"
    DELETE FROM pgqrs_archive WHERE id = $1;
"#;

#[derive(Debug, Clone)]
pub struct SqliteArchiveTable {
    pool: SqlitePool,
}

impl SqliteArchiveTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<ArchivedMessage> {
        let id: i64 = row.try_get("id")?;
        let original_msg_id: i64 = row.try_get("original_msg_id")?;
        let queue_id: i64 = row.try_get("queue_id")?;
        let producer_worker_id: Option<i64> = row.try_get("producer_worker_id")?;
        let consumer_worker_id: Option<i64> = row.try_get("consumer_worker_id")?;

        let payload_str: String = row.try_get("payload")?;
        let payload: Value = serde_json::from_str(&payload_str)?;

        let enqueued_at = parse_sqlite_timestamp(&row.try_get::<String, _>("enqueued_at")?)?;
        let vt = parse_sqlite_timestamp(&row.try_get::<String, _>("vt")?)?;
        let read_ct: i32 = row.try_get("read_ct")?;
        let archived_at = parse_sqlite_timestamp(&row.try_get::<String, _>("archived_at")?)?;

        let dequeued_at_str: Option<String> = row.try_get("dequeued_at")?;
        let dequeued_at = match dequeued_at_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        Ok(ArchivedMessage {
            id,
            original_msg_id,
            queue_id,
            producer_worker_id,
            consumer_worker_id,
            payload,
            enqueued_at,
            vt,
            read_ct,
            archived_at,
            dequeued_at,
        })
    }
}

#[async_trait]
impl crate::store::ArchiveTable for SqliteArchiveTable {
    async fn insert(&self, data: NewArchivedMessage) -> Result<ArchivedMessage> {
        let archived_at = Utc::now();
        let archived_at_str = format_sqlite_timestamp(&archived_at);
        let dequeued_at_str = data.dequeued_at.map(|d| format_sqlite_timestamp(&d));

        let id: i64 = sqlx::query_scalar(INSERT_ARCHIVE)
            .bind(data.original_msg_id)
            .bind(data.queue_id)
            .bind(data.producer_worker_id)
            .bind(data.consumer_worker_id)
            .bind(data.payload.to_string())
            .bind(format_sqlite_timestamp(&data.enqueued_at))
            .bind(format_sqlite_timestamp(&data.vt))
            .bind(data.read_ct)
            .bind(dequeued_at_str)
            .bind(archived_at_str)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_ARCHIVE".into(),
                source: e,
                context: format!("Failed to archive message {}", data.original_msg_id),
            })?;

        Ok(ArchivedMessage {
            id,
            original_msg_id: data.original_msg_id,
            queue_id: data.queue_id,
            producer_worker_id: data.producer_worker_id,
            consumer_worker_id: data.consumer_worker_id,
            payload: data.payload,
            enqueued_at: data.enqueued_at,
            vt: data.vt,
            read_ct: data.read_ct,
            archived_at,
            dequeued_at: data.dequeued_at,
        })
    }

    async fn get(&self, id: i64) -> Result<ArchivedMessage> {
        let row = sqlx::query(GET_ARCHIVE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_ARCHIVE_BY_ID ({})", id),
                source: e,
                context: format!("Failed to get archived message {}", id),
            })?;
        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<ArchivedMessage>> {
        let rows = sqlx::query(LIST_ALL_ARCHIVE)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_ARCHIVE".into(),
                source: e,
                context: "Failed to list all archived messages".into(),
            })?;

        let mut archives = Vec::with_capacity(rows.len());
        for row in rows {
            archives.push(Self::map_row(row)?);
        }
        Ok(archives)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_archive")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_ARCHIVE".into(),
                source: e,
                context: "Failed to count archived messages".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_ARCHIVE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_ARCHIVE_BY_ID ({})", id),
                source: e,
                context: format!("Failed to delete archive {}", id),
            })?;
        Ok(result.rows_affected())
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<ArchivedMessage>> {
        let rows = sqlx::query(LIST_ARCHIVE_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_ARCHIVE_BY_QUEUE ({})", queue_id),
                source: e,
                context: format!("Failed to list archives for queue {}", queue_id),
            })?;

        let mut archives = Vec::with_capacity(rows.len());
        for row in rows {
            archives.push(Self::map_row(row)?);
        }
        Ok(archives)
    }

    async fn list_dlq_messages(
        &self,
        max_attempts: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let sql = r#"
            SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt,
                   read_ct, archived_at, dequeued_at
            FROM pgqrs_archive
            WHERE read_ct >= $1
              AND consumer_worker_id IS NULL
              AND dequeued_at IS NULL
            ORDER BY archived_at DESC
            LIMIT $2 OFFSET $3;
        "#;

        let rows = sqlx::query(sql)
            .bind(max_attempts)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_DLQ_MESSAGES".into(),
                source: e,
                context: format!(
                    "Failed to list DLQ messages (max_attempts={})",
                    max_attempts
                ),
            })?;

        let mut archives = Vec::with_capacity(rows.len());
        for row in rows {
            archives.push(Self::map_row(row)?);
        }
        Ok(archives)
    }

    async fn dlq_count(&self, max_attempts: i32) -> Result<i64> {
        let sql = r#"
            SELECT COUNT(*)
            FROM pgqrs_archive
            WHERE read_ct >= $1
              AND consumer_worker_id IS NULL
              AND dequeued_at IS NULL;
        "#;

        let count: i64 = sqlx::query_scalar(sql)
            .bind(max_attempts)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_DLQ_MESSAGES".into(),
                source: e,
                context: format!(
                    "Failed to count DLQ messages (max_attempts={})",
                    max_attempts
                ),
            })?;
        Ok(count)
    }

    async fn list_by_worker(
        &self,
        worker_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ArchivedMessage>> {
        let sql = r#"
            SELECT id, original_msg_id, queue_id, producer_worker_id, consumer_worker_id, payload, enqueued_at, vt,
                   read_ct, archived_at, dequeued_at
            FROM pgqrs_archive
            WHERE consumer_worker_id = $1
            ORDER BY archived_at DESC
            LIMIT $2 OFFSET $3
        "#;
        let rows = sqlx::query(sql)
            .bind(worker_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("ARCHIVE_LIST_WITH_WORKER ({})", worker_id),
                source: e,
                context: format!("Failed to list archives for worker {}", worker_id),
            })?;

        let mut archives = Vec::with_capacity(rows.len());
        for row in rows {
            archives.push(Self::map_row(row)?);
        }
        Ok(archives)
    }

    async fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let sql = "SELECT COUNT(*) FROM pgqrs_archive WHERE consumer_worker_id = $1";
        let count: i64 = sqlx::query_scalar(sql)
            .bind(worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("ARCHIVE_COUNT_WITH_WORKER ({})", worker_id),
                source: e,
                context: format!("Failed to count archives for worker {}", worker_id),
            })?;
        Ok(count)
    }

    async fn delete_by_worker(&self, worker_id: i64) -> Result<u64> {
        let sql = "DELETE FROM pgqrs_archive WHERE consumer_worker_id = $1";
        let result = sqlx::query(sql)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "ARCHIVE_DELETE_WITH_WORKER".into(),
                source: e,
                context: format!("Failed to delete archives for worker {}", worker_id),
            })?;
        Ok(result.rows_affected())
    }

    async fn replay_message(&self, msg_id: i64) -> Result<Option<QueueMessage>> {
        // Use CTE to delete from archive and insert into messages
        // SQLite 3.35+ supports basic RETURNING.
        // It does NOT support data-modifying CTEs in the way Postgres does (WHERE ... IN (DELETE ...)).
        // SQLite support for DELETE ... RETURNING is top-level.
        // Usage inside CTE is NOT supported for DELETE.
        // So we must do this in two steps: Fetch, Delete, Insert. Or Transaction.
        // Transaction is best.

        /*
        Postgres query:
            WITH archived AS (
                DELETE FROM pgqrs_archive WHERE id = $1 RETURNING *
            )
            INSERT ... SELECT ... FROM archived ...

        SQLite approach (Transaction):
            BEGIN;
            SELECT * FROM pgqrs_archive WHERE id = $?;
            DELETE FROM pgqrs_archive WHERE id = $?;
            INSERT INTO pgqrs_messages ...;
            COMMIT;
        */

        let mut tx =
            self.pool
                .begin()
                .await
                .map_err(|e| crate::error::Error::TransactionFailed {
                    source: e,
                    context: "Failed to begin transaction for replay".into(),
                })?;

        // 1. Get the archived message
        let row = sqlx::query(GET_ARCHIVE_BY_ID)
            .bind(msg_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_ARCHIVE_BY_ID_TX".into(),
                source: e,
                context: format!("Failed to get archived message {}", msg_id),
            })?;

        let archive = match row {
            Some(r) => Self::map_row(r)?,
            None => return Ok(None),
        };

        // 2. Delete from archive
        sqlx::query(DELETE_ARCHIVE_BY_ID)
            .bind(msg_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "DELETE_ARCHIVE_BY_ID_TX".into(),
                source: e,
                context: format!("Failed to delete archived message {}", msg_id),
            })?;

        // 3. Insert into messages
        let now = Utc::now();
        let now_str = format_sqlite_timestamp(&now);
        // QueueMessage fields to return
        // We need separate logic for map_row because we are getting back message row, not archive row.
        // So we just fetch the inserted row and parse it manually like SqliteMessageTable::map_row

        let insert_sql = r#"
            INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id)
            VALUES ($1, $2, 0, $3, $3, $4)
            RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
        "#;

        let msg_row = sqlx::query(insert_sql)
            .bind(archive.queue_id)
            .bind(archive.payload.to_string())
            .bind(now_str)
            .bind(archive.producer_worker_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_REPLAY_MESSAGE".into(),
                source: e,
                context: "Failed to insert replayed message".into(),
            })?;

        tx.commit()
            .await
            .map_err(|e| crate::error::Error::TransactionFailed {
                source: e,
                context: "Failed to commit replay transaction".into(),
            })?;

        // Parse result manually since we don't have access to SqliteMessageTable from here easily without duplicating code
        let id: i64 = msg_row.try_get("id")?;
        let queue_id: i64 = msg_row.try_get("queue_id")?;
        let payload_str: String = msg_row.try_get("payload")?;
        let payload: Value = serde_json::from_str(&payload_str)?;
        let enqueued_at = parse_sqlite_timestamp(&msg_row.try_get::<String, _>("enqueued_at")?)?;
        let vt = match msg_row.try_get::<Option<String>, _>("vt")? {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };
        let read_ct: i32 = msg_row.try_get("read_ct")?;
        let dequeued_at = match msg_row.try_get::<Option<String>, _>("dequeued_at")? {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };
        let producer_worker_id: Option<i64> = msg_row.try_get("producer_worker_id")?;
        let consumer_worker_id: Option<i64> = msg_row.try_get("consumer_worker_id")?;

        Ok(Some(QueueMessage {
            id,
            queue_id,
            payload,
            vt: vt.unwrap_or(enqueued_at), // Should be set by default
            enqueued_at,
            read_ct,
            dequeued_at,
            producer_worker_id,
            consumer_worker_id,
        }))
    }

    async fn count_for_queue(&self, queue_id: i64) -> Result<i64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_archive WHERE queue_id = $1")
                .bind(queue_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("COUNT_ARCHIVE_BY_QUEUE ({})", queue_id),
                    source: e,
                    context: format!("Failed to count archives for queue {}", queue_id),
                })?;
        Ok(count)
    }
}
