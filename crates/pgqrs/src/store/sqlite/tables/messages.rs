use crate::error::Result;
use crate::store::sqlite::{format_sqlite_timestamp, parse_sqlite_timestamp};
use crate::types::QueueMessage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool};

const INSERT_MESSAGE: &str = r#"
    INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id;
"#;

const GET_MESSAGE_BY_ID: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE id = $1;
"#;

const LIST_ALL_MESSAGES: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    ORDER BY enqueued_at DESC;
"#;

const DELETE_MESSAGE_BY_ID: &str = r#"
    DELETE FROM pgqrs_messages
    WHERE id = $1;
"#;

const LIST_MESSAGES_BY_QUEUE: &str = r#"
    SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id
    FROM pgqrs_messages
    WHERE queue_id = $1
    ORDER BY enqueued_at DESC
    LIMIT 1000;
"#;

const UPDATE_MESSAGE_VT: &str = r#"
    UPDATE pgqrs_messages
    SET vt = $2
    WHERE id = $1;
"#;

#[derive(Debug, Clone)]
pub struct SqliteMessageTable {
    pool: SqlitePool,
}

impl SqliteMessageTable {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn map_row(row: sqlx::sqlite::SqliteRow) -> Result<QueueMessage> {
        let id: i64 = row.try_get("id")?;
        let queue_id: i64 = row.try_get("queue_id")?;

        let payload_str: String = row.try_get("payload")?;
        let payload: Value = serde_json::from_str(&payload_str)?;

        let vt_str: String = row.try_get("vt")?;
        let vt = parse_sqlite_timestamp(&vt_str)?;

        let enqueued_at_str: String = row.try_get("enqueued_at")?;
        let enqueued_at = parse_sqlite_timestamp(&enqueued_at_str)?;

        let read_ct: i32 = row.try_get("read_ct")?;

        let dequeued_at_str: Option<String> = row.try_get("dequeued_at")?;
        let dequeued_at = match dequeued_at_str {
            Some(s) => Some(parse_sqlite_timestamp(&s)?),
            None => None,
        };

        let producer_worker_id: Option<i64> = row.try_get("producer_worker_id")?;
        let consumer_worker_id: Option<i64> = row.try_get("consumer_worker_id")?;

        Ok(QueueMessage {
            id,
            queue_id,
            payload,
            vt,
            enqueued_at,
            read_ct,
            dequeued_at,
            producer_worker_id,
            consumer_worker_id,
        })
    }
}

#[async_trait]
impl crate::store::MessageTable for SqliteMessageTable {
    async fn insert(&self, data: crate::types::NewMessage) -> Result<QueueMessage> {
        let payload_str = data.payload.to_string();
        let enqueued_at_str = format_sqlite_timestamp(&data.enqueued_at);
        let vt_str = format_sqlite_timestamp(&data.vt);

        let row = sqlx::query(INSERT_MESSAGE)
            .bind(data.queue_id)
            .bind(payload_str)
            .bind(data.read_ct)
            .bind(enqueued_at_str)
            .bind(vt_str)
            .bind(data.producer_worker_id)
            .bind(data.consumer_worker_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "INSERT_MESSAGE".into(),
                source: Box::new(e),
                context: "Failed to insert message".into(),
            })?;

        Self::map_row(row)
    }

    async fn get(&self, id: i64) -> Result<QueueMessage> {
        let row = sqlx::query(GET_MESSAGE_BY_ID)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("GET_MESSAGE_BY_ID ({})", id),
                source: Box::new(e),
                context: format!("Failed to get message {}", id),
            })?;

        Self::map_row(row)
    }

    async fn list(&self) -> Result<Vec<QueueMessage>> {
        let rows = sqlx::query(LIST_ALL_MESSAGES)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "LIST_ALL_MESSAGES".into(),
                source: Box::new(e),
                context: "Failed to list all messages".into(),
            })?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(row)?);
        }
        Ok(messages)
    }

    async fn count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pgqrs_messages")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "COUNT_MESSAGES".into(),
                source: Box::new(e),
                context: "Failed to count messages".into(),
            })?;
        Ok(count)
    }

    async fn delete(&self, id: i64) -> Result<u64> {
        let result = sqlx::query(DELETE_MESSAGE_BY_ID)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("DELETE_MESSAGE_BY_ID ({})", id),
                source: Box::new(e),
                context: format!("Failed to delete message {}", id),
            })?;
        Ok(result.rows_affected())
    }

    async fn filter_by_fk(&self, queue_id: i64) -> Result<Vec<QueueMessage>> {
        let rows = sqlx::query(LIST_MESSAGES_BY_QUEUE)
            .bind(queue_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("LIST_MESSAGES_BY_QUEUE ({})", queue_id),
                source: Box::new(e),
                context: format!("Failed to list messages for queue {}", queue_id),
            })?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(row)?);
        }
        Ok(messages)
    }

    async fn batch_insert(
        &self,
        queue_id: i64,
        payloads: &[serde_json::Value],
        params: crate::types::BatchInsertParams,
    ) -> Result<Vec<i64>> {
        if payloads.is_empty() {
            return Ok(vec![]);
        }

        // query_builder handles construction.

        let mut query_builder = sqlx::QueryBuilder::new("INSERT INTO pgqrs_messages (queue_id, payload, read_ct, enqueued_at, vt, producer_worker_id, consumer_worker_id) ");

        query_builder.push_values(payloads, |mut b, payload| {
            b.push_bind(queue_id)
                .push_bind(payload.to_string())
                .push_bind(params.read_ct)
                .push_bind(format_sqlite_timestamp(&params.enqueued_at))
                .push_bind(format_sqlite_timestamp(&params.vt))
                .push_bind(params.producer_worker_id)
                .push_bind(params.consumer_worker_id);
        });

        query_builder.push(" RETURNING id");

        let ids = query_builder
            .build_query_scalar()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "BATCH_INSERT_MESSAGES_DYNAMIC".into(),
                source: Box::new(e),
                context: format!("Failed to batch insert {} messages", payloads.len()),
            })?;

        Ok(ids)
    }

    async fn get_by_ids(&self, ids: &[i64]) -> Result<Vec<QueueMessage>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let mut query_builder = sqlx::QueryBuilder::new("SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE id IN (");

        let mut separated = query_builder.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") ORDER BY id");

        let rows = query_builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "GET_MESSAGES_BY_IDS_DYNAMIC".into(),
                source: Box::new(e),
                context: format!("Failed to get {} messages by IDs", ids.len()),
            })?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(Self::map_row(row)?);
        }
        Ok(messages)
    }

    async fn update_visibility_timeout(&self, id: i64, vt: DateTime<Utc>) -> Result<u64> {
        let vt_str = format_sqlite_timestamp(&vt);
        let result = sqlx::query(UPDATE_MESSAGE_VT)
            .bind(id)
            .bind(vt_str)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("UPDATE_MESSAGE_VT ({})", id),
                source: Box::new(e),
                context: format!("Failed to update visibility timeout for message {}", id),
            })?;
        Ok(result.rows_affected())
    }

    async fn extend_visibility(
        &self,
        id: i64,
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<u64> {
        let sql = r#"
            UPDATE pgqrs_messages
            SET vt = datetime(vt, '+' || ? || ' seconds')
            WHERE id = ? AND consumer_worker_id = ?;
        "#;

        let result = sqlx::query(sql)
            .bind(additional_seconds as i32)
            .bind(id)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: format!("EXTEND_MESSAGE_VT ({})", id),
                source: Box::new(e),
                context: format!("Failed to extend visibility for message {}", id),
            })?;

        Ok(result.rows_affected())
    }

    async fn extend_visibility_batch(
        &self,
        message_ids: &[i64],
        worker_id: i64,
        additional_seconds: u32,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut query_builder =
            sqlx::QueryBuilder::new("UPDATE pgqrs_messages SET vt = datetime(vt, '+' || ");
        query_builder.push_bind(additional_seconds as i32);
        query_builder.push(" || ' seconds') WHERE id IN (");

        let mut separated = query_builder.separated(", ");
        for id in message_ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") AND consumer_worker_id = ");
        query_builder.push_bind(worker_id);
        query_builder.push(" RETURNING id");

        let extended_ids: Vec<i64> = query_builder
            .build_query_scalar()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "EXTEND_BATCH_VT_DYNAMIC".into(),
                source: Box::new(e),
                context: format!(
                    "Failed to batch extend visibility for {} messages",
                    message_ids.len()
                ),
            })?;

        let extended_set: std::collections::HashSet<i64> = extended_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| extended_set.contains(id))
            .collect();

        Ok(result)
    }

    async fn release_messages_by_ids(
        &self,
        message_ids: &[i64],
        worker_id: i64,
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut query_builder = sqlx::QueryBuilder::new("UPDATE pgqrs_messages SET vt = datetime('now'), consumer_worker_id = NULL WHERE id IN (");

        let mut separated = query_builder.separated(", ");
        for id in message_ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") AND consumer_worker_id = ");
        query_builder.push_bind(worker_id);
        query_builder.push(" RETURNING id");

        let released_ids: Vec<i64> = query_builder
            .build_query_scalar()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| crate::error::Error::QueryFailed {
                query: "RELEASE_SPECIFIC_MESSAGES_DYNAMIC".into(),
                source: Box::new(e),
                context: format!("Failed to release {} messages", message_ids.len()),
            })?;

        let released_set: std::collections::HashSet<i64> = released_ids.into_iter().collect();
        let result = message_ids
            .iter()
            .map(|id| released_set.contains(id))
            .collect();

        Ok(result)
    }

    async fn count_pending(&self, queue_id: i64) -> Result<i64> {
        self.count_pending_filtered(queue_id, None).await
    }

    async fn count_pending_filtered(&self, queue_id: i64, worker_id: Option<i64>) -> Result<i64> {
        let count: i64 = match worker_id {
            Some(wid) => {
                sqlx::query_scalar(
                    r#"
                    SELECT COUNT(*)
                    FROM pgqrs_messages
                    WHERE queue_id = $1 AND consumer_worker_id = $2
                    "#,
                )
                .bind(queue_id)
                .bind(wid)
                .fetch_one(&self.pool)
                .await
            }
            None => {
                sqlx::query_scalar(
                    r#"
                    SELECT COUNT(*)
                    FROM pgqrs_messages
                    WHERE queue_id = $1 AND (vt IS NULL OR vt <= datetime('now')) AND consumer_worker_id IS NULL
                    "#,
                )
                .bind(queue_id)
                .fetch_one(&self.pool)
                .await
            }
        }.map_err(|e| crate::error::Error::QueryFailed {
            query: format!("COUNT_PENDING (queue_id={})", queue_id),
            source: Box::new(e),
            context: format!("Failed to count pending messages for queue {}", queue_id),
        })?;

        Ok(count)
    }

    async fn delete_by_ids(&self, ids: &[i64]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(ids.len());

        // For large deletes, transactions might be better but iterating is simpler and likely fast enough for typical batches.
        // Or we could use dynamic DELETE WHERE id IN (...)
        // But the trait implies per-ID result, but actually returns boolean if *that* delete succeeded.
        // If we do batch delete, we don't know which ones were deleted unless we use RETURNING id.

        if ids.is_empty() {
            return Ok(vec![]);
        }

        // The Postgres implementation loops and executes DELETE one by one. I'll do the same for consistency regarding return type.
        for &id in ids {
            let rows_affected = sqlx::query(DELETE_MESSAGE_BY_ID)
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(|e| crate::error::Error::QueryFailed {
                    query: format!("DELETE_MESSAGE_BY_ID ({})", id),
                    source: Box::new(e),
                    context: format!("Failed to delete message {}", id),
                })?
                .rows_affected();
            results.push(rows_affected > 0);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::sqlite::tables::queues::SqliteQueueTable;
    use crate::store::{MessageTable, QueueTable};
    use crate::types::{BatchInsertParams, NewMessage, NewQueue};

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("Failed to create pool");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to create queues table");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pgqrs_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                payload TEXT NOT NULL,
                read_ct INTEGER NOT NULL DEFAULT 0,
                enqueued_at TEXT NOT NULL,
                vt TEXT,
                dequeued_at TEXT,
                producer_worker_id INTEGER,
                consumer_worker_id INTEGER,
                FOREIGN KEY (queue_id) REFERENCES pgqrs_queues(id) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to create messages table");

        pool
    }

    #[tokio::test]
    async fn test_message_insert_and_get() {
        let pool = create_test_pool().await;
        let queue_table = SqliteQueueTable::new(pool.clone());
        let msg_table = SqliteMessageTable::new(pool);

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "test_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        let now = chrono::Utc::now();
        let payload = serde_json::json!({"test": "data"});

        let msg = msg_table
            .insert(NewMessage {
                queue_id: queue.id,
                payload: payload.clone(),
                read_ct: 0,
                enqueued_at: now,
                vt: now,
                producer_worker_id: None,
                consumer_worker_id: None,
            })
            .await
            .expect("Failed to insert message");

        assert_eq!(msg.payload, payload);
        assert_eq!(msg.queue_id, queue.id);

        let fetched = msg_table.get(msg.id).await.expect("Failed to get message");
        assert_eq!(fetched.id, msg.id);
    }

    #[tokio::test]
    async fn test_message_list_and_count() {
        let pool = create_test_pool().await;
        let queue_table = SqliteQueueTable::new(pool.clone());
        let msg_table = SqliteMessageTable::new(pool);

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "test_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        let now = chrono::Utc::now();

        msg_table
            .insert(NewMessage {
                queue_id: queue.id,
                payload: serde_json::json!({"msg": 1}),
                read_ct: 0,
                enqueued_at: now,
                vt: now,
                producer_worker_id: None,
                consumer_worker_id: None,
            })
            .await
            .expect("Failed to insert");

        let messages = msg_table.list().await.expect("Failed to list");
        assert!(!messages.is_empty());

        let count = msg_table.count().await.expect("Failed to count");
        assert!(count >= 1);
    }

    #[tokio::test]
    async fn test_message_batch_insert() {
        let pool = create_test_pool().await;
        let queue_table = SqliteQueueTable::new(pool.clone());
        let msg_table = SqliteMessageTable::new(pool);

        let queue = queue_table
            .insert(NewQueue {
                queue_name: "batch_queue".to_string(),
            })
            .await
            .expect("Failed to create queue");

        let payloads = vec![
            serde_json::json!({"msg": 1}),
            serde_json::json!({"msg": 2}),
            serde_json::json!({"msg": 3}),
        ];

        let now = chrono::Utc::now();
        let params = BatchInsertParams {
            read_ct: 0,
            enqueued_at: now,
            vt: now,
            producer_worker_id: None,
            consumer_worker_id: None,
        };

        let ids = msg_table
            .batch_insert(queue.id, &payloads, params)
            .await
            .expect("Failed to batch insert");

        assert_eq!(ids.len(), 3);
    }
}
