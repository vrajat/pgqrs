use crate::traits::{Queue, QueueMessage, QueueRepo, MessageRepo, QueueStats};
use crate::error::PgqrsError;
use async_trait::async_trait;
use sqlx::{PgPool, Row};
use sqlx::types::JsonValue;
use chrono::{DateTime, Utc};

pub struct PgQueueRepo {
    pub pool: PgPool,
}

pub struct PgMessageRepo {
    pub pool: PgPool,
}

#[async_trait]
impl QueueRepo for PgQueueRepo {
    async fn create_queue(&self, name: &str, unlogged: bool) -> Result<Queue, PgqrsError> {
        let mut tx = self.pool.begin().await?;
        // 1. Create the queue table
        let unlogged_str = if unlogged { "UNLOGGED" } else { "" };
        let create_table_sql = crate::constants::CREATE_QUEUE_STATEMENT
            .replace("{UNLOGGED}", unlogged_str)
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", name);
        sqlx::query(&create_table_sql).execute(&mut *tx).await?;

        // 2. Create the index
        let create_index_sql = crate::constants::CREATE_INDEX_STATEMENT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", name);
        sqlx::query(&create_index_sql).execute(&mut *tx).await?;

        // 3. Insert into meta table
        let insert_meta_sql = crate::constants::INSERT_QUEUE_METADATA
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA);
        sqlx::query(&insert_meta_sql)
            .bind(name)
            .bind(unlogged)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.get_queue(name).await
    }

    async fn delete_queue(&self, name: &str) -> Result<(), PgqrsError> {
        let mut tx = self.pool.begin().await?;
        // 1. Drop the queue table
        let drop_table_sql = crate::constants::DROP_QUEUE_STATEMENT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", name);
        sqlx::query(&drop_table_sql)
            .execute(&mut *tx)
            .await?;
        // 2. Remove from meta table
        let delete_meta_sql = crate::constants::DELETE_QUEUE_METADATA
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA);
        sqlx::query(&delete_meta_sql)
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_queues(&self) -> Result<Vec<Queue>, PgqrsError> {
        let sql = crate::constants::LIST_QUEUES_META
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA);
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await?;
        let queues = rows.into_iter().map(|row| Queue {
            queue_name: row.get("queue_name"),
            created_at: row.get::<DateTime<Utc>, _>("created_at"),
            unlogged: row.get("unlogged"),
        }).collect();
        Ok(queues)
    }

    async fn purge_queue(&self, name: &str) -> Result<(), PgqrsError> {
        let sql = crate::constants::PURGE_QUEUE_STATEMENT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", name);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_queue(&self, name: &str) -> Result<Queue, PgqrsError> {
        let sql = crate::constants::SELECT_QUEUE_META
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA);
        let row = sqlx::query(&sql)
            .bind(name)
            .fetch_one(&self.pool)
            .await?;
        Ok(Queue {
            queue_name: row.get("queue_name"),
            created_at: row.get::<DateTime<Utc>, _>("created_at"),
            unlogged: row.get("unlogged"),
        })
    }
}

#[async_trait]
impl MessageRepo for PgMessageRepo {
    async fn enqueue(&self, queue: &str, payload: &JsonValue) -> Result<QueueMessage, PgqrsError> {
        let now = Utc::now();
        let vt = now;
        let sql = crate::constants::INSERT_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let row = sqlx::query(&sql)
            .bind(0i32) // read_ct
            .bind(now)
            .bind(vt)
            .bind(payload)
            .fetch_one(&self.pool)
            .await?;
        Ok(QueueMessage {
            id: row.get("msg_id"),
            payload: row.get("message"),
            enqueued_at: row.get("enqueued_at"),
            vt: row.get("vt"),
            read_ct: row.get("read_ct"),
        })
    }

    async fn enqueue_delayed(&self, queue: &str, payload: &JsonValue, delay_seconds: u32) -> Result<QueueMessage, PgqrsError> {
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(delay_seconds as i64);
        let sql = crate::constants::INSERT_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let row = sqlx::query(&sql)
            .bind(0i32)
            .bind(now)
            .bind(vt)
            .bind(payload)
            .fetch_one(&self.pool)
            .await?;
        Ok(QueueMessage {
            id: row.get("msg_id"),
            payload: row.get("message"),
            enqueued_at: row.get("enqueued_at"),
            vt: row.get("vt"),
            read_ct: row.get("read_ct"),
        })
    }

    async fn batch_enqueue(&self, queue: &str, payloads: &[JsonValue]) -> Result<Vec<QueueMessage>, PgqrsError> {
        let now = Utc::now();
        let vt = now;
        let sql = crate::constants::INSERT_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let mut messages = Vec::with_capacity(payloads.len());
        let mut tx = self.pool.begin().await?;
        for payload in payloads {
            let row = sqlx::query(&sql)
                .bind(0i32)
                .bind(now)
                .bind(vt)
                .bind(payload)
                .fetch_one(&mut *tx)
                .await?;
            messages.push(QueueMessage {
                id: row.get("msg_id"),
                payload: row.get("message"),
                enqueued_at: row.get("enqueued_at"),
                vt: row.get("vt"),
                read_ct: row.get("read_ct"),
            });
        }
        tx.commit().await?;
        Ok(messages)
    }

    async fn dequeue(&self, queue: &str, message_id: i64) -> Result<QueueMessage, PgqrsError> {
        let sql = crate::constants::DELETE_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let row = sqlx::query(&sql)
            .bind(message_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(QueueMessage {
            id: row.get("msg_id"),
            payload: row.get("message"),
            enqueued_at: row.get("enqueued_at"),
            vt: row.get("vt"),
            read_ct: row.get("read_ct"),
        })
    }

    async fn ack(&self, _queue: &str, _message_id: i64) -> Result<(), PgqrsError> {
        // For this implementation, ack is a no-op (dequeue deletes the message)
        Ok(())
    }

    async fn nack(&self, _queue: &str, _message_id: i64) -> Result<(), PgqrsError> {
        // For this implementation, nack is a no-op (could move to dead-letter queue)
        Ok(())
    }

    async fn peek(&self, queue: &str, limit: usize) -> Result<Vec<QueueMessage>, PgqrsError> {
        let now = Utc::now();
        let sql = crate::constants::READ_MESSAGES
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let rows = sqlx::query(&sql)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        let messages = rows.into_iter().map(|row| QueueMessage {
            id: row.get("msg_id"),
            payload: row.get("message"),
            enqueued_at: row.get("enqueued_at"),
            vt: row.get("vt"),
            read_ct: row.get("read_ct"),
        }).collect();
        Ok(messages)
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats, PgqrsError> {
        // Only pending count for now
        let now = Utc::now();
        let sql = crate::constants::PENDING_COUNT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let row = sqlx::query(&sql)
            .bind(now)
            .fetch_one(&self.pool)
            .await?;
        Ok(QueueStats {
            pending: row.get("count"),
            in_flight: 0,
            dead_lettered: 0,
        })
    }

    async fn get_message_by_id(&self, queue: &str, message_id: i64) -> Result<QueueMessage, PgqrsError> {
        let sql = crate::constants::SELECT_MESSAGE_BY_ID
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let row = sqlx::query(&sql)
            .bind(message_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(QueueMessage {
            id: row.get("msg_id"),
            payload: row.get("message"),
            enqueued_at: row.get("enqueued_at"),
            vt: row.get("vt"),
            read_ct: row.get("read_ct"),
        })
    }

    async fn heartbeat(&self, queue: &str, message_id: i64, additional_seconds: u32) -> Result<(), PgqrsError> {
        let sql = crate::constants::UPDATE_MESSAGE_VT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue);
        let vt = chrono::Utc::now() + chrono::Duration::seconds(additional_seconds as i64);
        let _ = sqlx::query(&sql)
            .bind(vt)
            .bind(message_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(())
    }
}
