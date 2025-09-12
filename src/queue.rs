use crate::constants::{INSERT_MESSAGE, SELECT_MESSAGE_BY_ID};
use crate::error::Result;
use crate::types::constants::{PGQRS_SCHEMA, QUEUE_PREFIX, VISIBILITY_TIMEOUT};
use crate::QueueMessage;
use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::r2d2::ConnectionManager;
use diesel::sql_query;
use diesel::RunQueryDsl;
use r2d2::Pool;

/// Producer interface for adding messages to queues
pub struct Queue<'a> {
    pub pool: &'a Pool<ConnectionManager<PgConnection>>,
}

impl<'a> Queue<'a> {
    /// Create a new Producer instance
    pub fn new(pool: &'a Pool<ConnectionManager<PgConnection>>) -> Self {
        Self { pool }
    }

    /// Retrieve a message by its ID from the queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    /// * `msg_id` - ID of the message to retrieve
    pub async fn get_message_by_id(&self, queue_name: &str, msg_id: i64) -> Result<QueueMessage> {
        let pool = self.pool.clone();
        let sql = SELECT_MESSAGE_BY_ID
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);

        let row = tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::BigInt, _>(msg_id)
                .get_result::<QueueMessage>(&mut conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;
        Ok(row)
    }

    /// Add a single message to the queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue to add the message to
    /// * `payload` - JSON payload for the message
    ///
    /// # Returns
    /// The UUID of the enqueued message
    pub async fn enqueue(
        &self,
        queue_name: &str,
        payload: &serde_json::Value,
    ) -> Result<QueueMessage> {
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(VISIBILITY_TIMEOUT as i64);
        let id = self.insert_message(queue_name, payload, now, vt).await?;
        let queue_message = self.get_message_by_id(queue_name, id).await?;
        Ok(queue_message)
    }

    /// Schedule a message to be available for consumption at a specific time
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    /// * `payload` - JSON payload for the message
    /// * `delay_seconds` - Seconds to delay before message becomes available
    pub async fn enqueue_delayed(
        &self,
        queue_name: &str,
        payload: &serde_json::Value,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(delay_seconds as i64);
        let id = self.insert_message(queue_name, payload, now, vt).await?;
        self.get_message_by_id(queue_name, id).await
    }

    // Helper to build the insert SQL for a queue
    fn insert_sql_for_queue(queue_name: &str) -> String {
        INSERT_MESSAGE
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name)
    }

    // Helper to insert a single message (sync, for use in both single and batch)
    fn insert_one_message_sync(
        conn: &mut PgConnection,
        sql: &str,
        payload_json: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> std::result::Result<i64, crate::error::PgqrsError> {
        #[derive(diesel::QueryableByName)]
        struct MsgId {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            msg_id: i64,
        }
        let result = sql_query(sql)
            .bind::<diesel::sql_types::Int4, _>(0)
            .bind::<diesel::sql_types::Timestamptz, _>(now)
            .bind::<diesel::sql_types::Timestamptz, _>(vt)
            .bind::<diesel::sql_types::Jsonb, _>(payload_json)
            .get_result::<MsgId>(conn)?;
        Ok(result.msg_id)
    }

    async fn insert_message(
        &self,
        queue_name: &str,
        payload: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<i64> {
        let pool = self.pool.clone();
        let sql = Self::insert_sql_for_queue(queue_name);
        let payload_cloned = payload.clone();
        let id = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            Self::insert_one_message_sync(&mut conn, &sql, &payload_cloned, now, vt)
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;
        Ok(id)
    }

    /// Add a batch of messages to the queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue to add messages to
    /// * `messages` - Vector of (payload, message_type) tuples
    ///
    /// # Returns
    /// Vector of UUIDs for the enqueued messages (in same order as input)
    pub async fn batch_enqueue(
        &self,
        queue_name: &str,
        payloads: &Vec<serde_json::Value>,
    ) -> Result<Vec<QueueMessage>> {
        use diesel::Connection;
        let pool = self.pool.clone();
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(VISIBILITY_TIMEOUT as i64);
        let sql = Self::insert_sql_for_queue(queue_name);
        let payloads_cloned = payloads.clone();
        let ids = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            conn.transaction::<_, crate::error::PgqrsError, _>(|txn| {
                let mut ids = Vec::with_capacity(payloads_cloned.len());
                for payload_json in &payloads_cloned {
                    let id = Self::insert_one_message_sync(txn, &sql, payload_json, now, vt)?;
                    ids.push(id);
                }
                Ok(ids)
            })
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;

        // Fetch all messages in a single query using WHERE msg_id IN (...)
        let table_name = format!("{}.{}_{}", PGQRS_SCHEMA, QUEUE_PREFIX, queue_name);
        let ids_str = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT msg_id, read_ct, enqueued_at, vt, message FROM {} WHERE msg_id IN ({})",
            table_name, ids_str
        );
        let pool = self.pool.clone();
        let queue_messages = tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            diesel::sql_query(&sql)
                .get_results::<QueueMessage>(&mut conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;
        Ok(queue_messages)
    }

    /// Get count of pending messages in a queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    pub async fn pending_count(&self, queue_name: &str) -> Result<i64> {
        use chrono::Utc;
        let pool = self.pool.clone();
        let now = Utc::now();
        let table_name = format!("{}.{}_{}", PGQRS_SCHEMA, QUEUE_PREFIX, queue_name);
        let sql = format!(
            "SELECT COUNT(*) as count FROM {} WHERE vt <= $1",
            table_name
        );
        #[derive(diesel::QueryableByName)]
        struct CountRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }
        let count = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            let row = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Timestamptz, _>(now)
                .get_result::<CountRow>(&mut conn)?;
            Ok::<i64, crate::error::PgqrsError>(row.count)
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;
        Ok(count)
    }

    /// Read a single message from the queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue to read from
    /// * `options` - Read options (lock time, message type filter, etc.)
    ///
    /// # Returns
    /// Option containing the message if available, None if queue is empty
    pub async fn read(
        &self,
        queue_name: &str,
        vt: u32,
        limit: usize,
    ) -> Result<Option<Vec<QueueMessage>>> {
        let pool = self.pool.clone();
        let sql = crate::types::constants::READ_MESSAGES
            .replace("{PGQRS_SCHEMA}", crate::types::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::types::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name)
            .replace("{vt}", &vt.to_string())
            .replace("{limit}", &limit.to_string());

        let messages = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
            let result = diesel::sql_query(&sql)
                .get_results::<QueueMessage>(&mut conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            Ok::<Vec<QueueMessage>, crate::error::PgqrsError>(result)
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;

        if messages.is_empty() {
            Ok(None)
        } else {
            Ok(Some(messages))
        }
    }

    /// Remove a message from the queue (delete it permanently)
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    ///
    /// # Returns
    /// True if message was deleted, false if not found
    pub async fn dequeue(&self, queue_name: &str) -> Result<QueueMessage> {
        let pool = self.pool.clone();
        let sql = crate::types::constants::DEQUEUE_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::types::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::types::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name)
            .replace("{PGMQ_SCHEMA}", crate::types::constants::PGQRS_SCHEMA)
            .replace("{name}", queue_name);

        let message = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
            let result = diesel::sql_query(&sql)
                .get_result::<QueueMessage>(&mut conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            Ok::<QueueMessage, crate::error::PgqrsError>(result)
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;

        Ok(message)
    }

    /// Remove a batch of messages from the queue
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    /// * `message_ids` - Vector of message IDs to delete
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input)
    pub async fn delete_batch(
        &self,
        queue_name: &str,
        message_ids: Vec<i64>,
    ) -> Result<Vec<bool>> {
        let pool = self.pool.clone();
        let sql = crate::types::constants::DELETE_MESSAGE_BATCH
            .replace("{PGQRS_SCHEMA}", crate::types::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::types::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);

        #[derive(diesel::QueryableByName)]
        struct DeletedRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            msg_id: i64,
        }

        let ids = message_ids.clone();
        let deleted_ids = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
            let rows = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&ids)
                .get_results::<DeletedRow>(&mut conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            Ok::<Vec<i64>, crate::error::PgqrsError>(rows.into_iter().map(|r| r.msg_id).collect())
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;

        // For each input id, true if it was deleted, false otherwise
        let deleted_set: std::collections::HashSet<i64> = deleted_ids.into_iter().collect();
        let result = message_ids.into_iter().map(|id| deleted_set.contains(&id)).collect();
        Ok(result)
    }

    /// Extend the lock time for a message (to prevent it from becoming visible again)
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    /// * `message_id` - ID of the message
    /// * `additional_seconds` - Additional seconds to extend the visibility
    pub async fn extend_visibility(
        &self,
        queue_name: &str,
        message_id: i64,
        additional_seconds: u32,
    ) -> Result<bool> {
        let pool = self.pool.clone();
        let sql = crate::types::constants::UPDATE_MESSAGE_VT
            .replace("{PGQRS_SCHEMA}", crate::types::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::types::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);

        let updated = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| crate::error::PgqrsError::Connection {
                message: e.to_string(),
            })?;
            let result = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Int4, _>(additional_seconds as i32)
                .bind::<diesel::sql_types::BigInt, _>(message_id)
                .execute(&mut conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            Ok::<usize, crate::error::PgqrsError>(result)
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })??;

        Ok(updated > 0)
    }
}
