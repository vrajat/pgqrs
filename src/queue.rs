//! Queue operations and producer interface for pgqrs.
//!
//! This module defines the [`Queue`] struct, which provides methods for enqueuing, dequeuing, and managing jobs in a PostgreSQL-backed queue.
//!
//! ## What
//!
//! - [`Queue`] is the main interface for interacting with a queue: adding jobs, fetching jobs, updating visibility, and batch operations.
//!
//! ## How
//!
//! Create a [`Queue`] using the admin API, then use its methods to enqueue and process jobs.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::queue::Queue;
//! // let queue = ...
//! // queue.enqueue(...)
//! ```
use crate::constants::PENDING_COUNT;
use crate::constants::PGQRS_SCHEMA;
use crate::constants::QUEUE_PREFIX;
use crate::error::Result;
use crate::types::QueueMessage;
use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::r2d2::ConnectionManager;
use diesel::sql_query;
use diesel::RunQueryDsl;
use r2d2::Pool;

/// Producer interface for adding messages to queues
pub struct Queue {
    /// Connection pool for PostgreSQL
    pub pool: Pool<ConnectionManager<PgConnection>>,
    /// Logical name of the queue
    pub queue_name: String,
    /// Table name in the database for this queue
    pub table_name: String,
    /// SQL for inserting a message
    pub insert_sql: String,
    /// SQL for selecting a message by ID
    pub select_by_id_sql: String,
    /// SQL for reading messages
    pub read_messages_sql: String,
    /// SQL for dequeuing a message
    pub dequeue_sql: String,
    /// SQL for updating message visibility timeout
    pub update_vt_sql: String,
    /// SQL for deleting a batch of messages
    pub delete_batch_sql: String,
}

impl Queue {
    /// Create a new Producer instance
    pub(crate) fn new(pool: Pool<ConnectionManager<PgConnection>>, queue_name: &str) -> Self {
        let table_name = format!("{}.{}_{}", PGQRS_SCHEMA, QUEUE_PREFIX, queue_name);
        let insert_sql = crate::constants::INSERT_MESSAGE
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let select_by_id_sql = crate::constants::SELECT_MESSAGE_BY_ID
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let read_messages_sql = crate::constants::READ_MESSAGES
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let dequeue_sql = crate::constants::DEQUEUE_MESSAGE
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let update_vt_sql = crate::constants::UPDATE_MESSAGE_VT
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        let delete_batch_sql = crate::constants::DELETE_MESSAGE_BATCH
            .replace("{PGQRS_SCHEMA}", crate::constants::PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", crate::constants::QUEUE_PREFIX)
            .replace("{queue_name}", queue_name);
        Self {
            pool,
            queue_name: queue_name.to_string(),
            table_name,
            insert_sql,
            select_by_id_sql,
            read_messages_sql,
            dequeue_sql,
            update_vt_sql,
            delete_batch_sql,
        }
    }

    /// Run a closure with a database connection in a blocking task.
    ///
    /// This is a low-level helper for executing synchronous Diesel operations in an async context.
    /// Most users should use higher-level queue methods instead.
    ///
    /// # Arguments
    /// * `f` - Closure that receives a mutable database connection and returns a result.
    ///
    /// # Returns
    /// The result of the closure, or a connection error.
    async fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            f(&mut conn)
        })
        .await
        .map_err(|e| crate::error::PgqrsError::Connection {
            message: e.to_string(),
        })?
    }

    /// Retrieve a message by its ID from the queue.
    ///
    /// # Arguments
    /// * `msg_id` - ID of the message to retrieve
    ///
    /// # Returns
    /// The message if found, or an error if not found.
    pub async fn get_message_by_id(&self, msg_id: i64) -> Result<QueueMessage> {
        let sql = self.select_by_id_sql.clone();
        self.with_conn(move |conn| {
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::BigInt, _>(msg_id)
                .get_result::<QueueMessage>(conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })
        })
        .await
    }

    /// Add a single message to the queue.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    ///
    /// # Returns
    /// The enqueued message.
    pub async fn enqueue(&self, payload: &serde_json::Value) -> Result<QueueMessage> {
        self.enqueue_delayed(payload, 0).await
    }

    /// Schedule a message to be available for consumption after a delay.
    ///
    /// # Arguments
    /// * `payload` - JSON payload for the message
    /// * `delay_seconds` - Seconds to delay before message becomes available
    ///
    /// # Returns
    /// The enqueued message.
    pub async fn enqueue_delayed(
        &self,
        payload: &serde_json::Value,
        delay_seconds: u32,
    ) -> Result<QueueMessage> {
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(delay_seconds as i64);
        let id = self.insert_message(payload, now, vt).await?;
        self.get_message_by_id(id).await
    }

    // Helper to build the insert SQL for a queue (no longer needed)

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
        payload: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
        vt: chrono::DateTime<chrono::Utc>,
    ) -> Result<i64> {
        let pool = self.pool.clone();
        let sql = self.insert_sql.clone();
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
    /// Add multiple messages to the queue in a single batch operation.
    ///
    /// # Arguments
    /// * `payloads` - Slice of JSON payloads to enqueue
    ///
    /// # Returns
    /// Vector of enqueued messages.
    pub async fn batch_enqueue(&self, payloads: &[serde_json::Value]) -> Result<Vec<QueueMessage>> {
        use diesel::Connection;
        let pool = self.pool.clone();
        let now = Utc::now();
        let vt = now + chrono::Duration::seconds(0);
        let sql = self.insert_sql.clone();
        let payloads_cloned = payloads.to_vec();
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
        let ids_str = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT msg_id, read_ct, enqueued_at, vt, message FROM {} WHERE msg_id IN ({})",
            self.table_name, ids_str
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

    /// Get the count of pending (not locked) messages in the queue.
    ///
    /// # Returns
    /// Number of pending messages.
    pub async fn pending_count(&self) -> Result<i64> {
        use chrono::Utc;
        let now = Utc::now();
        let sql = PENDING_COUNT
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", &self.queue_name);

        #[derive(diesel::QueryableByName)]
        struct CountRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }
        self.with_conn(move |conn| {
            let row = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Timestamptz, _>(now)
                .get_result::<CountRow>(conn)?;
            Ok(row.count)
        })
        .await
    }

    /// Read up to `limit` messages from the queue, using the default visibility timeout.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of messages to read
    ///
    /// # Returns
    /// Vector of messages read from the queue.
    pub async fn read(&self, limit: usize) -> Result<Vec<QueueMessage>> {
        self.read_delay(crate::constants::VISIBILITY_TIMEOUT as u32, limit)
            .await
    }

    /// Read up to `limit` messages from the queue, with a custom visibility timeout.
    ///
    /// # Arguments
    /// * `vt` - Visibility timeout (seconds)
    /// * `limit` - Maximum number of messages to read
    ///
    /// # Returns
    /// Vector of messages read from the queue.
    pub async fn read_delay(&self, vt: u32, limit: usize) -> Result<Vec<QueueMessage>> {
        let sql = self
            .read_messages_sql
            .clone()
            .replace("{vt}", &vt.to_string())
            .replace("{limit}", &limit.to_string());
        let sql = sql;
        self.with_conn(move |conn| {
            let result = diesel::sql_query(&sql)
                .get_results::<QueueMessage>(conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            Ok(result)
        })
        .await
    }

    /// Remove a message from the queue (delete it permanently).
    ///
    /// # Arguments
    /// * `message_id` - ID of the message to delete
    ///
    /// # Returns
    /// The deleted message, or an error if not found.
    pub async fn dequeue(&self, message_id: i64) -> Result<QueueMessage> {
        let sql = self
            .dequeue_sql
            .clone()
            .replace("{PGQRS_SCHEMA}", PGQRS_SCHEMA)
            .replace("{QUEUE_PREFIX}", QUEUE_PREFIX)
            .replace("{queue_name}", &self.queue_name);

        self.with_conn(move |conn| {
            let result = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::BigInt, _>(message_id)
                .get_result::<QueueMessage>(conn)
                .map_err(|e| crate::error::PgqrsError::Connection {
                    message: e.to_string(),
                })?;
            Ok(result)
        })
        .await
    }

    /// Remove a batch of messages from the queue.
    ///
    /// # Arguments
    /// * `message_ids` - Vector of message IDs to delete
    ///
    /// # Returns
    /// Vector of booleans indicating success for each message (same order as input).
    pub async fn delete_batch(&self, message_ids: Vec<i64>) -> Result<Vec<bool>> {
        let sql = self.delete_batch_sql.clone();

        #[derive(diesel::QueryableByName)]
        struct DeletedRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            msg_id: i64,
        }

        let ids = message_ids.clone();
        let deleted_ids = self
            .with_conn(move |conn| {
                let rows = diesel::sql_query(&sql)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&ids)
                    .get_results::<DeletedRow>(conn)
                    .map_err(|e| crate::error::PgqrsError::Connection {
                        message: e.to_string(),
                    })?;
                Ok(rows.into_iter().map(|r| r.msg_id).collect::<Vec<i64>>())
            })
            .await?;

        // For each input id, true if it was deleted, false otherwise
        let deleted_set: std::collections::HashSet<i64> = deleted_ids.into_iter().collect();
        let result = message_ids
            .into_iter()
            .map(|id| deleted_set.contains(&id))
            .collect();
        Ok(result)
    }

    /// Extend the lock time for a message, preventing it from becoming visible again.
    ///
    /// # Arguments
    /// * `message_id` - ID of the message
    /// * `additional_seconds` - Additional seconds to extend the visibility
    ///
    /// # Returns
    /// True if the message's visibility was extended, false otherwise.
    pub async fn extend_visibility(
        &self,
        message_id: i64,
        additional_seconds: u32,
    ) -> Result<bool> {
        let sql = self.update_vt_sql.clone();
        let updated = self
            .with_conn(move |conn| {
                let result = diesel::sql_query(&sql)
                    .bind::<diesel::sql_types::Int4, _>(additional_seconds as i32)
                    .bind::<diesel::sql_types::BigInt, _>(message_id)
                    .execute(conn)
                    .map_err(|e| crate::error::PgqrsError::Connection {
                        message: e.to_string(),
                    })?;
                Ok(result)
            })
            .await?;
        Ok(updated > 0)
    }
}
