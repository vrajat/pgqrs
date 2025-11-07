//! Worker management for pgqrs
//!
//! This module provides worker lifecycle management, registration, heartbeats,
//! and message assignment tracking for distributed queue processing.
//!
//! ## What
//!
//! - [`Worker`] represents a worker instance processing messages from a queue
//! - Worker registration and identity management
//! - Heartbeat system for health monitoring
//! - Graceful shutdown with message release
//!
//! ## How
//!
//! Workers register themselves with a hostname and port, maintain heartbeats,
//! and can be tracked throughout their lifecycle. Messages can be assigned
//! to specific workers for processing accountability.
//!
//! ### Example
//!
//! ```no_run
//! use pgqrs::types::Worker;
//! use pgqrs::admin::PgqrsAdmin;
//! use pgqrs::config::Config;
//!
//! async fn worker_example() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::from_dsn("postgresql://user:pass@localhost/db");
//!     let admin = PgqrsAdmin::new(&config).await?;
//!     let queue = admin.create_queue(&"jobs".to_string(), false).await?;
//!     
//!     // Register a worker
//!     let worker = Worker::register(&queue, "worker-host".to_string(), 8080).await?;
//!     
//!     // Send heartbeat
//!     worker.heartbeat(&queue).await?;
//!     
//!     // Graceful shutdown
//!     worker.begin_shutdown(&queue).await?;
//!     worker.mark_stopped(&queue).await?;
//!     
//!     Ok(())
//! }
//! ```

use crate::error::{PgqrsError, Result};
use crate::queue::Queue;
use crate::types::{Worker, WorkerStatus};
use chrono::Utc;
use std::time::Duration;

impl Worker {
    /// Create and register a new worker for a queue
    ///
    /// # Arguments
    /// * `queue` - The queue this worker will process
    /// * `hostname` - Hostname where the worker is running
    /// * `port` - Port number for the worker
    ///
    /// # Returns
    /// A new `Worker` instance registered in the database
    ///
    /// # Errors
    /// Returns `PgqrsError` if database operations fail or if hostname+port
    /// combination is already registered by another active worker
    pub async fn register(queue: &Queue, hostname: String, port: i32) -> Result<Worker> {
        let now = Utc::now();

        // Insert the worker into the database and get the generated ID
        let sql = r#"
            INSERT INTO pgqrs.pgqrs_workers (hostname, port, queue_id, started_at, heartbeat_at, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        "#;

        let worker_id: i64 = sqlx::query_scalar(sql)
            .bind(&hostname)
            .bind(port)
            .bind(&queue.queue_name)
            .bind(now)
            .bind(now)
            .bind("ready")
            .fetch_one(&queue.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        let worker = Worker {
            id: worker_id,
            hostname: hostname.clone(),
            port,
            queue_id: queue.queue_name.clone(),
            started_at: now,
            heartbeat_at: now,
            shutdown_at: None,
            status: WorkerStatus::Ready,
        };

        Ok(worker)
    }

    /// Update this worker's heartbeat timestamp
    ///
    /// Should be called periodically to indicate the worker is still alive
    ///
    /// # Arguments
    /// * `queue` - The queue connection for database access
    ///
    /// # Errors
    /// Returns `PgqrsError` if the database update fails
    pub async fn heartbeat(&self, queue: &Queue) -> Result<()> {
        let now = Utc::now();

        let sql = r#"
            UPDATE pgqrs.pgqrs_workers 
            SET heartbeat_at = $1 
            WHERE id = $2
        "#;

        sqlx::query(sql)
            .bind(now)
            .bind(self.id)
            .execute(&queue.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Mark this worker as shutting down gracefully
    ///
    /// This signals that the worker is beginning shutdown process
    /// and should not receive new message assignments
    ///
    /// # Arguments
    /// * `queue` - The queue connection for database access
    ///
    /// # Errors
    /// Returns `PgqrsError` if the database update fails
    pub async fn begin_shutdown(&self, queue: &Queue) -> Result<()> {
        let now = Utc::now();

        let sql = r#"
            UPDATE pgqrs.pgqrs_workers 
            SET status = 'shutting_down', shutdown_at = $1 
            WHERE id = $2
        "#;

        sqlx::query(sql)
            .bind(now)
            .bind(self.id)
            .execute(&queue.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Mark this worker as stopped (final state)
    ///
    /// This is the final step in worker lifecycle
    ///
    /// # Arguments
    /// * `queue` - The queue connection for database access
    ///
    /// # Errors
    /// Returns `PgqrsError` if the database update fails
    pub async fn mark_stopped(&self, queue: &Queue) -> Result<()> {
        let sql = r#"
            UPDATE pgqrs.pgqrs_workers 
            SET status = 'stopped' 
            WHERE id = $1
        "#;

        sqlx::query(sql)
            .bind(self.id)
            .execute(&queue.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Check if this worker is healthy based on heartbeat age
    ///
    /// # Arguments
    /// * `max_age` - Maximum allowed age for the last heartbeat
    ///
    /// # Returns
    /// `true` if the worker's last heartbeat is within the max_age threshold
    pub fn is_healthy(&self, max_age: Duration) -> bool {
        let now = Utc::now();
        let heartbeat_age = now.signed_duration_since(self.heartbeat_at);

        heartbeat_age.to_std().unwrap_or(Duration::MAX) <= max_age
    }
}

impl Queue {
    /// Get all workers currently processing this queue
    ///
    /// # Returns
    /// A vector of all workers registered for this queue
    ///
    /// # Errors
    /// Returns `PgqrsError` if database query fails
    pub async fn list_workers(&self) -> Result<Vec<Worker>> {
        let sql = r#"
            SELECT id, hostname, port, queue_id, started_at, heartbeat_at, shutdown_at, status
            FROM pgqrs.pgqrs_workers 
            WHERE queue_id = $1
            ORDER BY started_at DESC
        "#;

        let worker_rows = sqlx::query_as::<_, crate::types::WorkerRow>(sql)
            .bind(&self.queue_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        let workers = worker_rows.into_iter().map(|row| row.into()).collect();
        Ok(workers)
    }

    /// Read messages, optionally assigning them to a specific worker
    ///
    /// # Arguments
    /// * `limit` - Maximum number of messages to read
    /// * `worker_id` - Optional worker ID to assign messages to
    ///
    /// # Returns
    /// Vector of messages read from the queue
    ///
    /// # Errors
    /// Returns `PgqrsError` if database operations fail
    pub async fn read_with_worker(
        &self,
        limit: usize,
        worker_id: Option<i64>,
    ) -> Result<Vec<crate::types::QueueMessage>> {
        let vt = chrono::Utc::now()
            + chrono::Duration::seconds(crate::constants::VISIBILITY_TIMEOUT as i64);

        let sql = if worker_id.is_some() {
            format!(
                r#"
                UPDATE pgqrs.q_{} 
                SET vt = $1, read_ct = read_ct + 1, worker_id = $3
                WHERE msg_id IN (
                    SELECT msg_id FROM pgqrs.q_{} 
                    WHERE vt < NOW() 
                    ORDER BY msg_id 
                    FOR UPDATE SKIP LOCKED 
                    LIMIT $2
                )
                RETURNING msg_id, read_ct, enqueued_at, vt, message, worker_id
                "#,
                self.queue_name, self.queue_name
            )
        } else {
            format!(
                r#"
                UPDATE pgqrs.q_{} 
                SET vt = $1, read_ct = read_ct + 1
                WHERE msg_id IN (
                    SELECT msg_id FROM pgqrs.q_{} 
                    WHERE vt < NOW() 
                    ORDER BY msg_id 
                    FOR UPDATE SKIP LOCKED 
                    LIMIT $2
                )
                RETURNING msg_id, read_ct, enqueued_at, vt, message, worker_id
                "#,
                self.queue_name, self.queue_name
            )
        };

        let messages = if let Some(wid) = worker_id {
            sqlx::query_as(&sql)
                .bind(vt)
                .bind(limit as i32)
                .bind(wid)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as(&sql)
                .bind(vt)
                .bind(limit as i32)
                .fetch_all(&self.pool)
                .await
        };

        messages.map_err(|e| PgqrsError::Connection {
            message: e.to_string(),
        })
    }

    /// Release messages from a specific worker (for shutdown)
    ///
    /// # Arguments
    /// * `worker_id` - The worker whose messages should be released
    ///
    /// # Returns
    /// Number of messages released
    ///
    /// # Errors
    /// Returns `PgqrsError` if database operations fail
    pub async fn release_worker_messages(&self, worker_id: i64) -> Result<u64> {
        let sql = format!(
            r#"
            UPDATE pgqrs.q_{} 
            SET vt = NOW(), worker_id = NULL 
            WHERE worker_id = $1
            "#,
            self.queue_name
        );

        let result = sqlx::query(&sql)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(result.rows_affected())
    }

    /// Get messages currently assigned to a worker
    ///
    /// # Arguments
    /// * `worker_id` - The worker whose messages to retrieve
    ///
    /// # Returns
    /// Vector of messages assigned to the specified worker
    ///
    /// # Errors
    /// Returns `PgqrsError` if database query fails
    pub async fn get_worker_messages(
        &self,
        worker_id: i64,
    ) -> Result<Vec<crate::types::QueueMessage>> {
        let sql = format!(
            r#"
            SELECT msg_id, read_ct, enqueued_at, vt, message, worker_id FROM pgqrs.q_{}
            WHERE worker_id = $1
            ORDER BY msg_id
            "#,
            self.queue_name
        );

        let messages = sqlx::query_as(&sql)
            .bind(worker_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| PgqrsError::Connection {
                message: e.to_string(),
            })?;

        Ok(messages)
    }
}
