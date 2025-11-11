//! Table trait for consistent CRUD operations across pgqrs tables.
//!
//! This module provides a unified interface for database table operations,
//! allowing each table to implement consistent patterns for create, read, update,
//! and delete operations while maintaining type safety through associated types.

#![allow(async_fn_in_trait)]

use crate::error::Result;

/// Trait defining consistent CRUD operations for pgqrs tables with type safety.
///
/// This trait uses associated types to ensure type safety while providing
/// a consistent interface across all table implementations.
///
/// # Type Parameters
/// - `Entity`: The full entity type returned from database reads (e.g., `WorkerInfo`)
/// - `NewEntity`: The input type for creating new entities (e.g., `NewWorker`)
///
/// # Example
/// ```rust,no_run
/// # use sqlx::PgPool;
/// # use pgqrs::tables::{Table, pgqrs_workers::{PgqrsWorkers, NewWorker}};
/// # async fn example(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// // Create a new worker
/// let new_worker = NewWorker {
///     hostname: "worker-1".to_string(),
///     port: 8080,
///     queue_name: "jobs".to_string(),
/// };
///
/// // Insert returns full WorkerInfo with generated ID and timestamps
/// let workers = PgqrsWorkers::new(pool);
/// let worker = workers.insert(new_worker).await?;
/// # Ok(())
/// # }
/// ```
pub trait Table {
    /// The full entity type returned from database operations
    type Entity;

    /// The input type for creating new entities
    type NewEntity;

    /// Insert a new record and return the complete entity.
    ///
    /// # Arguments
    /// * `data` - The new entity data to insert
    ///
    /// # Returns
    /// The complete entity with generated fields (ID, timestamps, etc.)
    async fn insert(&self, data: Self::NewEntity) -> Result<Self::Entity>;

    /// Get a single record by ID.
    ///
    /// # Arguments
    /// * `id` - Primary key of the entity to retrieve
    ///
    /// # Returns
    /// The entity with the specified ID
    async fn get(&self, id: i64) -> Result<Self::Entity>;

    /// List records, optionally filtered.
    ///
    /// # Arguments
    /// * `filter_id` - Optional filter parameter (implementation-specific)
    ///
    /// # Returns
    /// Vector of entities matching the criteria
    async fn list(&self, filter_id: Option<i64>) -> Result<Vec<Self::Entity>>;

    /// Delete a record by ID.
    ///
    /// # Arguments
    /// * `id` - Primary key of the entity to delete
    ///
    /// # Returns
    /// Number of rows affected (0 or 1)
    async fn delete(&self, id: i64) -> Result<u64>;
}