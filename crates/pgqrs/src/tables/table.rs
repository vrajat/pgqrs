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
/// # use pgqrs::tables::{Table, pgqrs_workers::{Workers, NewWorker}};
/// # async fn example(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// // Create a new worker
/// let new_worker = NewWorker {
///     hostname: "worker-1".to_string(),
///     port: 8080,
///     queue_id: Some(1),  // None for Admin workers
/// };
///
/// // Insert returns full WorkerInfo with generated ID and timestamps
/// let workers = Workers::new(pool);
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

    /// List all records.
    ///
    /// # Returns
    /// Vector of all entities in the table
    async fn list(&self) -> Result<Vec<Self::Entity>>;

    /// Filter records by foreign key.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Value of the foreign key to filter by
    ///
    /// # Returns
    /// Vector of entities matching the foreign key criteria
    async fn filter_by_fk(&self, foreign_key_value: i64) -> Result<Vec<Self::Entity>>;

    /// Count all records in the table.
    ///
    /// # Returns
    /// Total number of records in the table
    async fn count(&self) -> Result<i64>;

    /// Count records by foreign key.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Value of the foreign key to filter by
    ///
    /// # Returns
    /// Number of records matching the foreign key criteria
    async fn count_for_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<i64>;

    /// Delete a record by ID.
    ///
    /// # Arguments
    /// * `id` - Primary key of the entity to delete
    ///
    /// # Returns
    /// Number of rows affected (0 or 1)
    async fn delete(&self, id: i64) -> Result<u64>;

    /// Delete records by foreign key within a transaction.
    ///
    /// # Arguments
    /// * `foreign_key_value` - Value of the foreign key to filter by
    /// * `tx` - Mutable reference to an active SQL transaction
    ///
    /// # Returns
    /// Number of rows affected
    async fn delete_by_fk<'a, 'b: 'a>(
        &self,
        foreign_key_value: i64,
        tx: &'a mut sqlx::Transaction<'b, sqlx::Postgres>,
    ) -> Result<u64>;
}
