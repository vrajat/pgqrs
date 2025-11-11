//! Table operations for pgqrs unified architecture.
//!
//! This module contains CRUD operations for each artifact/table in the pgqrs system.
//! Each table module provides focused operations on a specific table without heavy business logic.

pub mod pgqrs_archive;
pub mod pgqrs_messages;
pub mod pgqrs_queues;
pub mod pgqrs_workers;
pub mod table;

pub use pgqrs_archive::PgqrsArchive;
pub use pgqrs_messages::{PgqrsMessages, NewMessage};
pub use pgqrs_queues::{PgqrsQueues, NewQueue};
pub use pgqrs_workers::{PgqrsWorkers, NewWorker};
pub use table::Table;