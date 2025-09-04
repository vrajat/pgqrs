use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");
/// Run embedded Diesel migrations
pub fn run_migrations(conn: &mut diesel::PgConnection) -> Result<()> {
    match conn.run_pending_migrations(MIGRATIONS) {
        Ok(_) => Ok(()),
        Err(e) => Err(PgqrsError::Migration{message: e.to_string()}),
    }
}
/**
 # pgqrs

 A high-performance PostgreSQL-backed job queue for Rust applications.

 pgqrs provides APIs for managing queues of messages and producing/consuming
 messages with guaranteed exactly-once delivery within a visibility timeout.

 ## Features

 - High performance using PostgreSQL's SKIP LOCKED
 - Low latency with LISTEN/NOTIFY (typically under 3ms)
 - Type-safe job payloads with Rust's type system
 - Exactly-once delivery guarantees
 - Message archiving for retention and replayability
 - CLI tools for debugging and administration
*/

pub mod admin;
pub mod producer;
pub mod consumer;
pub mod config;
pub mod types;
pub mod error;

pub use admin::Admin;
pub use producer::Producer;
pub use consumer::Consumer;
pub use config::Config;
pub use types::*;
pub use error::{PgqrsError, Result};
pub use diesel::pg::PgConnection;
pub use diesel::r2d2::{ConnectionManager, Pool};

/// Main client for pgqrs operations
pub struct PgqrsClient {
    admin: Admin,
    producer: Producer,
    consumer: Consumer,
}

impl PgqrsClient {
    /// Create a new pgqrs client with the given configuration
    pub async fn new(config: Config) -> Result<Self> {
        todo!("Implement PgqrsClient::new")
    }

    /// Get admin interface
    pub fn admin(&self) -> &Admin {
        &self.admin
    }

    /// Get producer interface
    pub fn producer(&self) -> &Producer {
        &self.producer
    }

    /// Get consumer interface
    pub fn consumer(&self) -> &Consumer {
        &self.consumer
    }
}