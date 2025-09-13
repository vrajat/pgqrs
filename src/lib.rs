use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");
/// Run embedded Diesel migrations
pub fn run_migrations(conn: &mut diesel::PgConnection) -> Result<()> {
    eprintln!("Current dir: {:?}", std::env::current_dir());
    // Print applied migrations
    match conn.applied_migrations() {
        Ok(applied) => {
            eprintln!("Applied migrations:");
            for m in applied {
                eprintln!("  {}", m.to_string());
            }
        }
        Err(e) => eprintln!("Error fetching applied migrations: {}", e),
    }

    // Print pending migrations
    match conn.pending_migrations(MIGRATIONS) {
        Ok(pending) => {
            eprintln!("Pending migrations:");
            for m in &pending {
                eprintln!("  {}", m.name());
            }
        }
        Err(e) => eprintln!("Error fetching pending migrations: {}", e),
    }

    // Run migrations
    match conn.run_pending_migrations(MIGRATIONS) {
        Ok(_) => Ok(()),
        Err(e) => Err(PgqrsError::Migration {
            message: e.to_string(),
        }),
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
pub mod config;
pub mod error;
pub mod queue;
mod schema;
pub mod types;

pub use admin::Admin;
pub use config::Config;
pub use diesel::pg::PgConnection;
pub use diesel::r2d2::{ConnectionManager, Pool};
pub use error::{PgqrsError, Result};
pub use queue::Queue;
pub use types::*;

/// Main client for pgqrs operations

pub fn create_pool(config: &Config) -> Result<Pool<ConnectionManager<PgConnection>>> {
    eprintln!("pgqrs config: {:?}", config);

    Pool::builder()
        .max_size(16)
        .build(ConnectionManager::new(config.database_url()))
        .map_err(|e| PgqrsError::from(e))
}
