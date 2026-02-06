use async_trait::async_trait;
use std::sync::Mutex;

use super::resource::TestResource;

/// External PostgreSQL database implementation
///
/// This resource connects to an external Postgres instance (either from CI services
/// or local Docker containers managed by the Makefile). It does not manage the lifecycle
/// of the database - that's handled externally.
pub struct ExternalPostgresResource {
    dsn: String,
    schema: Mutex<Option<String>>,
}

impl ExternalPostgresResource {
    pub fn new(dsn: String) -> Self {
        println!("Using external PostgreSQL database: {}", dsn);
        Self {
            dsn,
            schema: Mutex::new(None),
        }
    }
}

#[async_trait]
impl TestResource for ExternalPostgresResource {
    async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        // No-op for external DB - database is already running
        Ok(())
    }

    async fn get_dsn(&self, schema: Option<&str>) -> String {
        if let Some(s) = schema {
            super::database_setup::setup_database_common(
                self.dsn.clone(),
                s,
                "External PostgreSQL",
            )
            .await
            .expect("Failed to setup external database schema");

            if s != "public" {
                let mut guard = self.schema.lock().unwrap();
                *guard = Some(s.to_string());
            }
        }
        self.dsn.clone()
    }

    async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        let schema_opt = {
            let mut guard = self.schema.lock().unwrap();
            guard.take()
        };

        if let Some(schema) = schema_opt {
            let _ = super::database_setup::cleanup_database_common(
                self.dsn.clone(),
                &schema,
                "External PostgreSQL",
            )
            .await;
        }

        println!("External PostgreSQL database cleanup complete (database still running)");
        Ok(())
    }
}
