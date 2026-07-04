//! Test-only workflow lifecycle helpers.

use crate::error::Result;
use crate::store::Store;
use crate::types::QueueMessage;
use crate::workers::{Consumer, Run};
use std::future::Future;

/// Role-oriented test harness for workflow execution attempts.
///
/// This is intentionally test-only. It exposes the consumer and external-actor
/// phases directly so integration tests can model workflow lifecycle checkpoints
/// without relying entirely on timing-based orchestration.
#[derive(Clone)]
pub struct WorkflowTestRig {
    store: Store,
    consumer: Consumer,
}

/// Backward-compatible alias for the more role-oriented harness name.
pub type WorkflowAttemptHarness = WorkflowTestRig;

/// A single workflow delivery attempt consisting of the dequeued trigger
/// message and the materialized run handle tied to that message.
#[derive(Clone)]
pub struct WorkflowAttempt {
    pub message: QueueMessage,
    pub run: Run,
}

impl WorkflowTestRig {
    /// Create a test rig from a store and consumer representing the actor roles.
    pub fn new(store: Store, consumer: Consumer) -> Self {
        Self { store, consumer }
    }

    /// Access the consumer actor used by this rig.
    pub fn consumer(&self) -> &Consumer {
        &self.consumer
    }

    /// Access the store backing this rig.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Dequeue the next workflow trigger message as the consumer actor.
    pub async fn as_consumer_dequeue(&self) -> Result<Option<QueueMessage>> {
        let mut messages = self.consumer.dequeue().await?;
        Ok(messages.pop())
    }

    /// Materialize a workflow attempt from a dequeued trigger message.
    pub async fn as_consumer_open_attempt(&self, msg: QueueMessage) -> Result<WorkflowAttempt> {
        let run = self.store.run(msg.clone()).await?;
        Ok(WorkflowAttempt { message: msg, run })
    }

    /// Resolve the run associated with a workflow message as an external actor.
    pub async fn as_external_actor_get_run(&self, message: &QueueMessage) -> Result<Run> {
        self.store.run(message.clone()).await
    }
}

impl WorkflowAttempt {
    /// Refresh the persisted run record backing this attempt.
    pub async fn refresh(&mut self) -> Result<Run> {
        self.run = self.run.refresh().await?;
        Ok(self.run.clone())
    }

    /// Start the materialized run.
    pub async fn start(&mut self) -> Result<Run> {
        self.run = self.run.start().await?;
        Ok(self.run.clone())
    }

    /// Invoke workflow logic against the current run handle.
    pub async fn invoke<T, F, Fut>(&mut self, handler: F) -> Result<T>
    where
        F: FnOnce(Run) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let result = handler(self.run.clone()).await;
        self.run = self.run.refresh().await?;
        result
    }

    /// Archive the attempt message as the consumer actor.
    pub async fn archive(&self, consumer: &Consumer) -> Result<()> {
        let _ = consumer.archive(self.message.id).await?;
        Ok(())
    }

    /// Release the attempt message back to the queue as the consumer actor.
    pub async fn release(&self, consumer: &Consumer) -> Result<()> {
        let _ = consumer.release_messages(&[self.message.id]).await?;
        Ok(())
    }
}

pub const TEST_SCHEMAS: &[&str] = &[
    "pgqrs_admin_scan_interval_test",
    "pgqrs_admin_scan_cron_test",
    "pgqrs_admin_reclaim_test",
    "pgqrs_admin_timeout_test",
    "pgqrs_builder_test",
    "pgqrs_builder_ergonomics_test",
    "pgqrs_concurrent_test",
    "pgqrs_error_test",
    "pgqrs_lib_test",
    "pgqrs_zombie_tests",
    "pgqrs_lib_stat_test",
    "pgqrs_pgbouncer_test",
    "pgqrs_cli_test",
    "pgqrs_worker_test",
    "pgqrs_workflow_test",
    "pgqrs_workflow_creation_test",
    "pgqrs_workflow_retry_test",
    "macro_test_creation",
    "macro_test_success",
    "macro_test_idempotency",
    "macro_test_step_failure",
    "macro_test_workflow_failure",
    "macro_test_run_metadata",
    "workflow_tests",
    "workflow_get_tests",
    "workflow_retrieval_tests",
    "workflow_polling_tests",
    "workflow_error_polling_tests",
    "workflow_fk_tests",
    "workflow_retry_integration_tests",
    "guide_tests",
    "test_sql_job_success",
    "test_sql_job_select",
    "test_sql_job_safety",
    "test_sql_workflow",
    "test_sql_wf_fail",
];

pub async fn run_postgres_schema_setup(dsn: &str, cleanup_mode: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(dsn)
        .await?;

    if cleanup_mode {
        println!("Cleaning up test schemas using DSN: {}", dsn);

        for schema in TEST_SCHEMAS {
            println!("Dropping schema: {}", schema);
            let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
            sqlx::query(&drop_sql).execute(&pool).await?;
        }

        println!("All test schemas cleaned up successfully!");
    } else {
        println!("Setting up test databases using DSN: {}", dsn);
        println!("Connected to database.");

        for schema in TEST_SCHEMAS {
            println!("Provisioning schema: {}", schema);

            // 1. Drop and Recreate Schema (Clean Slate for Suite)
            let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema);
            sqlx::query(&drop_sql).execute(&pool).await?;

            let create_sql = format!("CREATE SCHEMA \"{}\"", schema);
            sqlx::query(&create_sql).execute(&pool).await?;

            // 2. Install Migration
            // We rely on search_path to install tables into the new schema
            let config = crate::config::Config::from_dsn_with_schema(dsn, *schema)?;
            let store = crate::connect_with_config(&config).await?;

            crate::admin(&store).install().await?;
            println!("  -> Installed pgqrs tables.");
        }

        println!("All test schemas provisioned successfully!");
    }

    Ok(())
}
