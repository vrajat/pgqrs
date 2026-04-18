#[derive(Debug, Clone, Copy)]
pub(crate) struct StepSql {
    pub get: &'static str,
    pub list: &'static str,
    pub count: &'static str,
    pub delete: &'static str,
    pub acquire: &'static str,
    pub clear_retry: &'static str,
    pub complete: &'static str,
    pub fail: &'static str,
}

#[derive(Debug, Clone, Copy)]
#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) struct QueueSql {
    pub insert: &'static str,
    pub get: &'static str,
    pub get_by_name: &'static str,
    pub list: &'static str,
    pub delete: &'static str,
    pub delete_by_name: &'static str,
    pub exists: &'static str,
}

#[derive(Debug, Clone, Copy)]
#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) struct RunSql {
    pub insert: &'static str,
    pub get: &'static str,
    pub list: &'static str,
    pub count: &'static str,
    pub delete: &'static str,
    pub start: &'static str,
    pub get_status: &'static str,
    pub complete: &'static str,
    pub pause: &'static str,
    pub cancel: &'static str,
    pub fail: &'static str,
    pub get_by_message_id: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MessageSql {
    pub list_by_consumer_worker: &'static str,
    pub count_by_consumer_worker: &'static str,
    pub count_worker_references: &'static str,
    pub move_to_dlq: &'static str,
    pub release_by_consumer_worker: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkerSql {
    pub mark_stopped: &'static str,
    pub suspend: &'static str,
    pub poll: &'static str,
    pub interrupt: &'static str,
    pub shutdown: &'static str,
    pub complete_poll: &'static str,
    pub heartbeat: &'static str,
    pub resume: &'static str,
}

#[derive(Debug, Clone, Copy)]
#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) struct WorkflowSql {
    pub get_by_name: &'static str,
    pub insert: &'static str,
    pub get: &'static str,
    pub list: &'static str,
    pub count: &'static str,
    pub delete: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DbStateSql {
    pub check_table_exists: &'static str,
    pub check_orphaned_messages: &'static str,
    pub check_orphaned_message_workers: &'static str,
    pub purge_queue_messages: &'static str,
    pub purge_queue_workers: &'static str,
    pub queue_metrics: &'static str,
    pub all_queues_metrics: &'static str,
    pub system_stats: &'static str,
    pub worker_health_global: &'static str,
    pub worker_health_by_queue: &'static str,
    pub purge_old_workers: &'static str,
}

pub(crate) trait SqlDialect {
    const STEP: StepSql;
    #[cfg(any(feature = "sqlite", feature = "turso"))]
    const QUEUE: QueueSql;
    #[cfg(any(feature = "sqlite", feature = "turso"))]
    const RUN: RunSql;
    const MESSAGE: MessageSql;
    const WORKER: WorkerSql;
    #[cfg(any(feature = "sqlite", feature = "turso"))]
    const WORKFLOW: WorkflowSql;
    const DB_STATE: DbStateSql;
}
