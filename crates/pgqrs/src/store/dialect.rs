#[derive(Debug, Clone, Copy)]
pub(crate) struct StepSql {
    pub acquire: &'static str,
    pub clear_retry: &'static str,
    pub complete: &'static str,
    pub fail: &'static str,
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
    const MESSAGE: MessageSql;
    const WORKER: WorkerSql;
    const DB_STATE: DbStateSql;
}
