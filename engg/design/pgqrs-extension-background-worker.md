# Mini-Design: pgqrs-extension Background Worker

## Overview
This document specifies the architecture, logic, GUC configurations, and error-handling strategies for the Postgres-resident background worker loop in `pgqrs-extension`. The background worker automates cron schedule scanning, lease reclamation, and timeout management directly inside PostgreSQL, eliminating the need for an external `pgqrs-admin` daemon in self-hosted deployments.

---

## 1. Lifecycle and Initialization

The background worker registers itself during PostgreSQL startup (`_PG_init`) when configured in `shared_preload_libraries`.

```rust
#[pg_guard]
pub extern "C" fn _PG_init() {
    // 1. Define GUC configuration parameters
    // 2. Register static background worker if enabled
    BackgroundWorkerBuilder::new("pgqrs coordinator")
        .set_function("pgqrs_coordinator_main")
        .set_library("pgqrs_extension")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .load();
}
```

- **Start Time**: `RecoveryFinished` ensures we only execute tasks on a read-write database that has completed recovery.
- **SPI Access**: Enabled to perform table queries and state changes natively.

---

## 2. GUC Configuration Parameters

We define custom PostgreSQL parameters (Grand Unified Configuration) to customize worker behavior:

| GUC Name | Type | Default | Description |
| --- | --- | --- | --- |
| `pgqrs.coordinator_enabled` | bool | `true` | Enables or disables the resident coordinator worker loop. |
| `pgqrs.database` | string | `"postgres"` | The target database containing pgqrs schema tables. |
| `pgqrs.user` | string | `"postgres"` | The Postgres user under whose privileges the worker executes. |
| `pgqrs.interval_ms` | int | `1000` | Polling interval for schedule scanning and maintenance sweep. |
| `pgqrs.heartbeat_timeout_secs` | int | `30` | Stale worker threshold for heartbeat timeouts. |
| `pgqrs.workflow_timeout_secs` | int | `3600` | Timeout threshold for active workflow execution. |

---

## 3. Concurrency & Advisory Locking

To ensure safety on multi-primary or multi-node PostgreSQL clusters:
1. The background worker acquires a session-level Postgres advisory lock (`pg_try_advisory_lock`) on a fixed 64-bit key space reserved for pgqrs coordinator operations (`0x5047515253000001` / `5784604930263089153`).
2. If the lock cannot be acquired immediately, the coordinator knows another instance or process is currently active and sleeps until the next interval before retrying.
3. This guarantees **exactly-once coordinator execution** without race conditions.

---

## 4. Main Loop & Coordinator Tasks

The coordinator entry point initializes connection to the target database and starts the polling loop:

```rust
#[pg_guard]
pub extern "C" fn pgqrs_coordinator_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    
    // Connect to target database
    let db = get_guc_string("pgqrs.database").unwrap_or("postgres");
    let user = get_guc_string("pgqrs.user").unwrap_or("postgres");
    BackgroundWorker::connect_worker_to_spi(Some(&db), Some(&user));
    
    while BackgroundWorker::worker_continue() {
        // Execute sweep and scan inside discrete transaction blocks
        if acquire_advisory_lock() {
            let _ = scan_schedules();
            let _ = run_maintenance();
            release_advisory_lock();
        }
        
        BackgroundWorker::wait_latch(Some(Duration::from_millis(get_guc_int("pgqrs.interval_ms"))));
    }
}
```

### Coordinator Routines
1. **Schedule Scanner**:
   - Queries `pgqrs_schedules` to find active crons due for execution.
   - Evaluates cron expressions using the Rust `cron` and `chrono` libraries.
   - Enqueues trigger messages to `pgqrs_messages` and calculates/updates `next_fire_at`.
2. **Worker Sweeper**:
   - Flags ready/polling workers as `stopped` if their heartbeats have elapsed.
3. **Lease Reclaimer**:
   - Resets message visibility timeouts (`vt = NOW()`) for messages leased to stopped/stale workers.
4. **Workflow Timeout Monitor**:
   - Moves timed out workflows to `ERROR` and aborts pending steps.
