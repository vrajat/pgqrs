# S3 Store Test Plan (Running Review)

## Reviewed Component
- Rust test coverage for S3 store behavior in `crates/pgqrs/tests/s3_tests.rs` on branch `feat/s3-queue`.

## Review Context
- Reviewer: (TBD)
- Start date: 2026-03-14
- Scope: Functional + reliability of S3-backed queue operations and LocalStack test harness.

## Naming Convention
- Use `given_<precondition>_when_<action>_then_<outcome>`.
- This keeps the test name aligned with contract, not implementation detail.
- Example:
  - `given_missing_remote_state_when_snapshot_bootstrap_then_creates_remote_state`
  - `given_existing_remote_state_when_consistent_bootstrap_then_restores_locally`
  - `given_changed_remote_state_when_consistent_write_then_returns_conflict`

## Test Surface Checklist

- [ ] `localstack_s3_health_endpoint_is_reachable`
  - Purpose: Validate LocalStack/S3 test endpoint is usable.
- [ ] `tables_tests::tables_sqlite_queue_insert_and_get`
  - Purpose: Validate queue insert/write to S3 write-side and read-side visibility after sync.
- [ ] `tables_tests::tables_sqlite_message_insert_and_count`
  - Purpose: Validate message insert/count split behavior before/after explicit sync.
- [ ] `tables_tests::syncstore_normal_flow_with_explicit_refresh_calls`
  - Purpose: Validate explicit `refresh()` + `sync()` + `snapshot()` handoff and visibility.
- [ ] `tables_tests::consistentdb_normal_flow_without_explicit_sync_or_refresh`
  - Purpose: Validate automatic durability mode sync behavior.
- [ ] `tables_tests::consistentdb_sequences_reads_and_writes_under_contention`
  - Purpose: Validate concurrency safety under simultaneous read/write operations.
- [ ] `localstack_s3_basic_ops_and_cas_etag`
  - Purpose: Validate raw S3 CAS, ETag semantics, and conditional reads/writes.
- [ ] `localstack_aws_adapter_round_trip`
  - Purpose: Validate adapter-level get/put-if-match behavior with mocked object store wrapper.
- [ ] `consistent_bootstrap::consistent_bootstrap_open_and_queue_lifecycle`
  - Purpose: Validate durable-mode lifecycle without manual snapshot/sync/refresh calls in the caller.
- [ ] `consistent_bootstrap::consistent_bootstrap_compatibility_aliases`
  - Purpose: Validate alias compatibility between `snapshot()/sync()/refresh()` in durable mode.
- [ ] `snapshot_bootstrap::s3_bootstrap_creates_remote_state_when_key_is_missing`
  - Purpose: Validate local bootstrap plus explicit `sync()` publishes remote state when missing.
- [ ] `snapshot_bootstrap::s3_bootstrap_restores_existing_remote_state`
  - Purpose: Validate remote snapshot restoration path from an explicitly synced source store.
- [ ] `snapshot_bootstrap::s3_bootstrap_is_idempotent`
  - Purpose: Validate repeated `sync()` calls are stable when database content is unchanged.
- [ ] `snapshot_bootstrap::s3_bootstrap_recovers_when_remote_state_deleted`
  - Purpose: Validate caller-triggered sync can recreate remote state after deletion.
- [ ] `snapshot_bootstrap::s3_bootstrap_rejects_invalid_s3_dsn`
  - Purpose: Validate malformed S3 DSN still fails predictably.

## Bootstrap Coverage
- [ ] `snapshot_bootstrap` module:
  - [ ] `s3_bootstrap_creates_remote_state_when_key_is_missing`
  - [ ] `s3_bootstrap_restores_existing_remote_state`
  - [ ] `s3_bootstrap_is_idempotent`
  - [ ] `s3_bootstrap_recovers_when_remote_state_deleted`
  - [ ] `s3_bootstrap_rejects_invalid_s3_dsn`
- [ ] `consistent_bootstrap` module:
  - [ ] `consistent_bootstrap_open_and_queue_lifecycle`
  - [ ] `consistent_bootstrap_compatibility_aliases`

## ConsistentDb Bootstrap Scenarios
- Contract:
  - Caller explicitly invokes `bootstrap()`.
  - Caller should not need `sync()` / `refresh()` / `snapshot()` after bootstrap.
  - `ConsistentDb` should return `Conflict` when the write path observes a real conflict instead of hiding it.

- Create:
  - [ ] `given_missing_remote_state_when_consistent_bootstrap_then_creates_remote_state`
  - [ ] `given_missing_remote_state_when_consistent_bootstrap_then_state_is_immediately_readable_without_sync_or_refresh`

- Restore:
  - [ ] `given_existing_remote_state_when_consistent_bootstrap_then_restores_locally`
  - [ ] `given_existing_remote_messages_when_consistent_bootstrap_then_messages_are_visible_without_sync_or_refresh`

- Idempotency:
  - [ ] `given_unchanged_remote_state_when_consistent_bootstrap_repeated_then_state_is_unchanged`

- Conflict:
  - [ ] `given_existing_remote_state_when_stale_consistent_writer_syncs_then_returns_conflict`
  - [ ] `given_two_consistent_instances_when_one_updates_remote_before_other_write_then_second_write_returns_conflict`

- Failure handling:
  - [ ] `given_deleted_remote_state_when_consistent_bootstrap_then_expected_error_or_recreate_behavior_is_explicit`
  - [ ] `given_invalid_remote_sqlite_object_when_consistent_bootstrap_then_returns_error`

## Coverage Gaps to Track
- Error-path behavior when LocalStack is unavailable.
- Failure/retry behavior during `sync`/`snapshot` under transient network errors.
- Behavior on non-200/412 S3 consistency/permission failures.
- Idempotency and cleanup on repeated bootstrap/sync/snapshot calls.
- Artifact cleanup assertions to avoid environment leakage across test runs.
- Timeout behavior and flakiness controls for long-running/dependent async paths.

## Progress Log
- 2026-03-14: Plan initialized.

## Notes
- Mark each checkbox as review proceeds.
- Add a short finding note beside each test as reviewed:
  - `[ ] TestName — status: pass/fail/blocked`
  - `[ ] TestName — gap found: ...`
