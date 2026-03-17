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
  - `given_existing_remote_state_when_consistent_bootstrap_then_returns_conflict`
  - `given_changed_remote_state_when_consistent_write_then_returns_conflict`

## Current Design Assumptions
- `SnapshotDb` uses a single local SQLite store for both reads and writes.
- `SnapshotDb::snapshot()` is metadata-first:
  - `head()` remote object
  - compare remote etag with local `last_etag`
  - return early when unchanged
  - download and reopen only when etag changed
- `SnapshotDb::snapshot()` performs a final compare-and-swap on `last_etag` before replacing the local store.
- `ConsistentDb` provides durable sequencing:
  - write
  - sync
  - refresh
- `ConsistentDb` should serialize the full durable write sequence for a shared handle.

## Test Surface Checklist

- [x] `localstack_s3_health_endpoint_is_reachable`
  - Purpose: Validate LocalStack/S3 test endpoint is usable.
- [x] `tables_tests::consistentdb_normal_flow_without_explicit_sync_or_refresh`
  - Purpose: Validate automatic durability mode sync behavior.
- [x] `tables_tests::consistentdb_sequences_reads_and_writes_under_contention`
  - Purpose: Validate concurrency safety under simultaneous read/write operations.
- [x] `localstack_s3_basic_ops_and_cas_etag`
  - Purpose: Validate raw S3 CAS, ETag semantics, and conditional reads/writes.
- [x] `localstack_aws_adapter_round_trip`
  - Purpose: Validate adapter-level get/put-if-match behavior with mocked object store wrapper.
- [x] `consistent_db_tests::consistent_bootstrap_open_and_queue_lifecycle`
  - Purpose: Validate durable-mode lifecycle without manual snapshot/sync/refresh calls in the caller.
- [x] `snapshot_bootstrap::s3_bootstrap_creates_remote_state_when_key_is_missing`
  - Purpose: Validate local bootstrap plus explicit `sync()` publishes remote state when missing.
- [x] `snapshot_bootstrap::s3_bootstrap_restores_existing_remote_state`
  - Purpose: Validate remote snapshot restoration path from an explicitly synced source store.
- [x] `snapshot_bootstrap::s3_bootstrap_is_idempotent`
  - Purpose: Validate repeated `sync()` calls are stable when database content is unchanged.
- [x] `snapshot_bootstrap::s3_bootstrap_recovers_when_remote_state_deleted`
  - Purpose: Validate caller-triggered sync can recreate remote state after deletion.
- [x] `snapshot_bootstrap::s3_bootstrap_rejects_invalid_s3_dsn`
  - Purpose: Validate malformed S3 DSN still fails predictably.

## Bootstrap Coverage
- [x] `snapshot_bootstrap` module:
  - [x] `s3_bootstrap_creates_remote_state_when_key_is_missing`
  - [x] `s3_bootstrap_restores_existing_remote_state`
  - [x] `s3_bootstrap_is_idempotent`
  - [x] `s3_bootstrap_recovers_when_remote_state_deleted`
  - [x] `s3_bootstrap_rejects_invalid_s3_dsn`
- [x] `consistent_db_tests` module:
  - [x] `consistent_bootstrap_open_and_queue_lifecycle`
  - [x] `given_missing_remote_state_when_consistent_bootstrap_then_creates_remote_state`
  - [x] `given_missing_remote_state_when_consistent_bootstrap_then_state_is_immediately_readable_without_sync_or_refresh`
  - [x] `given_existing_remote_state_when_consistent_bootstrap_then_returns_conflict`
  - [x] `given_unchanged_remote_state_when_consistent_bootstrap_repeated_then_state_is_unchanged`

## ConsistentDb Bootstrap Scenarios
- Contract:
  - Caller explicitly invokes `bootstrap()`.
  - Caller should not need `sync()` / `refresh()` / `snapshot()` after bootstrap.
  - `ConsistentDb` should return `Conflict` when the write path observes a real conflict instead of hiding it.

- Create:
  - [x] `given_missing_remote_state_when_consistent_bootstrap_then_creates_remote_state`
  - [x] `given_missing_remote_state_when_consistent_bootstrap_then_state_is_immediately_readable_without_sync_or_refresh`

- Idempotency:
  - [x] `given_unchanged_remote_state_when_consistent_bootstrap_repeated_then_state_is_unchanged`

- Conflict:
  - [x] `given_existing_remote_state_when_consistent_bootstrap_then_returns_conflict`
  - [ ] `given_changed_remote_state_when_consistent_write_then_returns_conflict`

- Failure handling:
  - [ ] `given_deleted_remote_state_when_consistent_bootstrap_then_expected_error_or_recreate_behavior_is_explicit`
  - [ ] `given_invalid_remote_sqlite_object_when_consistent_bootstrap_then_returns_error`

## SnapshotDb Snapshot Scenarios
- Contract:
  - Caller explicitly invokes `snapshot()`.
  - `snapshot()` should be idempotent when local and remote etags match.
  - `snapshot()` should avoid reopening the local SQLite store unless remote state changed.
  - `snapshot()` should not overwrite newer local knowledge if another operation wins before the final swap.
  - `snapshot()` should return `Conflict` when local state is dirty.

- Idempotency:
  - [x] `given_matching_local_and_remote_etag_when_snapshot_then_returns_without_rewrite`

- Refresh on change:
  - [ ] `given_changed_remote_etag_when_snapshot_then_downloads_and_reopens_local_store`
  - [x] `given_changed_remote_state_when_snapshot_then_latest_remote_data_is_visible_locally`

- CAS / race handling:
  - [ ] `given_local_etag_changes_before_snapshot_swap_when_snapshot_then_does_not_replace_newer_local_state`
  - [ ] `given_concurrent_snapshot_calls_when_one_wins_first_then_other_returns_without_corruption`

- Error handling:
  - [x] `given_missing_remote_object_when_snapshot_then_returns_not_found`
  - [ ] `given_invalid_remote_sqlite_payload_when_snapshot_then_returns_error`
  - [ ] `given_head_succeeds_and_get_fails_when_snapshot_then_returns_error_without_partial_swap`

## SnapshotDb Sync Scenarios
- Contract:
  - Caller explicitly invokes `sync()`.
  - `sync()` should be a no-op when there are no dirty local writes.
  - `sync()` should avoid holding the write lock across file and network I/O.
  - `sync()` should clear dirty state only after a successful CAS-protected publish.
  - `sync()` should return `Conflict` if local state changes during the sync window.
  - `sync()` should not recreate missing remote state unless there is dirty local state to publish.

- No-op:
  - [x] `given_clean_local_state_when_sync_then_returns_without_remote_change`
  - [x] `given_deleted_remote_object_and_clean_local_state_when_sync_then_returns_without_recreating_remote_state`

- Publish:
  - [x] `given_dirty_local_state_when_sync_then_publishes_remote_state`
  - [x] `given_deleted_remote_object_and_dirty_local_state_when_sync_then_recreates_remote_state`
  - [x] `given_successful_sync_when_snapshot_follows_then_snapshot_returns_without_change`
  - [ ] `given_successful_sync_when_remote_head_is_checked_then_etag_advances_and_dirty_state_is_cleared`

- Conflict:
  - [x] `given_dirty_local_state_when_snapshot_then_returns_conflict`
  - [ ] `given_local_state_changes_during_sync_when_sync_finishes_then_returns_conflict`
  - [ ] `given_remote_etag_changes_during_sync_when_sync_finishes_then_returns_conflict`

- Recovery / reopen:
  - [x] `given_successful_sync_when_local_revision_is_reopened_then_latest_state_remains_queryable`
  - [ ] `given_failed_sync_when_local_state_is_reopened_then_pre_sync_state_remains_queryable`

## Coverage Gaps to Track
- Error-path behavior when LocalStack is unavailable.
- Failure/retry behavior during `sync`/`snapshot` under transient network errors.
- Behavior on non-200/412 S3 consistency/permission failures.
- Idempotency and cleanup on repeated bootstrap/sync/snapshot calls.
- Snapshot idempotence assertions should verify both behavior and absence of unnecessary local reopen.
- Artifact cleanup assertions to avoid environment leakage across test runs.
- Timeout behavior and flakiness controls for long-running/dependent async paths.

## Progress Log
- 2026-03-14: Plan initialized.

## Notes
- Mark each checkbox as review proceeds.
- Add a short finding note beside each test as reviewed:
  - `[ ] TestName — status: pass/fail/blocked`
  - `[ ] TestName — gap found: ...`
