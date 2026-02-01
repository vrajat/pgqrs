# SQLite Store Review (Craftsmanship Feedback)

This review covers the SQLite backend implementation in [sqlite_support_worktree/crates/pgqrs/src/store/sqlite](sqlite_support_worktree/crates/pgqrs/src/store/sqlite) plus optional feature flags and CI updates. Overall, the code is functional and thoughtfully structured. The suggestions below aim to raise the craftsmanship bar around correctness, performance, and ergonomics.

## Summary
- Strong modular design mirroring the Postgres backend: clear `tables/`, `worker/`, and `workflow/` separation.
- Sensible migration set under [migrations/sqlite](sqlite_support_worktree/crates/pgqrs/migrations/sqlite) with indices for common paths.
- CI matrix builds for multiple backends with feature gating; Makefile exposes backend-specific test targets.
- Areas to refine: transactional boundaries, timestamp handling consistency, `PRAGMA` configuration, minor API ergonomics, and a few performance/indexing wins.

## High-Impact Improvements
- **Enable foreign key enforcement:** Add `PRAGMA foreign_keys = ON` in the `after_connect` hook alongside WAL/busy timeout in [sqlite/mod.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/mod.rs#L1). This prevents silent orphan creation and ensures FK constraints in migrations are actually enforced.
- **Harden `query_bool()` decoding:** In [sqlite/mod.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/mod.rs#L1), `query_bool` currently relies on sqlx mapping a SQLite INTEGER/EXPR to bool. Prefer `query_scalar::<_, i64>(...)` and convert via `Ok(i != 0)` to avoid brittle decodes when expressions return numeric.
- **Unify `Drop` behavior for ephemeral workers:** `SqliteConsumer` uses `tokio::runtime::Handle::try_current()` while `SqliteProducer` uses `tokio::task::spawn` directly. Align `SqliteProducer`’s `Drop` to the safer pattern used in [worker/consumer.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/consumer.rs#L1) so it will not panic if dropped outside a runtime.
- **Run migrations in `Admin::install()`:** The `Admin.install()` in [worker/admin.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/admin.rs#L1) returns `Ok(())`. Either document that migrations are run in `SqliteStore::new()` or invoke `sqlx::migrate!("migrations/sqlite").run(...)` here for symmetry with Postgres and clearer operational semantics.
- **Reduce duplicate row mapping:** Multiple places (e.g., [worker/admin.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/admin.rs#L1) and [tables/archive.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/tables/archive.rs#L1)) manually reconstruct `QueueMessage`/`ArchivedMessage`. Consider implementing `sqlx::FromRow` for these types or exposing a shared mapping helper to avoid duplication and reduce mapping drift.

## Correctness & Reliability
- **Unique constraint error handling:** In [tables/queues.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/tables/queues.rs#L1), you match on `db_err.code()` values (`2067`, `1555`, `19`). For robustness, prefer checking `db_err.message()` contains `UNIQUE` or rely on sqlx’s constraint name if available. SQLite’s codes can vary across versions/builds.
- **Transactional consistency for batch ops:** Some batch operations iterate one-by-one (e.g., `delete_by_ids()` in [tables/messages.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/tables/messages.rs#L1)). Consider transactional `DELETE ... WHERE id IN (...) RETURNING id` to ensure atomicity and precise per-id results while reducing round-trips.
- **Timestamp consistency:** You standardize on `TEXT` timestamps (`%Y-%m-%d %H:%M:%S`) with helpers `parse_sqlite_timestamp` and `format_sqlite_timestamp`. Ensure every write path uses these helpers (it does in most places). Also confirm all comparisons use `datetime()` to avoid locale/collation surprises.
- **Visibility timeout semantics:** Make sure every path that sets or clears `vt` (e.g., [tables/messages.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/tables/messages.rs#L1), [worker/consumer.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/consumer.rs#L1)) is consistent with pending/locked logic used by `count_pending_filtered()`. Currently consistent, but worth a doc note in code.
- **Admin worker registration expectations:** `Admin::register()` in [worker/admin.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/admin.rs#L1) returns early if already registered. Consider adding a debug log for idempotent reuse to help ops.

## Performance & Indexing
- **Index for dequeue ordering:** Since `DEQUEUE` queries order by `enqueued_at ASC` in [worker/consumer.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/consumer.rs#L1), add `CREATE INDEX IF NOT EXISTS idx_messages_queue_enqueued_at ON pgqrs_messages(queue_id, enqueued_at)` to [03_create_messages.sql](sqlite_support_worktree/crates/pgqrs/migrations/sqlite/03_create_messages.sql#L1) for faster selection under load.
- **Batch operations via `QueryBuilder`:** Great use of `QueryBuilder` in `batch_insert()` and batch visibility/release updates. Where you still loop per-id, consider the `IN (...) RETURNING id` pattern to minimize round-trips and return exact affected IDs.
- **Pool sizing from config:** `SqlitePoolOptions::new().max_connections(4)` in [sqlite/mod.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/mod.rs#L1) is fine; allow overriding via `Config` to tune per deployment.

## API & Ergonomics
- **Consistent placeholder style:** SQLite commonly uses `?` placeholders. You currently use `$n` in many queries (supported by sqlx). Consider standardizing to `?` for readability across the SQLite backend.
- **Expose mapping via `FromRow`:** Implement `impl sqlx::FromRow<sqlx::sqlite::SqliteRow> for QueueMessage` and friends to remove the need for bespoke `map_row` calls and simplify usage across modules.
- **Doc comments and tracing:** Consider adding `///` doc comments on main structs (e.g., `SqliteStore`, `SqliteMessageTable`) and sprinkling `tracing` logs for notable state transitions (worker register/suspend/resume/shutdown, DLQ moves, workflow transitions). You already log producer registration—nice.
- **Minor cleanup:** In [worker/producer.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/worker/producer.rs#L1), the line `Arc::new(workers);` is a no-op leftover; remove it.

## Workflows
- **Step guard drop path:** Excellent protective logic in [workflow/guard.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/workflow/guard.rs#L1) to fail a step when dropped. Add tests to verify this path marks the step as `ERROR` and preserves `started_at` correctly.
- **Status transitions:** In [workflow/handle.rs](sqlite_support_worktree/crates/pgqrs/src/store/sqlite/workflow/handle.rs#L1), `start()` checks for terminal `ERROR` state. Consider explicit handling for already `RUNNING` to be idempotent, returning `Ok(())` with a trace.

## Features & Cargo
- **Feature gating looks solid:** In [store/mod.rs](sqlite_support_worktree/crates/pgqrs/src/store/mod.rs#L1), modules are gated behind `#[cfg(feature = "sqlite")]`. In [crates/pgqrs/Cargo.toml](sqlite_support_worktree/crates/pgqrs/Cargo.toml#L1), the `sqlite` feature pulls `sqlx/sqlite`. Good.
- **Docs for feature usage:** Consider a brief section in the README stating: build SQLite backend with `--no-default-features --features sqlite`, and how to pass `PGQRS_TEST_SQLITE_DSN`.

## CI & Makefile
- **Matrix backend testing:** In [ci.yml](sqlite_support_worktree/.github/workflows/ci.yml#L1), matrix builds for both `postgres` and `sqlite` are excellent. You can shave some CI time by starting Postgres services only when `matrix.backend == 'postgres'` (split jobs or conditionally include steps), since they’re unused for `sqlite` runs.
- **Feature flags in CI:** `CARGO_FEATURES="--no-default-features --features ${{ matrix.backend }}"` is perfect for isolating backends. Ensure `PGQRS_TEST_SQLITE_DSN` is set for the SQLite leg (it is: `sqlite:///tmp/ci_test.db`). Consider cleanup of the temp db between runs.
- **Makefile targets:** The `test-*` targets in [Makefile](sqlite_support_worktree/Makefile#L1) are clear and helpful. Nice use of `UV` for Python deps. Optional: add a `test-sqlite-only` target that skips the maturin develop step for faster pure-Rust validation.

## Quick Wins (Low Effort, High Clarity)
- Add `PRAGMA foreign_keys=ON` and a note in `SqliteStore::new()`.
- Fix `query_bool()` to coerce integer-to-bool intentionally.
- Remove the no-op `Arc::new(workers);` in `SqliteProducer::new()`.
- Add the `idx_messages_queue_enqueued_at` index.
- Align `Drop` handling in `SqliteProducer` with `SqliteConsumer`.
- Document that `Admin.install()` assumes migrations were run in `SqliteStore::new()` or run migrations there.

## Suggested Follow-up Tests
- Unit test for `SqliteStepGuard`’s drop path (ensuring a dropped, uncompleted step becomes `ERROR`).
- Integration tests for DLQ move and replay across transaction boundaries.
- Tests for `reclaim_messages()` ensuring zombie detection and release/shutdown are atomic.
- Tests for `count_pending_filtered()` with `vt=NULL` and `vt<=now` cases.

## Overall
Solid implementation with thoughtful parity to Postgres. Addressing the few correctness and ergonomic items above—particularly FK enforcement, boolean decoding, and consistent ephemeral cleanup—will make the SQLite backend more robust and maintainable.
