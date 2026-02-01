# Branch Summary: Build & CI Optimization

## Overview
This branch focused on overhauling the build system, optimizing CI performance, and stabilizing the test suite. Key achievements include migrating to `cargo-nextest`, integrating `uv` for Python management, and implementing a Global Setup strategy for PostgreSQL tests.

## 1. Build System Overhaul

### Makefile Restructuring
The `Makefile` was completely rewritten to be modular and backend-aware:
- **Backend-Specific Targets**: distinct targets for `test-postgres`, `test-sqlite`, and `test-turso`.
- **Docker Management**: Added `start-postgres` and `stop-postgres` to manage a global, conflict-free database container (Port 5433).
- **Dependency Checks**: Added targets like `check-nextest` to ensure developer environment validity.
- **Documentation**: Added `docs` and `docs-build` targets using `uv` and `mkdocs`.

### Cargo & Rust
- **Migration to `cargo-nextest`**: Replaced standard `cargo test` with `cargo nextest` for faster, parallel test execution.
- **Profile Optimization**: Configured profiles for better compile/test performance.
- **Feature Flags**: Standardized usage of `--no-default-features` to isolate backend dependencies.

### Python Integration (`uv`)
- **Adoption of `uv`**: Replaced `pip` with `uv` for significantly faster virtual environment creation and package installation.
- **Targets**: `requirements`, `build`, and `test-py` now rely on `uv` for consistent and rapid environment syncing.

## 2. CI Performance Optimization (Global Setup)

To resolve concurrency issues and slow test times, we shifted from per-test setup to a **Global Setup strategy**:

- **Global Provisioning**: Created `setup_test_schemas` binary that creates ALL 14+ test schemas (`pgqrs_builder_test`, etc.) once at the start of the run.
- **Orchestration**: `make test-postgres` now runs:
    1.  `start-postgres` (Database on 5433)
    2.  `test-setup-postgres` (Runs schema provisioner)
    3.  `cargo nextest` (Runs tests in parallel)
    4.  `stop-postgres` (Cleanup)
- **Code Changes**: Removed expensive `install()` (CREATE TABLE) calls from individual test files (`macro_tests.rs`, `workflow_tests.rs`, `common/mod.rs`), relying instead on the pre-provisioned environment.

## 3. Test Suite Fixes & Stabilization

- **Concurrency Fixes**: Removed `pg_advisory_lock` usage which was causing bottlenecks and deadlocks.
- **Port Conflicts**: Moved test DB to port **5433** to avoid conflicts with local Postgres instances.
- **CLI Tests**: Fixed ambiguity in `cargo run` calls by specifying `--bin pgqrs` and removing redundant admin commands.
- **PgBouncer**: Updated `lib_pgbouncer_tests` to manually handle schema creation, as it runs in an isolated container stack.
- **Macro/Workflow**: Updated `macro_tests.rs` and `workflow_tests.rs` to use correct, provisioned schema names (`pgqrs_workflow_test`) and avoid "No schema selected" errors.

## Results

- **Pass Rate**: **154 / 156** tests passing. (Remaining 2 failures in `worker_tests` are known isolation issues to be addressed).
- **Performance**: Significant reduction in test execution time due to full parallelism and removal of setup overhead.
- **Developer Experience**: Single command usage (`make test-postgres`) handles the entire lifecycle.

## How to Run

```bash
# Run full PostgreSQL test suite (Fast)
make test-postgres

# Run SQLite tests
make test-sqlite

# Run Python tests
make test-py
```
