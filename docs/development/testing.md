# Testing Guide

How to run pgqrs tests locally and in CI.

## Prerequisites

- Rust toolchain
- `uv`
- `cargo-nextest` for Rust test runs (`make install-nextest`)
- Docker for `make test-postgres` and `make test-localstack`
- Turso credentials only if you run `make test-turso`

## Quick Start

```bash
# Install test and docs dependencies
make requirements

# Build Rust and Python bindings for the active backend
make build

# Fastest full-suite option with no external services
make test-sqlite

# Full Postgres suite with Docker-managed Postgres + PgBouncer
make test-postgres
```

## Test Targets

`make test` and `make test-rust` use `PGQRS_TEST_BACKEND`, which defaults to `postgres`.

| Target | What it does |
| --- | --- |
| `make test` | Runs the Rust and Python test suites for the active backend |
| `make test-rust` | Runs Rust tests only via `cargo nextest` |
| `make test-py` | Runs Python tests only via `pytest` |
| `make test-postgres` | Runs the full suite on Postgres, including setup and cleanup |
| `make test-setup-postgres` | Provisions Postgres test schemas |
| `make test-cleanup-postgres` | Drops Postgres test schemas unless `PGQRS_KEEP_TEST_DATA` is set |
| `make test-sqlite` | Runs the full suite on SQLite |
| `make test-turso` | Runs the full suite on Turso |
| `make test-localstack` | Runs the full suite on the S3 backend against LocalStack |
| `make test-s3` | Alias for `make test-localstack` |
| `make test-all-backends` | Runs the suite across the supported backends that are configured locally |
| `make test-backends BACKENDS=sqlite,turso` | Runs selected backends that are already configured |

## Running Specific Tests

```bash
# Rust tests only on the default backend (postgres unless overridden)
make test-rust

# Rust tests on SQLite
make test-rust PGQRS_TEST_BACKEND=sqlite

# A specific Rust test file
make test-rust TEST=workflow_tests

# A specific Rust test inside that file
make test-rust TEST=workflow_tests FILTER='test_workflow_scenario_success'

# Python tests on SQLite
make test-py PGQRS_TEST_BACKEND=sqlite

# A specific Python test file
make test-py PGQRS_TEST_BACKEND=sqlite PYTEST_TARGET=py-pgqrs/tests/test_guides.py

# Additional pytest arguments
make test-py PGQRS_TEST_BACKEND=sqlite PYTEST_ARGS='-k guides -q'
```

## Backend Setup

### Postgres (Local Docker)

`make test-postgres` is the preferred one-shot target. It starts Postgres and PgBouncer, provisions schemas, runs tests, cleans up, and stops containers.

If you need the steps individually:

```bash
make start-pgbouncer
make test-setup-postgres
make test PGQRS_TEST_BACKEND=postgres CARGO_FEATURES="--no-default-features --features postgres"
make test-cleanup-postgres
make stop-postgres
```

### Postgres (Existing CI or External Database)

```bash
export CI_POSTGRES_RUNNING=true
export PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5432/postgres"
export PGBOUNCER_TEST_DSN="postgres://postgres@localhost:6432/postgres"

make test-postgres
```

### SQLite

```bash
make test-sqlite
```

### Turso

```bash
make test-turso
```

### S3 / LocalStack

```bash
make test-localstack
```

## Test Layout

- Rust integration tests live in `crates/pgqrs/tests/`.
- Shared Rust test helpers live in `crates/pgqrs/tests/common/mod.rs`.
- Test-only workflow lifecycle helpers live in `crates/pgqrs/src/test_utils.rs`.
- Python tests live in `py-pgqrs/tests/`.
- Guide-level coverage lives in `crates/pgqrs/tests/guide_tests.rs` and `py-pgqrs/tests/test_guides.py`.

When adding tests, prefer the existing backend-aware helpers instead of wiring DSNs manually in each file.

### Workflow Lifecycle Tests

Workflow cancellation and replay tests now use a test-only harness rather than
encoding the full actor model inline in every test.

The main helpers are:

- `WorkflowTestRig`: role-oriented shortcuts such as "as consumer, dequeue" and
  "as external actor, get run"
- `WorkflowAttempt`: a dequeued trigger message plus its materialized run handle

Use those helpers when a test needs to model:

- consumer dequeue/materialize/start
- external actor cancellation
- consumer archive/release after invoking workflow logic

This keeps cancellation tests readable as lifecycle state-machine checks instead
of ad hoc queue/run plumbing.

## Troubleshooting

### `cargo-nextest` Missing

```bash
make install-nextest
```

### Keep Postgres Test Data for Debugging

```bash
PGQRS_KEEP_TEST_DATA=true make test-postgres
```

### Clean Up Postgres Schemas Manually

```bash
export CI_POSTGRES_RUNNING=true
export PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5432/postgres"

make test-cleanup-postgres
```

### Backend Selection Notes

- `make test-postgres` is the safest way to run Postgres tests locally because it manages setup and cleanup for you.
- `make test-backends` assumes each selected backend is already configured.
- `make test-localstack` requires Docker because it starts a LocalStack container.

## Related Docs

- [Contributing Guide](contributing.md)
- [Release Process](release-process.md)
