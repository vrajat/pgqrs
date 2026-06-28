# Testing Guide

How to run pgqrs tests locally and in CI.

## Prerequisites

- Rust toolchain
- `uv`
- `cargo-nextest` for Rust test runs (`make install-nextest`)
- Docker for `make test-postgres`

## Quick Start

```bash
# Install test and docs dependencies
make requirements

# Build Rust and Python bindings
make build

# Full Postgres suite with Docker-managed Postgres + PgBouncer
make test-postgres
```

## Test Targets

`make test`, `make test-rust`, and `make test-py` are all PostgreSQL-oriented.

| Target | What it does |
| --- | --- |
| `make test` | Runs the Rust and Python test suites |
| `make test-rust` | Runs Rust tests only via `cargo nextest` |
| `make test-py` | Runs Python tests only via `pytest` |
| `make test-postgres` | Runs the full suite on Postgres, including setup and cleanup |
| `make test-setup-postgres` | Provisions Postgres test schemas |
| `make test-cleanup-postgres` | Drops Postgres test schemas unless `PGQRS_KEEP_TEST_DATA` is set |

## Running Specific Tests

```bash
# Rust tests only
make test-rust

# A specific Rust test file
make test-rust TEST=workflow_tests

# A specific Rust test inside that file
make test-rust TEST=workflow_tests FILTER='test_workflow_scenario_success'

# Python tests
make test-py

# A specific Python test file
make test-py PYTEST_TARGET=py-pgqrs/tests/test_guides.py

# Additional pytest arguments
make test-py PYTEST_ARGS='-k guides -q'
```

## Postgres Setup

### Postgres (Local Docker)

`make test-postgres` is the preferred one-shot target. It starts Postgres and PgBouncer, provisions schemas, runs tests, cleans up, and stops containers.

If you need the steps individually:

```bash
make start-pgbouncer
make test-setup-postgres
make test
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

## Test Layout

- Rust integration tests live in `crates/pgqrs/tests/`.
- Shared Rust test helpers live in `crates/pgqrs/tests/common/mod.rs`.
- Test-only workflow lifecycle helpers live in `crates/pgqrs/src/test_utils.rs`.
- Python tests live in `py-pgqrs/tests/`.
- Guide-level coverage lives in `crates/pgqrs/tests/guide_tests.rs` and `py-pgqrs/tests/test_guides.py`.

When adding tests, prefer the existing shared helpers instead of wiring DSNs manually in each file.

### Workflow Lifecycle Tests

Workflow cancellation and replay tests use a test-only harness rather than encoding the full actor model inline in every test.

The main helpers are:

- `WorkflowTestRig`: role-oriented shortcuts such as "as consumer, dequeue" and "as external actor, get run"
- `WorkflowAttempt`: a dequeued trigger message plus its materialized run handle

Use those helpers when a test needs to model:

- consumer dequeue/materialize/start
- external actor cancellation
- consumer archive/release after invoking workflow logic

## Postgres Extension (pgrx) Testing

The `pgqrs-extension` is built using the `pgrx` framework.

### Prerequisites

- `cargo-pgrx` CLI (`make pgrx-init` or `cargo install cargo-pgrx --version 0.12.6 --locked`)
- A local PostgreSQL 15 installation (compiled automatically by pgrx via `make pgrx-init`)

### Commands

| Command | Description |
| --- | --- |
| `make pgrx-init` | Installs `cargo-pgrx` and downloads/compiles PostgreSQL 15 for local development |
| `make pgrx-build` | Compiles the extension |
| `make pgrx-test` | Runs the extension unit/integration tests inside a temporary Postgres instance |
| `make pgrx-install` | Installs the extension into the pgrx-managed Postgres installation |
| `make pgrx-schema` | Generates the SQL schema definition files for the extension |

### Linker Configuration (macOS)

Developing `pgrx` extensions on macOS requires allowing unresolved symbols at link time because Postgres symbols are resolved dynamically when the library is loaded by the database server. This is handled automatically by the workspace configuration in `.cargo/config.toml`:

```toml
[target.aarch64-apple-darwin]
rustflags = ["-C", "link-arg=-Wl,-undefined,dynamic_lookup"]
```

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

## Related Docs

- [Contributing Guide](contributing.md)
- [Release Process](release-process.md)
