# Test Selection

How to choose the smallest useful test command for `pgqrs`.

## Principles

- Prefer the narrowest command that still covers the change.
- Prefer `make` targets over raw `cargo` or `pytest` commands.
- Treat runtime as the main test axis: Rust, Python, or both.
- Broaden coverage when a change touches shared storage behavior, migrations, or Rust APIs that Python bindings expose.

## Fast Defaults

| Change Type | Recommended Command |
|------|-------------|
| Docs changes | `make docs-build` |
| Prompts or repo metadata only | No runtime tests by default; optionally `make check` |
| Rust-only change with no database-runtime-specific behavior | `make test-rust` |
| Python-only binding or SDK change | `make test-py` |
| Shared Rust + Python behavior | `make test-postgres` |
| Storage, migration, or dialect change | `make test-postgres` |

## Main Commands

Service bootstrap matters for targeted commands:

- `make test-rust` and `make test-py` do not start backend services for you.
- For `make test-rust` or `make test-py`, run `make start-postgres` first when you need the local Docker service.
- `make test-postgres` handles startup, setup, cleanup, and shutdown itself.

### Quality

- `make check`
  - Fast repo-wide formatting and lint checks.

### Docs

- `make docs-build`
  - Builds the MkDocs site with strict mode.

### Rust Only

- `make test-rust`
  - Runs Rust tests only through `cargo nextest`.

Optional selectors:

- `TEST=<integration-test-file-stem>`
- `FILTER='<nextest expression>'`

Examples:

```bash
make test-rust TEST=lib_tests
make test-rust TEST=workflow_retry_integration_tests FILTER='test(test_zero_delay_allows_immediate_retry)'
```

### Python Only

- `make test-py`
  - Runs Python tests only.

Optional selectors:

- `PYTEST_TARGET=<path>`
- `PYTEST_ARGS='<pytest args>'`

Examples:

```bash
make test-py PYTEST_TARGET=py-pgqrs/tests/test_guides.py
make test-py PYTEST_ARGS='-k workflow -q'
```

### Full Validation

- `make test-postgres`

Use this when:

- a change affects shared public APIs
- a change touches store code
- a change touches migrations or setup logic
- a change affects both Rust and Python behavior

## Postgres Guidance

Use `make test-postgres` by default for Postgres validation.

If you need a targeted Rust-only Postgres loop:

```bash
make start-postgres
PGQRS_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres \
make test-rust TEST=lib_tests
```

If you need a targeted Python-only Postgres loop:

```bash
make start-postgres
PGQRS_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres \
make test-py PYTEST_TARGET=py-pgqrs/tests
```

## Escalation Rules

Start narrow, then broaden when any of these are true:

- the change touches `crates/pgqrs/src/store/`
- the change touches migrations
- the change touches `py-pgqrs/src/lib.rs` or shared Rust APIs exposed to Python
- the change affects multiple runtimes
- the initial targeted command passes but the risk surface is broader

Typical escalation path:

1. `make check`
2. targeted `make test-rust` or `make test-py`
3. `make test-postgres`

## Practical Heuristics

- If only docs changed, start with `make docs-build`.
- If only prompt files changed, do not invent runtime validation.
- If only Python wrappers changed, start with `make test-py`.
- If only Rust logic changed and the database runtime is not the point, start with `make test-rust`.
- If runtime semantics are the point, jump straight to `make test-postgres`.

## Load With

- `agents/process.md`
- `agents/context/technical-domain.md`
- `agents/personas/tester.md`
