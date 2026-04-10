# Test Selection

How to choose the smallest useful test command for `pgqrs`.

## Principles

- Prefer the narrowest command that still covers the change.
- Prefer `make` targets over raw `cargo` or `pytest` commands.
- Treat runtime and backend as separate axes:
  - runtime: Rust, Python, or both
  - backend: Postgres, SQLite, Turso, or S3
- Broaden coverage when a change touches shared storage behavior, migrations, backend-specific code, or Rust APIs that Python bindings expose.

## Fast Defaults

| Change Type | Recommended Command |
|------|-------------|
| Docs changes | `make docs-build` |
| Prompts or repo metadata only | No runtime tests by default; optionally `make check` |
| Rust-only change with no backend-specific behavior | `make test-rust` |
| Python-only binding or SDK change | `make test-py PGQRS_TEST_BACKEND=<backend>` |
| Shared Rust + Python behavior on one backend | `make test-<backend>` |
| Storage, migration, or dialect change | `make test-<backend>` on each relevant backend |
| S3 behavior change | `make test-localstack` |

## Main Commands

Service bootstrap matters for targeted commands:

- `make test-rust` and `make test-py` do not start backend services for you.
- For Postgres-targeted `make test-rust` or `make test-py`, run `make start-postgres` first.
- For S3-targeted `make test-rust` or `make test-py`, run `make start-localstack` first.
- Full backend targets such as `make test-postgres` and `make test-localstack` handle startup themselves.

### Quality

- `make check`
  - Fast repo-wide formatting and lint checks.
  - Good default for prompt-only or low-risk non-doc edits.

### Docs

- `make docs-build`
  - Builds the MkDocs site with strict mode.
  - Use this for documentation changes under `docs/`, `mkdocs.yml`, or related site content.

### Rust Only

- `make test-rust`
  - Runs Rust tests only through `cargo nextest`.
  - Best when the change is isolated to Rust logic and does not require Python validation.

Optional selectors:

- `TEST=<integration-test-file-stem>`
- `FILTER='<nextest expression>'`

Examples:

```bash
make test-rust TEST=lib_tests
make test-rust TEST=workflow_retry_integration_tests FILTER='test(test_zero_delay_allows_immediate_retry)'
```

### Python Only

- `make test-py PGQRS_TEST_BACKEND=<backend>`
  - Runs Python tests only.
  - Best when the change is isolated to `py-pgqrs` wrappers, guides, or Python behavior.

Optional selectors:

- `PYTEST_TARGET=<path>`
- `PYTEST_ARGS='<pytest args>'`

Examples:

```bash
make test-py PGQRS_TEST_BACKEND=sqlite PYTEST_TARGET=py-pgqrs/tests/test_guides.py
make test-py PGQRS_TEST_BACKEND=sqlite PYTEST_ARGS='-k workflow -q'
```

### Full Backend Validation

These targets run the backend-specific full suite and are the safest choice when Rust and Python behavior both matter.

- `make test-postgres`
- `make test-sqlite`
- `make test-turso`
- `make test-localstack`
- `make test-s3`

Use these when:

- a change affects shared public APIs
- a change touches backend-specific store code
- a change touches migrations or setup logic
- a change affects both Rust and Python behavior

## Backend Guidance

### Postgres

Use `make test-postgres` by default for Postgres validation.

Why:

- it provisions schemas
- it sets the right environment
- it runs cleanup automatically

Prefer the full target unless you explicitly need a tighter Rust-only loop.

If you need a targeted Rust-only Postgres loop:

```bash
make start-postgres
PGQRS_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres \
make test-rust PGQRS_TEST_BACKEND=postgres \
  CARGO_FEATURES="--no-default-features --features postgres" \
  TEST=lib_tests
```

If you need a targeted Python-only Postgres loop:

```bash
make start-postgres
PGQRS_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres \
make test-py PGQRS_TEST_BACKEND=postgres PYTEST_TARGET=py-pgqrs/tests
```

### SQLite

Use `make test-sqlite` for full backend coverage.

For a narrower Rust-only loop:

```bash
make test-rust PGQRS_TEST_BACKEND=sqlite \
  CARGO_FEATURES="--no-default-features --features sqlite" \
  TEST=lib_tests
```

For Python-only validation:

```bash
make test-py PGQRS_TEST_BACKEND=sqlite PYTEST_TARGET=py-pgqrs/tests/test_guides.py
```

### Turso

Use `make test-turso` for full backend coverage.

For a narrower Rust-only loop:

```bash
make test-rust PGQRS_TEST_BACKEND=turso \
  CARGO_FEATURES="--no-default-features --features turso" \
  TEST=turso_hardening
```

Use Turso validation when the change touches:

- SQL dialect behavior
- migration constraints
- SQLite-family locking or persistence logic

### S3

Use `make test-localstack` or its alias `make test-s3`.

Prefer the full target for S3 work because setup is more involved:

- LocalStack needs to be started
- environment variables must be set
- the target also lists stored SQLite objects after the run

Default choice for S3 changes:

```bash
make test-localstack
```

If you need a targeted S3 Rust loop instead:

```bash
make start-localstack
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_REGION=us-east-1 \
PGQRS_S3_BUCKET=pgqrs-test-bucket \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
make test-rust PGQRS_TEST_BACKEND=s3 \
  CARGO_FEATURES="--no-default-features --features s3" \
  TEST=s3_tests
```

If you need a targeted S3 Python loop instead:

```bash
make start-localstack
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_REGION=us-east-1 \
PGQRS_S3_BUCKET=pgqrs-test-bucket \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
make test-py PGQRS_TEST_BACKEND=s3 PYTEST_TARGET=py-pgqrs/tests
```

Use S3 validation when the change touches:

- `crates/pgqrs/src/store/s3/`
- object-store-backed persistence behavior
- backend detection or runtime backend selection for `s3://` DSNs

## Escalation Rules

Start narrow, then broaden when any of these are true:

- the change touches `crates/pgqrs/src/store/`
- the change touches migrations
- the change touches `py-pgqrs/src/lib.rs` or shared Rust APIs exposed to Python
- the change affects multiple runtimes
- the change fixes a backend-specific bug
- the initial targeted command passes but the risk surface is broader

Typical escalation path:

1. `make check`
2. targeted `make test-rust` or `make test-py`
3. relevant `make test-<backend>`
4. `make test-all-backends` only when the change genuinely spans the matrix

## Practical Heuristics

- If only docs changed, start with `make docs-build`.
- If only prompt files changed, do not invent runtime validation.
- If only Python wrappers changed, start with `make test-py`.
- If only Rust logic changed and the backend is not the point, start with `make test-rust`.
- If the backend semantics are the point, jump straight to the backend target.
- If S3 is involved, prefer `make test-localstack` unless you specifically need a tighter targeted loop.

## Load With

- `agents/process.md`
- `agents/context/technical-domain.md`
- `agents/personas/tester.md`
