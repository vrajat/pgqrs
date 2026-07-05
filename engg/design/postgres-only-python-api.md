# Mini-Design: Postgres-only Python API

## Context
With the removal of non-Postgres backends (SQLite, Turso, S3) in earlier workstreams, the Python bindings (`py-pgqrs`) have been consolidated to interface exclusively with the concrete Postgres-backed `Store` implementation in the Rust core.

## Status
- **Review Status:** Approved (Human Gate H1)
- **Implementation Status:** Completed (consolidated during backend removal and store API consolidation).

## Design Decisions

### 1. Concrete Postgres Store Handle
- Removed all internal references to `AnyStore`.
- The Python `pgqrs.Store` class directly wraps the concrete Rust `pgqrs::store::Store`.
- The connection methods `pgqrs.connect(dsn)` and `pgqrs.connect_with(config)` expect a Postgres connection string/configuration. Passing a non-Postgres DSN results in a `ConfigError` or `ConnectionError`.

### 2. Removal of Obsolete Config & Backend Selection
- No SQLite, Turso, or S3-specific configuration options are exposed in `pgqrs.Config`.
- Classes such as `S3StoreHandle` or other backend-specific wrappers have been completely removed.
- All operations (queues, messages, workers, workflow runs/steps) run on the unified Postgres tables.

### 3. Ergonomics & Type Stubs Stability
- Maintained the fluent API builder patterns (e.g., `pgqrs.workflow()`, `pgqrs.run()`, `pgqrs.step()`).
- Python type stubs (`__init__.pyi`) have been simplified to reflect only Postgres-backed types and the concrete `Store` API.

## Validation
Python integration tests cover the Postgres-only connection and execution flows:
```sh
make test-py
```
No references to `AnyStore`, `S3Store`, or non-Postgres configurations remain in `py-pgqrs`.
