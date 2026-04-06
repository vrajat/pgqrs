<!-- Context: project-intelligence/technical | Priority: critical | Version: 1.1 | Updated: 2026-04-06 -->
# Technical Domain

**Concept**: pgqrs is a Rust-first durable execution and queue library with Python bindings. PostgreSQL is the primary production backend, with SQLite, Turso, and S3 also supported in the current codebase.

## Key Points
- Primary focus is a library workspace, not a hosted service.
- Workspace members: `crates/pgqrs`, `crates/pgqrs-macros`, `py-pgqrs`.
- Rust provides the core queue and workflow engine.
- Python bindings extend the same engine through PyO3.
- Backend support currently includes Postgres, SQLite, Turso, and S3.
- The repo contains some older queue-only and postgres-only wording; verify claims against live code and `README.md`.

## Example
```rust
let store = pgqrs::connect("postgresql://localhost/mydb").await?;
pgqrs::admin(&store).install().await?;
store.queue("jobs").await?;
```

## Primary Stack
| Layer | Technology | Version | Rationale |
|-------|-----------|---------|-----------|
| Language | Rust, Python | Rust (workspace), Python 3.11+ | Core engine plus Python bindings |
| Framework | Not applicable | N/A | Library project |
| Persistence | Postgres, SQLite, Turso, S3 | Feature-gated in Cargo | Multi-backend queue and workflow persistence |
| Infrastructure | Not applicable | N/A | Library project |
| Key Libraries | sqlx, PyO3, turso, aws-sdk-s3, object_store | N/A | Database and binding integrations |

## Naming Conventions
| Type | Convention | Example |
|------|-----------|---------|
| Rust files | snake_case | `workflow.rs` |
| Rust types | PascalCase | `WorkflowRecord` |
| Rust functions | snake_case | `workflow_step` |
| Python modules | snake_case | `workers.py` |
| Database | snake_case | `pgqrs_messages` |

## Code Standards
- Rust: Rust Style Guide
- Python: PEP 8
- Bash: Google Shell Style Guide

## Security Requirements
- Avoid unchecked casts and unchecked arithmetic on external or persisted inputs.
- Avoid `unwrap()` and `expect()` in production paths unless the invariant is explicit and local.
- Consider backend-specific concurrency, retry, and migration behavior when changing persistence code.

## 📂 Codebase References
- **Workspace metadata**: `Cargo.toml`
- **Rust public API**: `crates/pgqrs/src/lib.rs`
- **Runtime backend selection**: `crates/pgqrs/src/store/any.rs`
- **Workflow support**: `crates/pgqrs/src/workflow.rs`
- **S3 backend**: `crates/pgqrs/src/store/s3/mod.rs`
- **Python bindings**: `py-pgqrs/src/lib.rs`

## Related Files
- `README.md`
- `AGENTS.md`
- `living-notes.md`
- `decisions-log.md`
