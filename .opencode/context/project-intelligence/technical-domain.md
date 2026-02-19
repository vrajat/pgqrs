<!-- Context: project-intelligence/technical | Priority: critical | Version: 1.0 | Updated: 2026-02-19 -->
# Technical Domain

**Concept**: pgqrs is a Rust-first queue library with Python bindings, optimized for high-performance persistence across Postgres, Sqlite, and Turso.

## Key Points
- Primary focus is a library (no framework or infrastructure layer).
- Rust (1.80+) delivers core queue performance and safety.
- Python (3.11+) bindings extend access to the Rust core.
- Persistence targets Postgres 15+, Sqlite 3+, and Turso 0.4+.
- Key libraries include sqlx, sqlite, and turso.

## Example
```rust
// Minimal store usage pattern
let store = PgqrsStore::connect("sqlite::memory:").await?;
let queue = store.queue("jobs");
queue.enqueue("payload").await?;
let message = queue.dequeue().await?;
store.ack(message.id()).await?;
```

Ref: https://doc.rust-lang.org/stable/style-guide/

## Primary Stack
| Layer | Technology | Version | Rationale |
|-------|-----------|---------|-----------|
| Language | Rust, Python | Rust (1.80+), Python (3.11+) | High-performance queue core with Python bindings |
| Framework | Not applicable | N/A | Library project |
| Database | Postgres, Sqlite, Turso | Postgres (15+), Sqlite (3+), Turso (0.4+) | Queue persistence across supported backends |
| Infrastructure | Not applicable | N/A | Library project |
| Key Libraries | sqlx, sqlite, turso | N/A | Backend drivers and persistence |

## Naming Conventions
| Type | Convention | Example |
|------|-----------|---------|
| Files | snake_case | `queue_store.rs` |
| Components | PascalCase | `QueueWorker` |
| Functions | PascalCase | `EnqueueMessage` |
| Database | snake_case | `pgqrs_messages` |

## Code Standards
- Rust: Rust Style Guide
- Python: PEP 8
- Bash: Google Shell Style Guide

## Security Requirements
- TBD

## 📂 Codebase References
**Rust store layer**: `crates/pgqrs/src/store/sqlite/worker/admin.rs`  
**Turso integration**: `crates/pgqrs/src/store/turso/worker/admin.rs`  
**Workspace metadata**: `Cargo.toml`  
**Python bindings**: `py-pgqrs/Cargo.toml`

## Related Files
- `business-domain.md`
- `business-tech-bridge.md`
- `decisions-log.md`
