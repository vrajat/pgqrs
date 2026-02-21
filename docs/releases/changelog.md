# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

*No unreleased changes.*

---

## [0.14.0] - 2026-02-21 {#0140---2026-02-21}

### Added

- **Trigger/Worker workflow architecture** with explicit workflow definitions and run execution
- **Durable workflow lifecycle states** including `QUEUED`, `PAUSED`, `SUCCESS`, and `ERROR`
- **Retry and pause semantics** using message visibility for backoff and external-event pauses
- **Result retrieval APIs** for non-blocking status checks and blocking waits
- **Python workflow bindings** aligned with the new workflow API patterns

### Changed

!!! warning "Breaking Changes"
    - Replaced legacy workflow creation APIs with `workflow("name").trigger(...).execute(...)`
    - Replaced global step acquisition with `ctx.step("id", ...)` on workflow handlers
    - Updated workflow schema to use definitions, runs, and step state tables

- **Workflow documentation overhaul** to match the v0.14 API and retry model
- **Worker registration** now uses fluent `consumer().handler().create()` pattern

### Fixed

- **Turso retry safety** to prevent duplicate inserts during visibility-based retries
- **Runtime initialization safety** to avoid panic in Python bindings

---

## [0.13.0] - 2025-10-03

### Added

- **Turso backend support** for distributed SQLite deployments
- **Turso backend documentation** covering configuration and usage

### Changed

- **Test infrastructure refactor** to support Turso integration

---

## [0.3.0] - 2024-11-13 {#030---2024-11-13}

### Added

- **Producer/Consumer Architecture** - Cleaner role-based interfaces with dedicated structs for message production and consumption
- **Unified Table Trait** - Consistent CRUD operations across all database tables
- **Count Methods** - `count()` and `count_by_fk()` for efficient metrics collection
- **Archive Table Trait** - Full Table trait implementation for `pgqrs_archive` with CRUD operations
- **Enhanced Schema Design** - Proper foreign key relationships between tables
- **Comprehensive Documentation** - Updated docs and examples

### Changed

!!! warning "Breaking Changes"
    - Replaced monolithic `Queue` struct with dedicated `Producer` and `Consumer` structs
    - Workers table now uses `queue_id` instead of `queue_name` for better normalization
    - Database architecture changed to four-table design (queues, workers, messages, archive)

**Migration Required:**

```rust
// Before (0.2.x)
let queue = Queue::new(&pool, "my_queue").await?;

// After (0.3.0)
let producer = pgqrs::producer(pool.clone(), &queue, host, port, &config).await?;
let consumer = pgqrs::consumer(pool.clone(), &queue, host, port, &config).await?;
```

### Improved

- Type-safe `Producer` with validation and rate limiting
- Optimized `Consumer` with batch operations
- Better error handling and validation
- README with architecture diagrams
- Developer documentation in CLAUDE.md

### Fixed

- All existing tests updated for new architecture
- Integration tests for Producer/Consumer workflows
- Documentation consistency

---

## [0.2.0] - Previous Release {#020---previous-release}

### Added

- Initial PostgreSQL-backed job queue implementation
- Basic queue operations and message handling
- CLI interface for queue management
- Schema installation and verification
- Worker registration and tracking

---

## [0.1.0] - Initial Release {#010---initial-release}

### Added

- Basic project structure
- Core queue functionality
- PostgreSQL integration
- Initial documentation

---

## Links

- [Unreleased]: https://github.com/vrajat/pgqrs/compare/v0.14.0...HEAD
- [0.14.0]: https://github.com/vrajat/pgqrs/compare/v0.13.0...v0.14.0
- [0.13.0]: https://github.com/vrajat/pgqrs/compare/v0.12.0...v0.13.0
- [0.3.0]: https://github.com/vrajat/pgqrs/compare/v0.2.0...v0.3.0
- [0.2.0]: https://github.com/vrajat/pgqrs/compare/v0.1.0...v0.2.0
- [0.1.0]: https://github.com/vrajat/pgqrs/releases/tag/v0.1.0
