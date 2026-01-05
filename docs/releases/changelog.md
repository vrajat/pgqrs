# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

*No unreleased changes.*

---

## [0.3.0] - 2024-11-13

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

## [0.2.0] - Previous Release

### Added

- Initial PostgreSQL-backed job queue implementation
- Basic queue operations and message handling
- CLI interface for queue management
- Schema installation and verification
- Worker registration and tracking

---

## [0.1.0] - Initial Release

### Added

- Basic project structure
- Core queue functionality
- PostgreSQL integration
- Initial documentation

---

## Links

- [Unreleased]: https://github.com/vrajat/pgqrs/compare/v0.3.0...HEAD
- [0.3.0]: https://github.com/vrajat/pgqrs/compare/v0.2.0...v0.3.0
- [0.2.0]: https://github.com/vrajat/pgqrs/compare/v0.1.0...v0.2.0
- [0.1.0]: https://github.com/vrajat/pgqrs/releases/tag/v0.1.0
