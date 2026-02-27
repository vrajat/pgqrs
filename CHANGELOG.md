# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.0] - 2026-02-21

### Added
- **Trigger/Worker workflow architecture** with explicit workflow definitions and run execution
- **Durable workflow lifecycle states** including `QUEUED`, `PAUSED`, `SUCCESS`, and `ERROR`
- **Retry and pause semantics** using message visibility for backoff and external-event pauses
- **Result retrieval APIs** for non-blocking status checks and blocking waits
- **Python workflow bindings** aligned with the new workflow API patterns

### Changed
- **BREAKING**: Replaced legacy workflow creation APIs with `workflow("name").trigger(...).execute(...)`
- **BREAKING**: Replaced global step acquisition with `ctx.step("id", ...)` on workflow handlers
- **BREAKING**: Updated workflow schema to use definitions, runs, and step state tables
- **Workflow documentation overhaul** to match the v0.14 API and retry model
- **Worker registration** now uses fluent `consumer().handler().create()` pattern

### Fixed
- **Turso retry safety** to prevent duplicate inserts during visibility-based retries
- **Runtime initialization safety** to avoid panic in Python bindings

## [0.13.0] - 2025-10-03

### Added
- **Turso backend support** for distributed SQLite deployments
- **Turso backend documentation** covering configuration and usage

### Changed
- **Test infrastructure refactor** to support Turso integration

## [0.12.0] - 2025-12-XX

### Changed
- **BREAKING**: Migrated from a two-table model (`pgqrs_messages` + `pgqrs_archive`) to a single-table model. All messages now reside in the `pgqrs_messages` table.
- **BREAKING**: Removed `ArchivedMessage` and `ArchiveTable` types. Use `QueueMessage` and `MessageTable::list_archived_by_queue` instead.
- Updated `dequeued_at` semantics to represent the first lease time only (never reset).
- Optimized indexes for the "hot set" (`archived_at IS NULL`) across all backends.

## [0.11.0] - 2025-12-19

### Added (Python Bindings)
- **Advanced Configuration**: Fine-tune specific settings like `lock_time`, `batch_size`, `pre_fetch` count, and `heartbeat_interval` via the `Config` object.
- **Archive Access**: Read-only access to archived messages via `get_archived_message` and `list_archived_messages`.
- **Worker API**: Python API for registering and managing workers.
- **Delayed Messages**: Support for `enqueue_delayed` to schedule messages for future delivery.
- **Batch Operations**: Efficient batch enqueue and delete operations.
- **Message Ownership**: Enforced ownership checks when deleting or updating messages.
- **Visibility Extension**: Ability to extend the visibility timeout of a message using `extend_visibility`.

### Documentation
- **Comprehensive Guide**: Added a complete User Guide with "Getting Started", "Concepts", and API references for both Rust and Python.
- **Catppuccin Theme**: Updated documentation theme to Catppuccin with multiple color schemes.

### Chores
- **Build System**: Improved build system and dependency management.
- **Release Workflow**: Added manual release workflow for coordinated Rust and Python releases.

## [0.4.0] - 2025-12-12

### Added
- **Workspace Support**: Restructured the repository into a Cargo workspace to support multiple crates (core library and python bindings).
- **Python Bindings**: Initial structure for `py-pgqrs` crate.
- **Zombie Reclamation**: Admin command to recover tasks from dead workers.
- **Worker Health**: New health check command for workers.
- **System Stats**: Admin command to view global system statistics.
- **Queue Metrics**: CLI command to view metrics for specific queues.

### Changed
- **Renaming**: Removed `Pgqrs` prefix from internal struct names for cleaner API.

## [0.3.0] - 2024-11-13

### Added
- Producer/Consumer architectural separation for cleaner role-based interfaces
- Unified Table trait with consistent CRUD operations across all database tables
- Count methods (`count()` and `count_by_fk()`) for efficient metrics collection
- Full Table trait implementation for pgqrs_archive with CRUD operations
- Enhanced schema design with proper foreign key relationships
- Comprehensive documentation updates and examples

### Changed
- **BREAKING**: Replaced monolithic Queue struct with dedicated Producer and Consumer structs
- Refactored database architecture to four-table design (queues, workers, messages, archive)
- Updated workers table to use queue_id instead of queue_name for better normalization
- Enhanced error handling and validation across all interfaces

### Improved
- Type-safe Producer for message creation with validation and rate limiting
- Optimized Consumer for message processing with batch operations
- Updated README.md with detailed architecture diagrams and usage examples
- Enhanced CLAUDE.md with current code structure and development patterns

### Fixed
- All existing tests updated and passing with new architecture
- Integration tests for Producer/Consumer workflows
- Documentation consistency across all modules

## [0.2.0] - Previous Release

### Added
- Initial PostgreSQL-backed job queue implementation
- Basic queue operations and message handling
- CLI interface for queue management
- Schema installation and verification
- Worker registration and tracking

## [0.1.0] - Initial Release
### Added
- Basic project structure
- Core queue functionality
- PostgreSQL integration