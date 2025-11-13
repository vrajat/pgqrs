# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

## [0.1.0] - Initial Release
### Added
- Basic project structure
- Core queue functionality
- PostgreSQL integration