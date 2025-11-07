# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building and Testing
- `cargo build` - Build the project
- `cargo build --release` - Build optimized release version
- `cargo test` - Run all tests (unit + integration)
- `cargo test --lib` - Run library tests only
- `cargo test --bin pgqrs` - Run CLI tests only
- `cargo test -- --test-threads=1` - Run tests sequentially (useful for database tests)

### Database Testing
- Tests use `testcontainers` for isolated PostgreSQL instances
- Integration tests are in `tests/` directory
- Use `RUST_LOG=debug` for detailed test output
- Tests marked with `#[serial_test::serial]` run sequentially to avoid database conflicts

### Code Quality and Linting
- `cargo clippy` - Run Rust linter
- `cargo clippy --all-targets --all-features` - Comprehensive linting
- `cargo fmt` - Format code
- `cargo fmt --check` - Check formatting without modifying files

### Documentation and Generation
- `cargo doc` - Generate documentation
- `cargo doc --open` - Generate and open documentation in browser

### CLI Development
- `cargo run -- --help` - Show CLI help
- `cargo run -- install` - Install pgqrs schema
- `cargo run -- queue create test-queue` - Create a test queue
- `cargo run -- message send --queue test-queue --payload '{"test": "data"}'` - Send test message

### Benchmarking
- `cd benchmark && docker-compose up -d` - Start benchmark environment
- `cd benchmark && ./run_benchmark.sh 40 5 2m` - Run performance benchmarks
- Results are stored in `benchmark/results/` directory

## Code Architecture

### High-Level Structure
pgqrs is a PostgreSQL-backed job queue system for Rust applications, providing both a library API and CLI tools. The architecture follows a modular design:

**Core Components:**
- `src/main.rs` - CLI entry point and command-line interface
- `src/lib.rs` - Library entry point and public API exports
- `src/admin.rs` - Administrative operations (install, create queues, metrics)
- `src/queue.rs` - Core queue operations (enqueue, dequeue, batch operations)
- `src/config.rs` - Configuration management and database connection setup
- `src/types.rs` - Data structures and type definitions
- `src/error.rs` - Error handling and custom error types
- `src/constants.rs` - System constants and defaults

**CLI Components:**
- `src/output.rs` - Output formatting (JSON, CSV, YAML, tables)
- Subcommands organized by domain (install, queue, message, metrics)

**Testing:**
- `tests/` - Integration tests using testcontainers
- `tests/common/` - Shared test utilities and helpers
- Uses PostgreSQL testcontainers for isolated test environments

**Benchmarking:**
- `benchmark/` - Performance testing suite with Docker Compose
- `benchmark/locustfile.py` - Load testing scenarios
- `benchmark/init_tables.sql` - Database schema for benchmarks

### Key Design Patterns
1. **PostgreSQL-Native**: Leverages PostgreSQL features like `SKIP LOCKED`, NOTIFY/LISTEN, and transactions
2. **Async/Await**: Built on tokio for async database operations
3. **Type Safety**: Strong typing for queue messages and configuration
4. **CLI + Library**: Dual-purpose crate serving both as library and CLI tool
5. **Testcontainers**: Isolated testing with real PostgreSQL instances

### Configuration Management
- `Config` struct handles database connection parameters
- Supports DSN (Data Source Name) format
- Environment variable support for containerized deployments
- File-based configuration with YAML/JSON support

### Database Schema
- Uses `pgqrs` schema namespace to avoid conflicts
- Queue tables are dynamically created per queue
- Metadata tables track queue information and statistics
- SKIP LOCKED pattern for concurrent job processing

### Error Handling
- Custom `PgqrsError` enum for domain-specific errors
- Integration with `anyhow` for error context
- Database errors are properly wrapped and contextualized

### Dependencies and External Systems

#### Key Dependencies
- `sqlx` - Database operations (async PostgreSQL driver with compile-time checked queries)
- `tokio` - Async runtime providing the foundation for all async operations
- `clap` - CLI argument parsing with derive macros for clean command definition
- `testcontainers` - Integration testing with isolated Docker-based PostgreSQL instances
- `serde` - JSON serialization/deserialization for message payloads and configuration
- `anyhow` - Error handling with context and chaining capabilities
- `tracing` - Structured logging and instrumentation
- `uuid` - Message ID generation and queue identification

#### PostgreSQL Version Compatibility
- **Minimum**: PostgreSQL 13 (required for improved `SKIP LOCKED` performance and reliability)
- **Tested**: PostgreSQL 13, 14, 15, 16
- **Key Features Used**:
  - JSON/JSONB types for flexible message payloads
  - `FOR UPDATE SKIP LOCKED` for concurrent queue processing without deadlocks
  - `NOTIFY/LISTEN` for real-time queue event notifications
  - Transactional DDL for atomic schema operations
  - Connection pooling and prepared statements for performance

#### External Integration Points
- **Database Connection**: Supports connection strings, SSL, and connection pooling
- **Container Orchestration**: Docker Compose for development and benchmarking environments
- **CI/CD**: GitHub Actions compatibility for automated testing and deployment
- **Monitoring**: Structured logging output compatible with log aggregation systems
- **Configuration**: Environment variable and file-based configuration for deployment flexibility

## Development Guidelines

### Code Style and Conventions
- Follow standard Rust formatting (`cargo fmt`)
- Use meaningful variable and function names
- Document public APIs with doc comments
- Prefer explicit error handling over unwrap/expect
- Use structured logging with `tracing` crate

### Database Interactions
- All database operations are async using `sqlx`
- Use prepared statements for performance and security
- Wrap operations in transactions where appropriate
- Handle connection errors gracefully
- Test with real PostgreSQL instances using testcontainers

### CLI Design Principles
- Follow conventional CLI patterns with clap
- Provide meaningful help text and examples
- Support multiple output formats (JSON, YAML, CSV, table)
- Use appropriate exit codes for success/failure
- Include progress indicators for long-running operations

### Testing Strategy
- Unit tests for business logic
- Integration tests for database operations
- Use `testcontainers` for isolated PostgreSQL testing
- Serial execution for database tests to avoid conflicts
- Mock external dependencies where appropriate

### Performance Considerations
- Use connection pooling for production deployments
- Leverage PostgreSQL's SKIP LOCKED for concurrent processing
- Batch operations where possible to reduce database round trips
- Monitor queue depth and processing latencies
- Consider unlogged tables for high-throughput scenarios

## Project Structure

### Core Library (`src/`)
```
src/
├── lib.rs           # Public API exports
├── main.rs          # CLI entry point
├── admin.rs         # Administrative operations
├── queue.rs         # Queue operations
├── config.rs        # Configuration management
├── types.rs         # Data structures
├── error.rs         # Error definitions
├── constants.rs     # System constants
└── output.rs        # CLI output formatting
```

### Testing (`tests/`)
```
tests/
├── cli_tests.rs     # CLI integration tests
├── lib_tests.rs     # Library integration tests
└── common/          # Shared test utilities
```

### Benchmarking (`benchmark/`)
```
benchmark/
├── docker-compose.yml  # Test environment
├── locustfile.py      # Load testing scenarios
├── init_tables.sql    # Benchmark schema
├── run_benchmark.sh   # Automation script
└── results/           # Performance data
```

## Important Implementation Notes

### PostgreSQL Features
- Uses `FOR UPDATE SKIP LOCKED` for concurrent queue processing
- Leverages PostgreSQL's JSON/JSONB for flexible message payloads
- Utilizes database transactions for atomicity
- Takes advantage of PostgreSQL's NOTIFY/LISTEN for real-time updates

### Queue Operations
- Enqueue operations insert messages with visibility timeout
- Dequeue operations use SKIP LOCKED to avoid contention
- Batch operations optimize for throughput
- Failed messages can be retried or moved to dead letter queues

### CLI Commands
- `install/uninstall` - Schema management
- `queue create/delete/list` - Queue administration
- `message send/list` - Message operations
- `metrics` - Performance monitoring

### Configuration
- Database connections via DSN or individual parameters
- Support for connection pooling and SSL
- Environment-specific configuration files
- Runtime configuration validation

### Error Recovery
- Automatic retry logic for transient failures
- Dead letter queue support for failed messages
- Graceful degradation on database connection issues
- Comprehensive error logging and monitoring

## Contribution Guidelines

### Git Workflow
1. Create feature branch from main
2. Make focused, atomic commits
3. Include tests and docs for new functionality
4. Create a new PR.

#### Branch naming
* Extract a feature name from the title.
* Create a branch with the feature name.
* Use kebabCase for the branchname

#### Conventional Commits (REQUIRED)

All commit messages and PR titles MUST follow conventional commit format:

```
Format: <type>(<scope>): <description>
```

Types:

* `feat`: - New features
* `fix`: - Bug fixes
* `refactor`: - Code refactoring
* `docs`: - Documentation
* `style`: - Code style/formatting
* `test`: - Testing changes
* `chore`: - Maintenance tasks
* `chore(deps)`: - Dependency updates
Common Scopes: admin, queue, cli

Examples:

feat(cli): add new command for tool management
fix(config): resolve parsing issue with nested tables
test(e2e): add tests for tool installation

#### PR Guidelines
Always include:

* Title should match commit conventions.
* Summary of changes.
* List of implemented features.
* Test status.
* Link to issue if applicable.

### Pre-commit Process

1. Run `cargo test` to ensure all tests pass
2. Run `cargo fmt` to format code
3. Update documentation for API changes
4. Add tests for new functionality
5. Ensure CI passes before requesting review

### Testing Best Practices

#### Test Categories
1. **Unit Tests** (`#[cfg(test)]` in `src/` files)
   - Pure business logic
   - Error handling
   - Configuration parsing

2. **Integration Tests** (`tests/` directory)
   - Database operations
   - CLI command execution
   - End-to-end workflows

3. **Benchmark Tests** (`benchmark/` directory)
   - Performance regression testing
   - Load testing scenarios

#### Writing Effective Tests
- Test both success and error cases
- Use descriptive test names: `test_enqueue_with_invalid_payload_returns_error`
- Mock external dependencies, use real PostgreSQL for integration
- Include edge cases: empty queues, large payloads, concurrent access
- Use `#[serial_test::serial]` for tests that require database isolation
- Run tests with `--test-threads=1` when database conflicts occur

#### Test Data Management
- Use `testcontainers` for isolated PostgreSQL instances in integration tests
- Clean up test data between test runs
- Use meaningful test data that reflects real-world scenarios
- Avoid hardcoded values; use constants or generate test data programmatically

### Handling PR Feedback and CI Issues

#### Common CI Failures and Fixes
1. **Formatting Issues**
   - Run `cargo fmt --all` to fix formatting violations
   - CI uses rustfmt action which checks all files
   - Ensure consistent formatting before pushing

2. **Clippy Warnings**
   - Run `cargo clippy --all-targets --all-features` locally
   - Address all warnings before pushing
   - Some warnings can be allowed with `#[allow(clippy::lint_name)]` if justified

3. **Test Failures**
   - Run `cargo test` locally before pushing
   - Check database connectivity for integration test failures
   - Use `RUST_LOG=debug` for detailed failure output

#### Addressing Review Feedback
- Read all review comments carefully before making changes
- Address each concern systematically:
  - Resource lifecycle management (cleanup, deletion)
  - API consistency and error handling
  - Documentation and examples completeness
- Add tests for any new functionality or bug fixes
- Update documentation and examples when APIs change
- Commit with descriptive messages explaining what feedback was addressed

#### Archive System Development Notes
- Archive tables follow naming pattern: `archive_{queue_name}`
- DELETE operations should handle both queue and archive tables
- PURGE operations can be separate for queue vs archive
- Always test table creation, deletion, and cleanup operations
- Consider transaction rollback scenarios for multi-table operations

````
