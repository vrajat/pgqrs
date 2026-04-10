# Project Process

Shared development process and conventions for `pgqrs`.

---

## Usage

- Read `README.md`, `Makefile`, and the files you plan to change before editing.
- For non-trivial work, produce a short plan covering affected components, API impact, backend impact, and tests.
- For small, well-bounded fixes, implement directly after enough code reading.
- Keep changes focused. Do not mix unrelated cleanup into feature or bug-fix work.
- If repo docs drift from the code, update them or call out the discrepancy.

## Related Files

- `agents/README.md` - shared agent-doc overview
- `agents/context/technical-domain.md` - current project and backend context
- `agents/context/living-notes.md` - active API gaps and follow-ups
- `agents/testing.md` - targeted test-selection guide
- `agents/personas/planner.md` - planning guidance
- `agents/personas/builder.md` - implementation guidance
- `agents/personas/tester.md` - testing guidance
- `agents/personas/reviewer.md` - review guidance

---

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
* Use git worktrees for parallel development (see Worktrees section below).

#### Worktrees
This project encourages using git worktrees for parallelizing tasks:
1. Create a worktree: `git worktree add ./<name>_worktree -b <branch-name>`
2. Directory naming convention: end with `_worktree/` (e.g., `deps_worktree/`) so it's ignored by git.
3. Cleanup: `rm -rf <name>_worktree && git worktree prune`


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
Common Scopes: admin, queue, workflow

Examples:

feat(queue): add new queue inspection helper
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

1. Run `make fmt` to format code (required before commit)
2. Run `make clippy` to run linter checks
3. Run `make test` to ensure all tests pass
4. Update documentation for API changes
5. Add tests for new functionality
6. Ensure CI passes before requesting review

### Git and GitHub Operations
**IMPORTANT: Use command line tools only - no MCP git tools**

**Note on Terminal Pager Workarounds:**
Many git and gh commands use pagers (like `less`) which can interfere with terminal automation. Use these patterns to pipe output:

**Basic Git Commands:**
- `git status` - Check working directory status
- `git add <file>` or `git add .` - Stage changes for commit
- `git commit -m "message"` - Commit staged changes with message
- `git push` - Push commits to remote repository
- `git pull` - Pull latest changes from remote
- `git log --oneline | cat` - View commit history (pipe to avoid pager)
- `git log --oneline -10 | cat` - View last 10 commits
- `git diff | cat` - View unstaged changes (pipe to avoid pager)
- `git diff --cached | cat` - View staged changes (pipe to avoid pager)
- `git branch | cat` - List branches
- `git branch -r | cat` - List remote branches

**GitHub CLI Commands with Pager Workarounds:**
- `gh issue list | cat` - List issues without pager
- `gh issue view <number> | cat` - View issue details
- `gh pr list | cat` - List pull requests
- `gh pr view <number> | cat` - View pull request details
- `gh pr view <number> --comments | cat` - View PR comments
- `gh api repos/owner/repo/pulls/number/comments | cat` - Get PR comments via API
- `gh api repos/owner/repo/issues/number | cat` - Get issue details via API

## Development Workflow

### Development Workflow for New Architecture

### Overview
- Plan and create tasks. Review after every task completion.
- Read all review comments carefully before making changes
- Address each concern systematically:
  - Resource lifecycle management (cleanup, deletion)
  - API consistency and error handling
  - Documentation and examples completeness
- Add tests for any new functionality or bug fixes
- Update documentation and examples when APIs change
- Commit with descriptive messages explaining what feedback was addressed


### Build
- `cargo build` - Build the project
- `cargo build --release` - Build optimized release version

### Test
- Check if there is a test Postgres container running.
  - Login should be possible with DSN `postgresql://pgbench:pgbench@127.0.0.1:5432/pgbench`
  - If there is a container, run tests by setting the env var `PGQRS_TEST_DSN=postgresql://pgbench:pgbench@127.0.0.1:5432/pgbench`
- `cargo test` - Run all tests (unit + integration)
- `cargo test --lib` - Run library tests only
- `cargo test -- --test-threads=1` - Run tests sequentially (useful for database tests)

### Database Testing

#### Test Infrastructure Strategy

**Rust Tests (PostgreSQL):**
- Use **Global Setup** pattern for faster, parallel test execution
- Schema provisioning handled by `setup_test_schemas` binary (in `src/bin/setup_test_schemas.rs`)
- All test schemas are created once at the start of the test suite
- Individual tests verify connection and use pre-provisioned schemas
- External PostgreSQL required (CI services or local Docker via Makefile)

**Global Setup Binary (`setup_test_schemas`):**
```rust
// Located at: crates/pgqrs/src/bin/setup_test_schemas.rs
// Purpose: Pre-provision all test schemas before running test suite
// Invoked by: make test-postgres (via setup_test_schemas target)
```

This binary:
1. Connects to PostgreSQL using `PGQRS_TEST_DSN`
2. Creates all test schemas (e.g., `pgqrs_workflow_test`, `pgqrs_builder_test`)
3. Runs migrations (`install()`) for each schema
4. Enables parallel test execution without schema conflicts

**Why this approach:**
- **Performance:** ~3s for 168 tests (vs ~30s+ with per-test setup)
- **Parallelism:** All tests run concurrently via `cargo-nextest`
- **No conflicts:** Each test uses its own pre-provisioned schema
- **Reliability:** Schemas created by production code, not test-specific logic

**Python Tests:**
- Use `testcontainers` for isolated PostgreSQL instances
- Each test gets its own ephemeral database
- Slower but provides complete isolation

**Test Execution:**
- `make test-postgres` - Run full test suite (setup → test → cleanup)
- `cargo nextest run` - Run tests directly (requires schemas already set up)
- Use `RUST_LOG=debug` for detailed test output

**Test Cleanup:**
- Schemas are automatically dropped after `make test-postgres` completes
- Set `PGQRS_KEEP_TEST_DATA=1` to preserve schemas for debugging
- Manual cleanup: `make test-cleanup-postgres`
- Uses same binary as setup: `setup_test_schemas --cleanup`

### Code Quality and Linting
- `cargo clippy` - Run Rust linter
- `cargo clippy --all-targets --all-features` - Comprehensive linting
- `cargo fmt` - Format code
- `cargo fmt --check` - Check formatting without modifying files

### Documentation and Generation
- `cargo doc` - Generate documentation
- `cargo doc --open` - Generate and open documentation in browser

### Benchmarking
- `cd benchmark && docker-compose up -d` - Start benchmark environment
- `cd benchmark && ./run_benchmark.sh 40 5 2m` - Run performance benchmarks
- Results are stored in `benchmark/results/` directory

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
- Test with real PostgreSQL instances (external databases in CI/local, testcontainers for Python)

### Testing Best Practices
#### Test Categories
1. **Unit Tests** (`#[cfg(test)]` in `src/` files)
   - Pure business logic
   - Error handling
   - Configuration parsing

2. **Integration Tests** (`tests/` directory)
   - Rust tests use external PostgreSQL (via `PGQRS_TEST_DSN` env var)
   - Python tests use `testcontainers` for isolated PostgreSQL testing
   - Database operations
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
- Rust tests connect to external PostgreSQL (CI services or local Docker managed by Makefile)
- Python tests use `testcontainers` for isolated PostgreSQL instances
- Clean up test data between test runs
- Use meaningful test data that reflects real-world scenarios
- Avoid hardcoded values; use constants or generate test data programmatically

## Code Architecture

### High-Level Structure
pgqrs is a PostgreSQL-backed job queue system for Rust applications with a clean Producer/Consumer architecture and unified table interface. The primary surface is the in-process library API with Python bindings.

**Core Components:**
- `src/lib.rs` - Library entry point and public API exports
- `src/admin.rs` - System administration (install, metrics, worker management)
- `src/producer.rs` - Message creation, validation, and enqueue operations
- `src/consumer.rs` - Message consumption, processing, and archive operations
- `src/config.rs` - Configuration management and database connection setup
- `src/types.rs` - Data structures and type definitions for all entities
- `src/error.rs` - Error handling and custom error types
- `src/constants.rs` - SQL queries and system constants
- `src/validation.rs` - Message payload validation logic
- `src/rate_limit.rs` - Rate limiting and token bucket implementation

**Table Interface (`src/tables/`):**
- `src/tables/mod.rs` - Module exports and table trait definitions
- `src/tables/table.rs` - Unified Table trait interface for CRUD operations
- `src/tables/pgqrs_queues.rs` - Queue management and metadata operations
- `src/tables/pgqrs_workers.rs` - Worker registration and health tracking
- `src/tables/pgqrs_messages.rs` - All message storage and operations

**Testing:**
- `tests/` - Integration tests using external PostgreSQL for Rust, testcontainers for Python
- `tests/common/` - Shared test utilities and helpers
- Rust tests connect to external databases (CI or local Docker)
- Python tests use testcontainers for isolated test environments

**Examples and Benchmarking:**
- `examples/` - Usage examples for different API patterns
- `benchmark/` - Performance testing suite with Docker Compose
- `benchmark/locustfile.py` - Load testing scenarios for Producer/Consumer patterns

### Architecture Patterns

#### **Producer/Consumer Separation**
1. **Producer Role**:
   - Focused on message creation, validation, and rate limiting
   - Queue-specific instances for type safety
   - Built-in payload validation and size limits
   - Rate limiting to prevent queue overload

2. **Consumer Role**:
   - Optimized for job fetching with automatic locking
   - Message processing and completion tracking
   - Archive operations for audit trails
   - Batch operations for efficiency

3. **Clear Boundaries**:
   - Producers never read/consume messages
   - Consumers never create new messages
   - Independent scaling and deployment patterns

#### **Unified Table Interface**
1. **Table Trait**:
   - Consistent CRUD operations across all tables
   - Type-safe entity and NewEntity associations
   - Unified counting and filtering methods
   - Error handling with contextual messages

2. **Four Core Tables**:
   - `pgqrs_queues`: Queue definitions and metadata
   - `pgqrs_workers`: Worker registrations with queue relationships
- `pgqrs_messages`: All messages (ready, leased, and archived)


3. **Foreign Key Relationships**:
   - Workers linked to queues via `queue_id`
   - Messages linked to queues via `queue_id`
   - Archive entries linked to queues via `queue_id`
   - Referential integrity enforced at database level

### Key Design Patterns
1. **PostgreSQL-Native**: Leverages PostgreSQL features like `SKIP LOCKED`, proper foreign keys, and transactions
2. **Async/Await**: Built on tokio for async database operations across all components
3. **Type Safety**: Strong typing for all entities with compile-time verification
4. **Role-Based APIs**: Separate Producer and Consumer APIs prevent cross-concern contamination
5. **Unified Data Model**: Single schema with proper relational design
6. **Testcontainers**: Isolated testing with real PostgreSQL instances for all components

### Configuration Management
- `Config` struct handles database connection parameters and validation settings
- Supports DSN (Data Source Name) format with schema specification
- Environment variable support for containerized deployments
- File-based configuration with YAML/JSON support
- Rate limiting and payload validation configuration

### Database Schema
- **Unified schema** with four core tables and proper relationships
- **Foreign key constraints** ensure data integrity across tables
- **Indexed columns** for efficient querying and filtering
- **Worker tracking** enables health monitoring and load balancing
- **Archive system** provides audit trails and compliance support
- Uses configurable schema namespace (default: 'public') to support multi-tenant deployments
- SKIP LOCKED pattern for concurrent job processing without deadlocks
- **IMPORTANT**: Schema must be pre-created before running `pgqrs install`

### Schema Management (Security Update - GitHub Issue #21)
**As of v0.2.1+, pgqrs uses PostgreSQL search_path instead of string replacement for schema handling:**

1. **Secure Schema Handling**: Uses PostgreSQL's search_path feature instead of SQL string replacement to prevent injection
2. **Schema Pre-creation Required**: The target schema must exist before running `pgqrs install`
3. **Configuration Options**:
   - Environment: `PGQRS_SCHEMA=my_schema`
   - Config file: `schema: "my_schema"`
   - Programmatic: `Config::from_dsn_with_schema(dsn, "my_schema")`
4. **Schema Validation**: Validates schema names according to PostgreSQL identifier rules
5. **Test Isolation**: Each test suite uses isolated schemas for parallel execution
6. **Backward Compatibility**: Defaults to 'public' schema if not specified

### Error Handling
- Custom `Error` enum for domain-specific errors
- Integration with `anyhow` for error context
- Database errors are properly wrapped and contextualized

### Dependencies and External Systems

#### Key Dependencies
- `sqlx` - Database operations (async PostgreSQL driver with compile-time checked queries)
- `tokio` - Async runtime providing the foundation for all async operations
- `serde` - JSON serialization/deserialization for message payloads and configuration
- `anyhow` - Error handling with context and chaining capabilities
- `tracing` - Structured logging and instrumentation
- `uuid` - Message ID generation and queue identification

**Note:** Rust tests use external PostgreSQL instances (no testcontainers dependency). Python tests still use `testcontainers` for isolated testing.

#### PostgreSQL Version Compatibility
- **Minimum**: PostgreSQL 13 (required for improved `SKIP LOCKED` performance and reliability)
- **Tested**: PostgreSQL 13, 14, 15, 16

#### External Integration Points
- **Database Connection**: Supports connection strings, SSL, and connection pooling
- **Container Orchestration**: Docker Compose for development and benchmarking environments
- **CI/CD**: GitHub Actions compatibility for automated testing and deployment
- **Monitoring**: Structured logging output compatible with log aggregation systems
- **Configuration**: Environment variable and file-based configuration for deployment flexibility

## Project Structure

### Core Library (`src/`)
```
src/
├── lib.rs              # Public API exports (Producer, Consumer, Admin, Table trait)
├── admin.rs            # System administration and cross-table operations
├── producer.rs         # Message creation, validation, and enqueue operations
├── consumer.rs         # Message consumption, processing, and completion
├── config.rs           # Configuration management with validation settings
├── types.rs            # Entity definitions (QueueInfo, WorkerInfo, QueueMessage, etc.)
├── error.rs            # Custom error types and handling
├── constants.rs        # SQL queries and database schema definitions
├── validation.rs       # Message payload validation logic
└── rate_limit.rs       # Token bucket rate limiting implementation
```

### Table Interface (`src/tables/`)
```
src/tables/
├── mod.rs              # Module exports and table trait re-exports
├── table.rs            # Unified Table trait definition (CRUD + counting)
├── pgqrs_queues.rs     # Queue management (NewQueue -> QueueInfo)
├── pgqrs_workers.rs    # Worker registration (NewWorker -> WorkerInfo)
├── pgqrs_messages.rs   # Message operations (NewMessage -> QueueMessage)
└── pgqrs_messages.rs   # Message operations
```

### Testing (`tests/`)
```
tests/
├── builder_tests.rs    # Builder integration tests
├── lib_tests.rs        # Library integration tests (Producer/Consumer/Admin)
├── default_schema_tests.rs  # Schema management and backward compatibility
├── error_tests.rs      # Error handling and validation testing
├── worker_tests.rs     # Worker registration and lifecycle testing
├── lib_pgbouncer_tests.rs   # Connection pooler compatibility testing
└── common/             # Shared test utilities and helpers
```

### Examples (`examples/`)
```
examples/
├── basic_usage.rs      # Complete Producer/Consumer workflow example
├── count_methods.rs    # Table trait interface demonstration
└── (future examples)   # Advanced patterns and use cases
```

### Benchmarking (`benchmark/`)
```
benchmark/
├── docker-compose.yml  # Test environment with PostgreSQL + monitoring
├── locustfile.py      # Load testing scenarios for Producer/Consumer patterns
├── init_tables.sql    # Benchmark schema with all four tables
├── run_benchmark.sh   # Automation script for performance testing
├── plot_section*.py   # Performance analysis and visualization scripts
├── results/           # Performance data and analysis results
└── scripts/           # Helper scripts for benchmark execution
```

### Additional Files
```
├── AGENTS.md          # Thin Codex/GPT wrapper
├── Cargo.toml         # Rust package configuration with feature flags
├── README.md          # User-facing documentation with updated architecture
├── CLAUDE.md          # Thin Claude wrapper
└── agents/            # Shared agent instructions
    ├── README.md      # Load order and directory overview
    ├── process.md     # This file - project processes
    ├── context/       # Current project context
    └── personas/      # Role-specific development guidelines
        ├── planner.md
        ├── builder.md
        ├── tester.md
        └── reviewer.md
```

### API Surface Organization

#### **Public API Exports (`src/lib.rs`)**
```rust
// Core role-based APIs
pub use crate::producer::Producer;
pub use crate::consumer::Consumer;
pub use crate::admin::Admin;

// Table interface for advanced use cases
pub use crate::tables::{Table, Queues, Workers, Messages};
pub use crate::tables::{NewQueue, NewWorker, NewMessage};

// Configuration and utilities
pub use crate::config::Config;
pub use crate::error::{Error, Result};
pub use crate::types::{QueueInfo, WorkerInfo, QueueMessage, WorkerStatus};
```
