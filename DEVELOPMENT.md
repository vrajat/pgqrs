# Development Guide

## AI Development Context

This file provides context and directives for AI assistants working on this codebase to improve collaboration efficiency and avoid common pitfalls.

### Known Issues & Blockers

#### CLI Subprocess Timing Issue
- **Problem**: CLI health commands (`health liveness`, `health readiness`) timeout when run as subprocesses during integration testing
- **Symptoms**: `gRPC status error: status: 'The operation was cancelled', self: "Timeout expired"`
- **Root Cause**: Unknown - appears to be async runtime or network context differences between CLI subprocess and test process environments
- **Status**: Documented in README, hybrid testing approach implemented
- **Action**: Do NOT spend time trying to fix subprocess CLI calls - use hybrid testing approach instead

### Testing Patterns & Preferences

#### Integration Testing
- **Prefer**: Direct client library tests over subprocess CLI tests for reliability
- **Pattern**: Hybrid approach - test CLI interface validation + direct client library workflow
- **Infrastructure**: Use `pgqrs-test-utils` for server mocking with `start_test_server()`
- **Coverage**: Test both healthy and unhealthy server scenarios

#### Test Structure
- **apps/pgqrs/tests/**: CLI integration tests
- **crates/*/tests/**: Component-specific tests
- **pgqrs-test-utils**: Shared test infrastructure (MockQueueRepo, MockMessageRepo, start_test_server)

### Code Organization

#### Project Structure
- `apps/pgqrs/`: CLI application using clap derive
- `crates/pgqrs-client/`: Client library (same code CLI uses internally)
- `crates/pgqrs-server/`: gRPC server implementation
- `crates/pgqrs-test-utils/`: Shared test utilities and mocks
- `crates/pgqrs-core/`: Core domain types and logic

#### Dependencies
- **gRPC**: tonic 0.14.2 (keep versions consistent across crates)
- **CLI**: clap derive pattern with global flags and subcommands
- **Testing**: tokio-test, mock repositories, in-process server testing

### Development Directives

#### When AI Gets Stuck in Loops
- **Stop Signal**: If the same approach fails 2-3 times, STOP and pivot
- **Document**: Add failed approaches to this file's "Known Issues" section
- **Pivot**: Look for alternative approaches that achieve the same goal
- **Example**: CLI subprocess testing → hybrid testing approach

#### Time Boxing
- **Investigation Limit**: Don't spend more than 3-4 iterations on the same technical problem
- **Documentation**: If something doesn't work, document it and implement workaround
- **Focus**: Prioritize working solutions over perfect theoretical approaches

#### Communication Patterns
- **Reference Previous Work**: Always check todo lists and previous findings before starting
- **Be Explicit**: State assumptions and when they're proven wrong
- **Track Attempts**: Use todo lists to track what's been tried and failed

#### Suggest More Directives
- **Continuous Improvement**: When encountering new patterns, blockers, or inefficiencies, suggest adding new directives to this file
- **Update Context**: Propose updates to this file when discovering new architectural patterns or constraints
- **Pattern Recognition**: If you notice repeated issues or time sinks, suggest adding specific guidance

### Quick Reference

#### Testing Commands
```bash
# Run all CLI tests
cargo test -p pgqrs-cli

# Run specific integration test
cargo test -p pgqrs-cli --test healthcheck_integration

# Run client library tests
cargo test -p pgqrs-client

# Run with output
cargo test -- --show-output
```

#### Development Workflow
1. Check this file for known issues and patterns
2. Use todo lists to track progress and failed attempts
3. Time-box investigations (2-3 attempts max)
4. Document blockers and implement workarounds
5. Suggest improvements to this file when encountering new patterns

## Contributing

When working on this codebase:
1. Read the known issues before starting
2. Follow the testing patterns
3. Update this file with new learnings
4. Don't repeat failed approaches documented here