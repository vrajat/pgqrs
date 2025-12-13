# Contributing to pgqrs

Thank you for your interest in contributing to pgqrs! This guide will help you get started.

## Ways to Contribute

- **Bug Reports** - Found a bug? Open an issue
- **Feature Requests** - Have an idea? Start a discussion
- **Documentation** - Improve docs and examples
- **Code** - Fix bugs or implement features

## Getting Started

### 1. Fork and Clone

```bash
# Fork on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/pgqrs.git
cd pgqrs
git remote add upstream https://github.com/vrajat/pgqrs.git
```

### 2. Set Up Development Environment

**Prerequisites:**

- Rust 1.70+ (`rustup update stable`)
- PostgreSQL 14+
- Docker (for integration tests)

**Build:**

```bash
# Build all crates
cargo build

# Build in release mode
cargo build --release
```

### 3. Set Up PostgreSQL

For local development:

```bash
# Using Docker
docker run -d \
  --name pgqrs-dev \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16

# Set connection string
export PGQRS_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
```

### 4. Run Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run with logging
RUST_LOG=debug cargo test
```

## Development Workflow

### Creating a Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/your-feature-name
```

### Making Changes

1. **Write code** following the style guide
2. **Add tests** for new functionality
3. **Update docs** if needed
4. **Run tests** to ensure nothing breaks

### Commit Messages

Use clear, descriptive commit messages:

```
feat: add delayed message support to Python bindings

- Implement enqueue_delayed method
- Add tests for delay functionality
- Update documentation
```

**Prefixes:**

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation
- `test:` - Tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

### Submitting a Pull Request

1. Push your branch to your fork
2. Open a PR against `vrajat/pgqrs:main`
3. Fill out the PR template
4. Wait for review

## Code Style

### Rust

Follow standard Rust conventions:

```rust
// Use rustfmt
cargo fmt

// Run clippy
cargo clippy
```

**Guidelines:**

- Use descriptive variable names
- Add documentation comments for public APIs
- Handle errors explicitly (no unwrap in library code)
- Write unit tests for new functions

```rust
/// Creates a new queue with the given name.
///
/// # Arguments
///
/// * `name` - The queue name. Must be unique.
///
/// # Example
///
/// ```
/// let queue = admin.create_queue("emails").await?;
/// ```
pub async fn create_queue(&self, name: &str) -> Result<QueueInfo> {
    // Implementation
}
```

### Python

Follow PEP 8 and use type hints:

```python
async def enqueue(self, payload: dict) -> int:
    """Enqueue a message.

    Args:
        payload: Message payload as a dictionary.

    Returns:
        The message ID.
    """
    ...
```

### Documentation

- Use clear, concise language
- Include code examples
- Test all examples

## Testing

### Unit Tests

Test individual functions:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_dsn() {
        let config = Config::from_dsn("postgresql://localhost/db");
        assert_eq!(config.schema, "public");
    }
}
```

### Integration Tests

Test with PostgreSQL:

```rust
#[tokio::test]
async fn test_create_queue() {
    let config = Config::from_env().unwrap();
    let admin = Admin::new(&config).await.unwrap();

    admin.install().await.unwrap();
    let queue = admin.create_queue("test_queue").await.unwrap();

    assert_eq!(queue.queue_name, "test_queue");
}
```

### Running Specific Tests

```bash
# Run tests matching pattern
cargo test queue

# Run tests in a specific file
cargo test --test integration_tests

# Run with verbose output
cargo test -- --nocapture
```

## Areas to Contribute

### Good First Issues

Look for issues labeled `good first issue`:

## Review Process

1. **Automated checks** run on all PRs
2. **Maintainer review** for code quality
3. **Approval** required before merge
4. **Squash merge** to main

## Getting Help

- **Questions?** Open a GitHub Discussion
- **Stuck?** Ask in the PR comments
- **Ideas?** Start a discussion first for larger changes

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 license.
