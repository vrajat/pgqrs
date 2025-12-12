# Development

Resources for contributors and developers working on pgqrs.

<div class="grid cards" markdown>

-   :material-git:{ .lg .middle } **Contributing**

    ---

    How to contribute to pgqrs development.

    [:octicons-arrow-right-24: Contributing Guide](contributing.md)

-   :material-test-tube:{ .lg .middle } **Testing**

    ---

    Run and write tests for pgqrs.

    [:octicons-arrow-right-24: Testing Guide](testing.md)

-   :material-rocket-launch:{ .lg .middle } **Release Process**

    ---

    How releases are managed and published.

    [:octicons-arrow-right-24: Release Process](release-process.md)

</div>

## Quick Start for Contributors

### Prerequisites

- Rust 1.70+
- PostgreSQL 14+
- Docker (for testing)

### Clone and Build

```bash
git clone https://github.com/vrajat/pgqrs.git
cd pgqrs

# Build all crates
cargo build

# Run tests (requires PostgreSQL)
cargo test
```

### Project Structure

```
pgqrs/
├── crates/
│   └── pgqrs/           # Main Rust library and CLI
│       ├── src/
│       │   ├── lib.rs   # Library exports
│       │   ├── admin.rs # Admin API
│       │   ├── producer.rs
│       │   ├── consumer.rs
│       │   ├── config.rs
│       │   └── worker/  # Worker trait and types
│       ├── examples/
│       └── tests/
├── py-pgqrs/            # Python bindings (PyO3)
│   ├── src/lib.rs
│   └── tests/
└── docs/                # Documentation
```

## Development Workflow

1. **Create a branch** from `main`
2. **Make changes** and add tests
3. **Run tests** locally
4. **Submit a PR** with clear description
5. **Address review feedback**

## Getting Help

- [GitHub Issues](https://github.com/vrajat/pgqrs/issues) - Bug reports and feature requests
- [GitHub Discussions](https://github.com/vrajat/pgqrs/discussions) - Questions and ideas
