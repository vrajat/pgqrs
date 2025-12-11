# pgqrs

[![Rust](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml/badge.svg)](https://github.com/vrajat/pgqrs/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/pgqrs.svg)](https://badge.fury.io/py/pgqrs)

**pgqrs** is a library-only queue backed by PostgreSQL.
It is written in Rust and also provides bindings for Python.

## Documentation

- **[Rust Documentation](crates/pgqrs/README.md)**
- **[Python Documentation](py-pgqrs/README.md)**

## Features

* **Lightweight**: No servers to operate. Directly use pgqrs as a library in your Rust applications.
* **Compatible with Connection Poolers**: Use with pgBouncer or pgcat to scale connections.
* **Efficient**: Uses PostgreSQL's SKIP LOCKED for concurrent job fetching.
* **Exactly Once Delivery**: Guarantees exactly-once delivery within a time range specified by time limit.
* **Message Archiving**: Built-in archiving system for audit trails and historical data retention.

## Installation

### Python

```bash
pip install pgqrs
```

### Rust

```toml
[dependencies]
pgqrs = "0.4.0"
```

## Development

Prerequisites:
- **Rust**: 1.70+
- **Python**: 3.8+
- **PostgreSQL**: 12+

### Setup

We use `make` to manage the development lifecycle.

```bash
# Setup environment and install dependencies
make requirements
```

### Build & Test

```bash
# Build both Rust core and Python bindings
make build

# Run all tests (Rust + Python)
make test
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
