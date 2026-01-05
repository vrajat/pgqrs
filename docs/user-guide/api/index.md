# API Reference

pgqrs provides a unified API for both Rust and Python, designed to be idiomatic in each language while sharing the same underlying concepts.

## Modules

| Module | Description |
|--------|-------------|
| [Producer](producer.md) | Enqueue messages to queues |
| [Consumer](consumer.md) | Dequeue and process messages |
| [Admin](admin.md) | Manage queues, workers, and schema |
| [Workflows](workflows.md) | Create durable, multi-step workflows |
| [Configuration](configuration.md) | Connect and configure the client |

## Language Support

### Rust
- **Type Safety**: Strong typing for message payloads and configuration.
- **Performance**: Zero-cost abstractions and direct usage of `sqlx`.
- **Macros**: Powerful procedural macros for defining workflows (`#[pgqrs_workflow]`).

### Python
- **Async/Await**: Native `asyncio` support.
- **Ease of Use**: Decorators for workflows (`@workflow`) and simple function abstractions.
- **Bindings**: High-performance bindings to the Rust core using PyO3.

## Feature Parity

Most features are available in both languages, with some platform-specific differences:

| Feature | Rust | Python |
|---------|------|--------|
| **Core Messaging** | ✅ Full Support | ✅ Full Support |
| **Durable Workflows** | ✅ Full Support | ✅ Full Support |
| **Worker Management** | ✅ Explicit Control | ⚠️ Automatic Only |
| **Metrics** | ✅ Full Object Access | ⚠️ Restricted to CLI/Table |

See specific pages for detailed API differences and limitations.
