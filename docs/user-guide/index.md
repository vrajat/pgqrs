# User Guide

Welcome to the pgqrs User Guide. This comprehensive guide covers everything you need to know to effectively use pgqrs in your applications.

## Overview

pgqrs is a library-only PostgreSQL-backed job queue designed for simplicity and reliability. Unlike traditional message queues that require separate infrastructure, pgqrs leverages your existing PostgreSQL database.

## Guide Structure

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Getting Started**

    ---

    Install pgqrs and create your first queue in minutes.

    - [Installation](getting-started/installation.md)
    - [Quickstart](getting-started/quickstart.md)

-   :material-school:{ .lg .middle } **Concepts**

    ---

    Understand the core architecture and design principles.

    - [Architecture](concepts/architecture.md)
    - [Producer & Consumer](concepts/producer-consumer.md)
    - [Workers](concepts/workers.md)
    - [Message Lifecycle](concepts/message-lifecycle.md)

-   :material-language-rust:{ .lg .middle } **Rust API**

    ---

    Complete reference for the Rust library.

    - [Overview](rust/index.md)
    - [Producer](rust/producer.md)
    - [Consumer](rust/consumer.md)
    - [Admin](rust/admin.md)

-   :material-language-python:{ .lg .middle } **Python API**

    ---

    Complete reference for Python bindings.

    - [Overview](python/index.md)
    - [Producer](python/producer.md)
    - [Consumer](python/consumer.md)
    - [Admin](python/admin.md)

-   :material-book-open-variant:{ .lg .middle } **Guides**

    ---

    Step-by-step tutorials for common use cases.

    - [Basic Workflow](guides/basic-workflow.md)
    - [Batch Processing](guides/batch-processing.md)
    - [Delayed Messages](guides/delayed-messages.md)
    - [Worker Management](guides/worker-management.md)

-   :material-console:{ .lg .middle } **CLI Reference**

    ---

    Command-line tool documentation.

    - [CLI Reference](cli-reference.md)

</div>

## Prerequisites

Before using pgqrs, ensure you have:

- **PostgreSQL 12+** - pgqrs uses features like `SKIP LOCKED` available in PostgreSQL 12 and later
- **Rust 1.70+** (for Rust applications) or **Python 3.8+** (for Python applications)

## Quick Links

| Topic | Description |
|-------|-------------|
| [Installation](getting-started/installation.md) | How to install pgqrs for Rust or Python |
| [Architecture](concepts/architecture.md) | System design and components |
| [Rust Producer](rust/producer.md) | Creating and sending messages in Rust |
| [Python Consumer](python/consumer.md) | Processing messages in Python |
| [CLI Reference](cli-reference.md) | Command-line operations |
