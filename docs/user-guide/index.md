<!-- Context: user-guide | Priority: high | Version: 1.0 | Updated: 2026-02-21 -->
# User Guide

Welcome to the pgqrs User Guide. This guide covers everything you need to know to effectively use pgqrs in your applications.

## Overview

pgqrs is a library-only durable execution engine with multiple storage backends. PostgreSQL remains the primary production backend, while SQLite, Turso, and S3-backed queues cover embedded, portable, and object-storage-backed deployments.

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
    - [Backend Selection](concepts/backends.md)
    - [Producer & Consumer](concepts/producer-consumer.md)
    - [Workers](concepts/workers.md)
    - [Message Lifecycle](concepts/message-lifecycle.md)
    - [Durable Workflows](concepts/durable-workflows.md)

-   :material-language-rust:{ .lg .middle } **Rust API**

    ---

    Complete reference for the Rust library.

    - [Overview](api/index.md)
    - [Producer](api/producer.md)
    - [Consumer](api/consumer.md)
    - [Admin](api/admin.md)

-   :material-language-python:{ .lg .middle } **Python API**

    ---

    Complete reference for Python bindings.

    - [Overview](api/index.md)
    - [Producer](api/producer.md)
    - [Consumer](api/consumer.md)
    - [Admin](api/admin.md)

-   :material-book-open-variant:{ .lg .middle } **Guides**

    ---

    Step-by-step tutorials for common use cases.

    - [Basic Workflow](guides/basic-workflow.md)
    - [S3 Queue Guide](guides/s3-queue.md)
    - [Durable Workflows](guides/durable-workflows.md)
    - [Batch Processing](guides/batch-processing.md)
    - [Delayed Messages](guides/delayed-messages.md)
    - [Worker Management](guides/worker-management.md)

-   :material-console:{ .lg .middle } **CLI Reference**

    ---

    Command-line tool documentation.

    - [Admin, Queue, and Worker CLI](cli-reference.md)

</div>

## Prerequisites

Before using pgqrs, ensure you have:

- **One supported backend**:
  PostgreSQL 12+, SQLite, Turso, or an S3 bucket plus AWS-compatible credentials
- **Rust 1.70+** (for Rust applications) or **Python 3.11+** (for Python applications)

## Quick Links

| Topic | Description |
|-------|-------------|
| [Installation](getting-started/installation.md) | How to install pgqrs for Rust or Python |
| [Backend Selection](concepts/backends.md) | Choose between PostgreSQL, SQLite, Turso, and S3 |
| [Architecture](concepts/architecture.md) | System design and components |
| [Producer](api/producer.md) | Creating and sending messages |
| [Consumer](api/consumer.md) | Processing messages |
| [S3 Queue Guide](guides/s3-queue.md) | Bootstrapping and syncing an S3-backed queue |
| [Durable Workflows](guides/durable-workflows.md) | Multi-step, crash-resistant workflows |
| [Admin, Queue, and Worker CLI](cli-reference.md) | Command-line operations |
