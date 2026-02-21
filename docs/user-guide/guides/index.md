<!-- Context: user-guide/guides | Priority: high | Version: 1.0 | Updated: 2026-02-21 -->
# Guides

Step-by-step tutorials and practical guides for common pgqrs use cases.

## Available Guides

<div class="grid cards" markdown>

-   :material-play-circle:{ .lg .middle } **Basic Workflow**

    ---

    Set up a complete producer-consumer workflow using the new Trigger/Worker architecture.

    [:octicons-arrow-right-24: Basic Workflow](basic-workflow.md)

-   :material-package-variant:{ .lg .middle } **Batch Processing**

    ---

    Efficiently process high volumes of messages.

    [:octicons-arrow-right-24: Batch Processing](batch-processing.md)

-   :material-clock-outline:{ .lg .middle } **Delayed Messages**

    ---

    Schedule messages for future processing.

    [:octicons-arrow-right-24: Delayed Messages](delayed-messages.md)

-   :material-account-group:{ .lg .middle } **Worker Management**

    ---

    Scale and manage multiple workers in production.

    [:octicons-arrow-right-24: Worker Management](worker-management.md)

-   :material-sync:{ .lg .middle } **Durable Workflows**

    ---

    Build crash-resistant, resumable multi-step workflows with automatic recovery.

    [:octicons-arrow-right-24: Durable Workflows](durable-workflows.md)

-   :material-sync:{ .lg .middle } **Retry Strategies**

    ---

    Handle transient failures with automatic, non-blocking retries and backoff.

    [:octicons-arrow-right-24: Retry Strategies](durable-workflows.md#best-practices)

-   :material-pause-circle:{ .lg .middle } **Pausing Workflows**

    ---

    Suspend execution to wait for external events or human approval.

    [:octicons-arrow-right-24: Pausing Workflows](durable-workflows.md#advanced-pausing-for-external-events)

</div>

## Quick Reference

| Guide | Use Case |
|-------|----------|
| [Basic Workflow](basic-workflow.md) | Getting started, understanding the basics |
| [Durable Workflows](durable-workflows.md) | Multi-step processes, crash recovery |
| [Retry Strategies](durable-workflows.md#best-practices) | Error handling, backoff |
| [Pausing Workflows](durable-workflows.md#advanced-pausing-for-external-events) | Human-in-the-loop, external webhooks |
| [Batch Processing](batch-processing.md) | High throughput, many messages |
| [Delayed Messages](delayed-messages.md) | Scheduled tasks, reminders, deferred work |
| [Worker Management](worker-management.md) | Production deployment, scaling |

## Prerequisites

Before following these guides, ensure you have:

1. pgqrs installed ([Installation Guide](../getting-started/installation.md))
2. A running PostgreSQL database
3. The pgqrs schema installed (`pgqrs install`)

## Code Examples

All guides include complete, runnable code examples in both Rust and Python. Choose the tab for your preferred language.
