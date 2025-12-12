# Guides

Step-by-step tutorials and practical guides for common pgqrs use cases.

## Available Guides

<div class="grid cards" markdown>

-   :material-play-circle:{ .lg .middle } **Basic Workflow**

    ---

    Set up a complete producer-consumer workflow from scratch.

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

</div>

## Quick Reference

| Guide | Use Case |
|-------|----------|
| [Basic Workflow](basic-workflow.md) | Getting started, understanding the basics |
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
