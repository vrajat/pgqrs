# Releases

Version history and release notes for pgqrs.

## Latest Release

<div class="grid cards" markdown>

-   :material-tag:{ .lg .middle } **v0.15.3**

    ---

    Library-only distribution, refined dequeue polling, and worker lifecycle cleanup across backends.

    [:octicons-arrow-right-24: Release Notes](changelog.md#0153-2026-04-11)

</div>

## Installation

=== "Rust"

    ```bash
    cargo add pgqrs
    ```

=== "Python"

    ```bash
    pip install pgqrs
    ```

## Release Schedule

pgqrs follows [Semantic Versioning](https://semver.org/):

| Version Type | Description | Example |
|--------------|-------------|---------|
| **Major** (x.0.0) | Breaking changes | 1.0.0 → 2.0.0 |
| **Minor** (0.x.0) | New features | 0.2.0 → 0.3.0 |
| **Patch** (0.0.x) | Bug fixes | 0.3.0 → 0.3.1 |

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| [0.15.3](changelog.md#0153-2026-04-11) | 2026-04-11 | Dequeue polling lifecycle fixes, library-only distribution, worker lifecycle cleanup |
| [0.15.2](changelog.md#0152-2026-04-07) | 2026-04-07 | Worker-name identity, refined S3 sync states, process-isolated S3 cache handling |
| [0.15.1](changelog.md#0151-2026-04-02) | 2026-04-02 | Python S3 handle support, S3 benchmarks, Turso/SQLite consistency |
| [0.15.0](changelog.md#0150-2026-03-23) | 2026-03-23 | S3-backed queue storage, benchmark docs |
| [0.14.0](changelog.md#0140-2026-02-21) | 2026-02-21 | Workflow trigger/worker redesign, retries, pausing |
| [0.13.0](changelog.md#0130-2025-10-03) | 2025-10-03 | Turso backend support |
| [0.12.0](changelog.md#0120-2025-12-xx) | 2025-12-XX | Single-table model migration |
| [0.11.0](changelog.md#0110-2025-12-19) | 2025-12-19 | Archive access, config options, worker API |
| [0.4.0](changelog.md#040-2025-12-12) | 2025-12-12 | Workspace support, Python bindings, metrics |
| [0.3.0](changelog.md#030-2024-11-13) | 2024-11-13 | Producer/Consumer architecture |
| [0.2.0](changelog.md#020-previous-release) | - | Initial queue implementation |
| [0.1.0](changelog.md#010-initial-release) | - | Project inception |

## Upgrade Guides

### Upgrading to 0.3.0

Version 0.3.0 introduces **breaking changes** with the new Producer/Consumer architecture.

**Before (0.2.x):**

```rust
// Old: Monolithic Queue struct
let queue = Queue::new(&pool, "my_queue").await?;
queue.enqueue(&payload).await?;
let msg = queue.dequeue().await?;
```

## Getting Updates

### Watch Repository

Star and watch the [GitHub repository](https://github.com/vrajat/pgqrs) for release notifications.

### Cargo

Check for updates:

```bash
cargo outdated
```

### PyPI

Check for updates:

```bash
pip list --outdated
```

## Pre-releases

Pre-release versions are available for testing:

```bash
# Rust (specific version)
cargo add pgqrs@0.4.0-beta.1

# Python (pre-release)
pip install pgqrs --pre
```

!!! warning
    Pre-release versions may contain breaking changes and are not recommended for production.

## Support Policy

| Version | Status | Support Until |
|---------|--------|---------------|
| 0.3.x | **Active** | Current |
| 0.2.x | Maintenance | 6 months after 0.3.0 |
| 0.1.x | End of Life | - |
