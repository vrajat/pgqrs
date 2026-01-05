# Releases

Version history and release notes for pgqrs.

## Latest Release

<div class="grid cards" markdown>

-   :material-tag:{ .lg .middle } **v0.3.0**

    ---

    Producer/Consumer architecture, unified Table trait, enhanced schema design.

    [:octicons-arrow-right-24: Release Notes](changelog.md#030---2024-11-13)

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

=== "CLI"

    ```bash
    cargo install pgqrs
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
| [0.3.0](changelog.md#030---2024-11-13) | 2024-11-13 | Producer/Consumer architecture |
| [0.2.0](changelog.md#020---previous-release) | - | Initial queue implementation |
| [0.1.0](changelog.md#010---initial-release) | - | Project inception |

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

**After (0.3.0):**

```rust
// New: Separated Producer and Consumer
let producer = pgqrs::producer(pool.clone(), &queue, host, port, &config).await?;
producer.enqueue(&payload).await?;

let consumer = pgqrs::consumer(pool.clone(), &queue, host, port, &config).await?;
let messages = consumer.dequeue().await?;
```

**Migration Steps:**

1. Update your `Cargo.toml` to `pgqrs = "0.3"`
2. Replace `Queue` usage with `Producer` and/or `Consumer`
3. Update method calls to match new signatures
4. Run the schema migration (automatic with `pgqrs install`)

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
