# Admin API

The Admin API provides queue management, schema administration, and monitoring capabilities.

## Usage

=== "Rust"

    ```rust
    use pgqrs;

    let store = pgqrs::connect("postgresql://localhost/mydb").await?;

    // Use admin builder for operations
    pgqrs::admin(&store).install().await?;
    store.queue("tasks").await?;
    ```

=== "Python"

    ```python
    import pgqrs

    store = await pgqrs.connect("postgresql://localhost/mydb")
    admin = pgqrs.admin(store)

    # Use admin methods
    await admin.install()
    await store.queue("tasks")
    ```

## Schema Management

### install

Install the pgqrs schema (tables, indexes, constraints). It is idempotent and safe to call multiple times.

=== "Rust"

    ```rust
    pgqrs::admin(&store).install().await?;
    println!("Schema installed successfully");
    ```

=== "Python"

    ```python
    await admin.install()
    print("Schema installed successfully")
    ```

### verify

Verify that the pgqrs schema is correctly installed. Returns/Raises error if schema is missing.

=== "Rust"

    ```rust
    admin.verify().await?;
    println!("Schema verification passed");
    ```

=== "Python"

    ```python
    await admin.verify()
    print("Schema verification passed")
    ```

### uninstall

Remove the pgqrs schema (tables and data).

!!! danger
    This permanently deletes all queues, messages, workers, and archives!

=== "Rust"

    ```rust
    admin.uninstall().await?;
    println!("Schema removed");
    ```

=== "Python"

    !!! warning "Not Supported"
        `uninstall()` is not exposed in Python to prevent accidental data loss. Use the CLI or SQL for schema removal.

## Queue Management

### create_queue

Create a new queue.

=== "Rust"

    ```rust
    let queue = store.queue("email-notifications").await?;
    println!("Created queue: {} (ID: {})", queue.queue_name, queue.id);
    ```

=== "Python"

    ```python
    queue = await store.queue("email-notifications")
    print(f"Created queue: {queue.queue_name} (ID: {queue.id})")
    ```

**Returns QueueInfo:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | `i64`/`int` | Unique queue ID |
| `queue_name` | `String`/`str` | Queue name |
| `created_at` | `DateTime`/`str` | Creation timestamp |

### get_queue

Get a queue by name.

=== "Rust"

    ```rust
    let queue = admin.get_queue("email-notifications").await?;
    ```

=== "Python"

    !!! warning "Not Supported"
        `get_queue()` is typically accessed via table APIs (`admin.queues.get_by_name()`) in Python or implied during worker creation.

### delete_queue

Delete a queue and all its messages.

=== "Rust"

    ```rust
    admin.delete_queue("old-queue").await?;
    ```

=== "Python"

    !!! warning "Not Supported"
        `delete_queue()` is not directly exposed on the Admin object. Use CLI for destructive queue operations.

## Metrics & Monitoring

### queue_metrics

Get metrics for a specific queue.

=== "Rust"

    ```rust
    let metrics = admin.queue_metrics("email-notifications").await?;
    println!("Pending: {}", metrics.pending_messages);
    ```

=== "Python"

    !!! warning "Not Supported"
        Direct metrics objects are not exposed in Python. Use table APIs (`admin.get_messages().count()`) etc.

## Table APIs

Admin provides direct access to table operations.

### Queues Table

=== "Rust"

    ```rust
    // List all queues
    let queues = admin.queues.list().await?;

    // Get queue by ID
    let queue = admin.queues.get(queue_id).await?;

    // Count queues
    let count = admin.queues.count().await?;
    ```

=== "Python"

    ```python
    # List not fully exposed, but count is available
    queues = await admin.get_queues()
    count = await queues.count()
    ```

### Workers Table

=== "Rust"

    ```rust
    // List all workers
    let workers = admin.workers.list().await?;
    ```

=== "Python"

    ```python
    workers = await admin.get_workers()
    all_workers = await workers.list()
    count = await workers.count()
    ```

### Messages Table

=== "Rust"

    ```rust
    use pgqrs::tables::Messages;
    let messages = Messages::new(admin.pool.clone());
    let pending = messages.count_pending(queue_id).await?;
    ```

=== "Python"

    ```python
    messages = await admin.get_messages()
    count = await messages.count()
    ```

### Archive Table

=== "Rust"

    ```rust
    use pgqrs::Archive;
    let archive = Archive::new(admin.pool.clone());
    // ... extensive archive filtering options ...
    ```

=== "Python"

    ```python
    archive = await admin.get_archive()
    count = await archive.count()
    ```

## Example: Health Check

=== "Rust"

    ```rust
    async fn health_check(admin: &Admin) -> bool {
        admin.verify().await.is_ok()
    }
    ```

=== "Python"

    ```python
    async def health_check(admin):
        try:
            await admin.verify()
            return True
        except:
            return False
    ```
