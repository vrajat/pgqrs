# Basic Queue (Producer + Consumer)

This guide shows the smallest end-to-end setup: enqueue JSON work from a producer, process it with a consumer, and shut down cleanly.

It is intentionally "low level" (queue primitives), and complements the workflow-focused guide.

## Prerequisites

- pgqrs installed
- A database backend selected (examples use SQLite for simplicity)
- Schema installed (`admin.install()`)


## Setup

The snippets in this page focus on the consumer patterns (polling + interrupt).

They assume you already have:

- `store` (connected + bootstrapped)
- `producer` and `consumer` (or `consumer_a` / `consumer_b`)

If you want fully runnable examples end-to-end, use the guide tests directly:

- Rust: `crates/pgqrs/tests/guide_tests.rs`
- Python: `py-pgqrs/tests/test_guides.py`


## Step 3: Create a Consumer and Poll

The consumer runs a poll loop that:

- dequeues up to `batch_size` messages
- calls your handler
- archives messages on success
- releases messages back to the queue on handler error

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_worker_poll"
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_interrupt_and_shutdown"
    ```

=== "Python"

    ```python
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_poll_and_interrupt"
    ```

## More Patterns

Two common variations you can build on top of the basic consumer loop.

### Handoff Between Consumers

Start consumer A, process one message, interrupt it, then start consumer B and confirm the next message is claimed by B.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_handoff_start_consumer_a"
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_handoff_interrupt_consumer_a"
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_handoff_start_consumer_b"
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_handoff_interrupt_consumer_b"
    ```

=== "Python"

    ```python
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_handoff_start_consumer_a"
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_handoff_interrupt_consumer_a"
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_handoff_start_consumer_b"
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_handoff_interrupt_consumer_b"
    ```



### Two Consumers Processing Continuously

Run two consumers in parallel and enqueue a small batch; both consumers should drain the queue until interrupted.

=== "Rust"

    ```rust
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_continuous_start_two_consumers"
    --8<-- "crates/pgqrs/tests/guide_tests.rs:basic_queue_continuous_interrupt_two_consumers"
    ```

=== "Python"

    ```python
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_continuous_start_two_consumers"
    --8<-- "py-pgqrs/tests/test_guides.py:basic_queue_py_continuous_interrupt_two_consumers"
    ```


## Next Steps

- If you need retries/backoff, see `docs/user-guide/guides/durable-workflows.md`
- If you need visibility timeouts and scheduling, see `docs/user-guide/guides/delayed-messages.md`
- For production worker patterns, see `docs/user-guide/guides/worker-management.md`
