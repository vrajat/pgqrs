# Dequeue Polling for Consumer Workflows

**Status**: Draft Design  
**Created**: 2026-02-22  
**Author**: OpenCode

---

## 1. Problem Statement

### 1.1 Why Polling Needs a Design
Consumers need a way to wait for work while remaining cancellable and healthy. Today, consumers can `fetch_one` or `fetch_all` immediately, but there is no first-class, interruptible, heartbeat-safe loop that waits for work and returns when items are available.

### 1.2 Practical Requirements
- **Graceful shutdown**: the app must stop polling without killing the process.
- **Batch flexibility**: return when 1..=batch_size items are available.
- **Worker liveness**: long polling must still update heartbeats.
- **Two deployment modes**:
  - Dedicated worker process
  - Composite application with background consumer

---

## 2. Goals & Non-Goals

### Goals
- Provide a `poll` API that returns when **any** items (1..=batch_size) are available.
- Make polling interruptible via a cancellation signal.
- Keep worker heartbeats alive while polling.
- Support dedicated worker and composite-app patterns.
- Provide Rust and Python usage patterns suitable for production.

### Non-Goals
- Changing producer or admin APIs (they are included for complete examples only).
- Replacing existing dequeue APIs (`fetch_one`, `fetch_all`).
- Providing an always-blocking server loop (apps should control their own lifecycle).
- Workflow result handling (out of scope for this design).

---

## 3. Architecture

### 3.1 Poll Loop (Core Behavior)
Polling is implemented as an async loop:
1. Attempt to dequeue up to `batch_size`.
2. If **any** messages are returned (1..=batch_size), return them.
3. Otherwise, wait for either:
   - a poll interval tick,
   - a heartbeat interval tick,
   - a cancellation signal.
4. On heartbeat tick, call `consumer.heartbeat()` and continue.
5. On cancellation, return a cancelled result.

### 3.2 Batch Semantics & Concurrency Guarantees
**Poll is best-effort.** If 1+ messages exist, it returns immediately with whatever it can obtain up to `batch_size`.

Guarantees:
- A poll loop will never return an empty vec *unless* it was cancelled.
- A poll loop will return as soon as any messages are available, even if fewer than `batch_size`.
- If multiple consumers are polling, each receives whatever it can acquire under normal dequeue semantics.

This removes the need for “peek” and avoids waiting for a full batch when work is already available.

---

## 4. API Design

### 4.1 Rust: `poll`

```rust
pub async fn poll<S, C>(self, store: &S, cancel: C) -> Result<Vec<QueueMessage>>
where
    S: Store,
    C: Future<Output = ()> + Send,
```

Behavior:
- Returns **1..=batch_size** when available.
- Returns a **cancel error** on cancellation.
- Uses a resolved consumer to avoid ephemeral worker churn.

Recommendation: keep `store` as an explicit parameter to match existing `fetch_one`/`fetch_all` patterns and avoid storing a reference inside the builder.

### 4.2 Poll Interval Method
Add a builder method for poll interval:

```rust
pub fn poll_interval(mut self, interval: std::time::Duration) -> Self
```

Heartbeat interval is not configurable (default only).

### 4.3 Python: `poll`
Python mirrors Rust semantics:
- Accept an `asyncio.Event` or task cancellation.
- Returns a list of messages or raises a cancel exception on cancellation.

---

## 5. Handler Support

Polling must work with both direct dequeue and handler-based flows.

### 5.1 Direct Poll
```rust
let messages = pgqrs::dequeue()
    .worker(&consumer)
    .batch(3)
    .poll_interval(Duration::from_millis(250))
    .poll(&store, cancel.cancelled())
    .await?;
```

### 5.2 Handler-Based Poll
Add a `poll` variant that executes the handler when **any** messages are ready.

```rust
pgqrs::dequeue()
    .worker(&consumer)
    .batch(3)
    .poll_interval(Duration::from_millis(250))
    .handle_batch(|msgs| async move {
        // run workflow
        Ok(())
    })
    .poll(&store, cancel.cancelled())
    .await?;
```

Semantics:
- `handle`: single-message handler (forces batch size = 1).
- `handle_batch`: executes when 1..=batch_size messages are available.
- Success: archive messages.
- Error: release messages.

---

## 6. Cancellation Strategy

### 6.1 In-Process Cancellation (Token/Event)
Use a cancellation token or event in the hosting app.

Pros:
- Clear lifecycle control
- Simple control flow inside the process

Cons:
- Requires in-process state
- Harder to trigger over the network

### 6.2 Worker State Cancellation (DB-backed)
Reuse the existing `suspended` state and add two new worker states:
- `polling`
- `interrupted`

This follows the Java thread model: consumers periodically check their own state and exit when interrupted.

**State Flow**
- Consumer starts polling -> set status to `polling`
- External caller sets worker state to `interrupted`
- Consumer checks state before each poll iteration and exits when `interrupted`
- Caller can set `suspended` or `shutdown` after exit

**Why DB-backed**
- Keeps cancellation state in the database
- Allows remote control without app-level signaling
- Avoids mixing control and data messages in a queue

### 6.3 Poison Message Cancellation (Rejected)
Mixing control and data messages adds ambiguity and complicates routing. Prefer DB-backed or in-process cancellation.

---

## 7. Usage Patterns

### 7.1 Dedicated Worker Process
**Startup**
- Connect store
- Create consumer
- Update worker status to `polling`

**Run**
- Poll loop checks worker state each iteration

**Shutdown**
- External caller marks worker as `interrupted`
- Consumer exits, then sets `suspended` or `shutdown`

### 7.2 Composite App (Background Component)
**Startup**
- Spawn background task
- Register worker, set status `polling`

**Monitoring**
- Check worker status via admin APIs
- Expose status endpoint

**Shutdown**
- Set worker to `interrupted` via admin API
- Background task exits cleanly

---

## 8. Error Handling
- Dequeue errors are surfaced immediately.
- Heartbeat errors are surfaced and stop the poll loop.
- Cancellation returns a dedicated cancel error (not an empty vec).

---

## 9. Testing Strategy
- Unit tests:
  - poll returns when any messages are available
  - poll returns cancel error on interruption
  - heartbeat interval is executed
- Integration:
  - `guide_tests.rs`
  - `test_guides.py`

---

## 10. Migration/Rollout
- Add poll methods without breaking existing APIs.
- Update guides in `docs/user-guides/guides/`.
- Add guide tests to ensure examples compile/run.

---

## 11. Alternatives Considered
- `poll()` without cancellation: unsafe for shutdown
- `poll_with_cancel` suffix: more explicit but noisier API
- LISTEN/NOTIFY: backend-specific and not portable
- Peek-based batching: unnecessary with best-effort semantics

---

## 12. Documentation Updates
- Update guides in `docs/user-guides/guides/` with polling and interruption examples.
- Add Rust guide coverage in `guide_tests.rs`.
- Add Python guide coverage in `test_guides.py`.
